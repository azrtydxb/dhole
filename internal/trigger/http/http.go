// Package http is the trigger that fires on an inbound POST.
//
// It is the plainest reading of ADR 0007: a body arrives, its fields become
// the pipeline's DECLARED inputs, and the pipeline never learns that a person
// with curl — rather than a cron boundary or another pipeline — put them on
// the table.
//
// Two rules carry the weight here, and both are about the endpoint being
// reachable by anyone who can route to it:
//
// The body is BOUNDED before it is read. `io.ReadAll` on a request body is a
// memory exhaustion vector that takes one request to exploit, and no amount of
// validation afterwards helps, because the damage is done during the read.
//
// The payload is checked against the pipeline's own input schema, and a body
// that fails it is refused at the boundary with the reason. This is what typed
// ports buy: a malformed payload is a 400 somebody can read, not a run that
// dies in its first step with the values already half-applied.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	nethttp "net/http"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/trigger"
)

// Kind is what this trigger reports itself as.
const Kind = "http"

// DefaultMaxBodyBytes is the ceiling on a request body when a trigger does not
// set one. It is generous for an event payload and cheap to hold: the point of
// the number is that there IS one.
const DefaultMaxBodyBytes int64 = 1 << 20

// shutdownGrace is how long Start lets in-flight requests finish after its
// context is done, before closing them.
const shutdownGrace = 5 * time.Second

// Config configures an HTTP trigger. Everything it can refuse, it refuses in
// New: an unscoped trigger, a binding the pipeline does not declare, and a
// mapping that reads nothing.
type Config struct {
	// ID is the trigger's identity within the tenant. It appears in errors
	// and, when Untrusted is set, in the taint mark.
	ID       string
	TenantID string
	// Binding names the pipeline and maps each of its inputs to a field of
	// the request body, by dotted path: `ref`, `repository.full_name`.
	Binding trigger.Binding
	// Pipeline is the definition the binding and every payload are checked
	// against. It is required (ADR 0007).
	Pipeline *dholev1.Pipeline

	// MaxBodyBytes defaults to DefaultMaxBodyBytes.
	MaxBodyBytes int64
	// Untrusted marks every value this trigger produces as tainted
	// (ADR 0015). Set it for any endpoint that is not behind authenticated
	// API access: an unauthenticated POST is exactly as trustworthy as a
	// webhook, and the git trigger taints unconditionally for that reason.
	Untrusted bool

	// Listener, when set, is what Start serves on. It takes precedence over
	// Addr and is closed by Start.
	Listener net.Listener
	// Addr is the address Start listens on when Listener is nil.
	Addr string
	// OnError is called with every error that has nowhere else to go.
	OnError func(error)
}

// Trigger is one configured HTTP endpoint.
type Trigger struct {
	id        string
	tenantID  string
	binding   trigger.Binding
	pipeline  *dholev1.Pipeline
	maxBody   int64
	untrusted bool
	listener  net.Listener
	addr      string
	onError   func(error)
}

// Compile-time proof that this is a trigger.
var _ trigger.Trigger = (*Trigger)(nil)

// New validates cfg and returns the trigger it describes.
func New(cfg Config) (*Trigger, error) {
	if cfg.TenantID == "" {
		return nil, fmt.Errorf("http trigger %q: %w", cfg.ID, trigger.ErrTenantRequired)
	}
	if cfg.ID == "" {
		return nil, errors.New("http trigger: an id is required")
	}
	if err := trigger.ValidateBinding(cfg.Pipeline, cfg.Binding); err != nil {
		return nil, fmt.Errorf("http trigger %q: %w", cfg.ID, err)
	}
	for input, path := range cfg.Binding.InputMapping {
		if strings.TrimSpace(path) == "" {
			return nil, fmt.Errorf(
				"http trigger %q: input %q is bound to no field of the body", cfg.ID, input)
		}
	}

	t := &Trigger{
		id:        cfg.ID,
		tenantID:  cfg.TenantID,
		binding:   cfg.Binding,
		pipeline:  cfg.Pipeline,
		maxBody:   cfg.MaxBodyBytes,
		untrusted: cfg.Untrusted,
		listener:  cfg.Listener,
		addr:      cfg.Addr,
		onError:   cfg.OnError,
	}
	if t.maxBody <= 0 {
		t.maxBody = DefaultMaxBodyBytes
	}
	return t, nil
}

// Kind implements trigger.Trigger.
func (t *Trigger) Kind() string { return Kind }

// Handler is the endpoint, for mounting on a control plane's own mux. Start is
// a server around it, and nothing else differs between the two.
func (t *Trigger) Handler(sink trigger.Sink) nethttp.Handler {
	return nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		t.serve(w, r, sink)
	})
}

// Start serves the endpoint until ctx is done, then returns ctx's error. It
// owns its listener and closes it: a cancelled trigger must not still be a
// live endpoint.
func (t *Trigger) Start(ctx context.Context, sink trigger.Sink) error {
	if sink == nil {
		return fmt.Errorf("http trigger %q: a sink is required", t.id)
	}
	ln := t.listener
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", t.addr); err != nil {
			return fmt.Errorf("http trigger %q: listening on %q: %w", t.id, t.addr, err)
		}
	}
	return Serve(ctx, ln, t.Handler(sink), t.onError)
}

// Serve runs one handler on one listener until ctx is done. The git trigger
// serves the same way, so it lives here rather than twice.
func Serve(
	ctx context.Context, ln net.Listener, handler nethttp.Handler, onError func(error),
) error {
	srv := &nethttp.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	done := make(chan error, 1)
	go func() {
		err := srv.Serve(ln)
		if errors.Is(err, nethttp.ErrServerClosed) {
			err = nil
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			return err
		}
		return ctx.Err()
	case <-ctx.Done():
		// The shutdown is not the caller's context's to cancel; it has its
		// own budget, after which everything still open is closed.
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
		defer cancel()
		if err := srv.Shutdown(shutdown); err != nil && onError != nil {
			onError(err)
		}
		_ = srv.Close()
		<-done
		return ctx.Err()
	}
}

// serve is one request: bound it, decode it, map it, check it, fire it.
func (t *Trigger) serve(w nethttp.ResponseWriter, r *nethttp.Request, sink trigger.Sink) {
	if r.Method != nethttp.MethodPost {
		w.Header().Set("Allow", nethttp.MethodPost)
		WriteError(w, nethttp.StatusMethodNotAllowed,
			fmt.Errorf("a trigger is fired with %s, not %s", nethttp.MethodPost, r.Method))
		return
	}

	body, err := ReadBounded(w, r, t.maxBody)
	if err != nil {
		WriteError(w, StatusFor(err), err)
		return
	}

	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		WriteError(w, nethttp.StatusBadRequest,
			fmt.Errorf("the body is not a JSON object: %w", err))
		return
	}

	inputs, err := t.inputs(payload)
	if err != nil {
		WriteError(w, nethttp.StatusBadRequest, err)
		return
	}
	if err := trigger.ValidateInputs(t.pipeline, inputs); err != nil {
		WriteError(w, nethttp.StatusBadRequest, err)
		return
	}
	if err := sink.Fire(r.Context(), t.tenantID, t.binding.PipelineID, inputs); err != nil {
		if t.onError != nil {
			t.onError(err)
		}
		WriteError(w, nethttp.StatusInternalServerError,
			errors.New("the run could not be started"))
		return
	}
	WriteAccepted(w, t.binding.PipelineID)
}

// inputs maps the body onto the pipeline's declared inputs. A field the
// binding names and the body does not carry is a refusal: firing with the
// input missing would start a run that cannot proceed, and firing with it
// empty would start one on a value nobody sent.
func (t *Trigger) inputs(payload map[string]any) (map[string]*structpb.Value, error) {
	out := make(map[string]*structpb.Value, len(t.binding.InputMapping))
	for _, input := range sortedKeys(t.binding.InputMapping) {
		path := t.binding.InputMapping[input]
		raw, ok := lookup(payload, path)
		if !ok {
			return nil, fmt.Errorf(
				"input %q reads %q, which the body does not carry", input, path)
		}
		v, err := structpb.NewValue(raw)
		if err != nil {
			return nil, fmt.Errorf("input %q reads %q, which is not representable: %w",
				input, path, err)
		}
		if t.untrusted {
			v = trigger.MarkTainted(v, Kind+":"+t.id)
		}
		out[input] = v
	}
	return out, nil
}

// lookup walks a dotted path through a decoded body.
func lookup(payload map[string]any, path string) (any, bool) {
	var cur any = payload
	for _, segment := range strings.Split(path, ".") {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = obj[segment]; !ok {
			return nil, false
		}
	}
	return cur, true
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ReadBounded reads at most limit bytes of a request body and refuses the rest
// rather than buffering it. It is exported because the git trigger has the
// same public endpoint and the same vector.
func ReadBounded(w nethttp.ResponseWriter, r *nethttp.Request, limit int64) ([]byte, error) {
	body, err := io.ReadAll(nethttp.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var tooLarge *nethttp.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, fmt.Errorf("%w: the limit is %d bytes", ErrBodyTooLarge, limit)
		}
		return nil, fmt.Errorf("the body could not be read: %w", err)
	}
	return body, nil
}

// ErrBodyTooLarge is what a body over the limit fails with.
var ErrBodyTooLarge = errors.New("the request body is too large")

// StatusFor maps a read failure to its status.
func StatusFor(err error) int {
	if errors.Is(err, ErrBodyTooLarge) {
		return nethttp.StatusRequestEntityTooLarge
	}
	return nethttp.StatusBadRequest
}

// WriteError answers with one JSON object carrying the reason. The reason is
// the whole point: "400 Bad Request" tells whoever is holding a webhook
// configuration nothing about which field they got wrong.
func WriteError(w nethttp.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

// WriteAccepted answers a fire. 202, not 200: the run has been started, not
// finished, and a caller that waits for the result is misreading the contract.
func WriteAccepted(w nethttp.ResponseWriter, pipelineID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(nethttp.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "accepted", "pipeline_id": pipelineID})
}
