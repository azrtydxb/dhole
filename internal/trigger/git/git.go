// Package git is the trigger that fires on a forge's webhook: GitHub, Gitea
// or Forgejo.
//
// A git push is ONE event source here and never the privileged one (ADR 0007).
// The pipeline it starts declares inputs like any other, and cannot tell a
// push from a cron boundary.
//
// Three rules are load-bearing, and each is a way this endpoint stops being a
// trigger and becomes a remote execution primitive:
//
// The signature is verified BEFORE the payload is parsed, in constant time,
// and a MISSING signature is refused exactly as hard as a wrong one. An
// endpoint that verifies a signature when it finds one and fires when it does
// not has no signature check at all: the attacker omits the header.
//
// The body is bounded before it is read, like every public endpoint's.
//
// Everything the payload produces is TAINTED (ADR 0015). A webhook body is
// attacker-controlled by definition — anyone who can open a pull request can
// choose a branch name — and this is the boundary the taint model is defined
// at. Nothing downstream can add a mark that was not applied here.
package git

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	nethttp "net/http"
	"sort"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/trigger"
	httptrigger "github.com/azrtydxb/dhole/internal/trigger/http"
)

// Kind is what this trigger reports itself as.
const Kind = "git"

// DefaultMaxBodyBytes is the ceiling on a webhook body when a trigger does not
// set one. Forge payloads carry every commit in a push, so the default is
// larger than the plain HTTP trigger's — and it is still a limit.
const DefaultMaxBodyBytes int64 = 5 << 20

// The forges whose push payloads this trigger parses. They are reported on the
// `flavour` event field, because a pipeline that runs for three forges may
// legitimately need to know which one it was.
const (
	FlavourGitHub  = "github"
	FlavourGitea   = "gitea"
	FlavourForgejo = "forgejo"
)

// The fields of a git event, which is all a binding may draw on. They are
// checked at configuration time: a binding reading `scheduled_for` from a
// webhook is a mistake to catch while somebody is wiring it up.
const (
	FieldRef        = "ref"
	FieldBranch     = "branch"
	FieldCommitSHA  = "commit_sha"
	FieldRepository = "repository"
	FieldCloneURL   = "clone_url"
	FieldPusher     = "pusher"
	FieldEvent      = "event"
	FieldFlavour    = "flavour"
	FieldTriggerID  = "trigger_id"
	FieldKind       = "kind"
)

// DefaultEvents is what a trigger listens for when it names nothing. A push is
// the event a pipeline is nearly always after, and firing on every comment on
// every issue is a pipeline running a hundred times a day for no reason.
var DefaultEvents = []string{"push"}

var (
	errMissingSignature = errors.New("the request carries no signature")
	errBadSignature     = errors.New("the signature does not match the body")
)

// Config configures a git webhook trigger.
type Config struct {
	ID       string
	TenantID string
	// Secret is the shared secret configured on the forge's side. It is
	// required: a webhook endpoint that cannot verify anything is an
	// unauthenticated pipeline trigger.
	Secret string
	// Binding names the pipeline and maps its inputs to the git event's
	// fields, by the Field constants above.
	Binding trigger.Binding
	// Pipeline is the definition the binding and every payload are checked
	// against.
	Pipeline *dholev1.Pipeline

	// Events are the forge event names this trigger fires on; it defaults to
	// DefaultEvents.
	Events []string
	// MaxBodyBytes defaults to DefaultMaxBodyBytes.
	MaxBodyBytes int64

	// Listener, when set, is what Start serves on; it takes precedence over
	// Addr and is closed by Start.
	Listener net.Listener
	// Addr is the address Start listens on when Listener is nil.
	Addr string
	// OnError is called with every error that has nowhere else to go.
	OnError func(error)
}

// Trigger is one configured webhook endpoint.
type Trigger struct {
	id       string
	tenantID string
	secret   []byte
	binding  trigger.Binding
	pipeline *dholev1.Pipeline
	events   map[string]struct{}
	maxBody  int64
	listener net.Listener
	addr     string
	onError  func(error)
}

// Compile-time proof that this is a trigger.
var _ trigger.Trigger = (*Trigger)(nil)

// New validates cfg and returns the trigger it describes.
func New(cfg Config) (*Trigger, error) {
	if cfg.TenantID == "" {
		return nil, fmt.Errorf("git trigger %q: %w", cfg.ID, trigger.ErrTenantRequired)
	}
	if cfg.ID == "" {
		return nil, errors.New("git trigger: an id is required")
	}
	if cfg.Secret == "" {
		return nil, fmt.Errorf(
			"git trigger %q: a webhook secret is required; an endpoint that cannot verify "+
				"a signature is an unauthenticated pipeline trigger", cfg.ID)
	}
	if err := trigger.ValidateBinding(cfg.Pipeline, cfg.Binding); err != nil {
		return nil, fmt.Errorf("git trigger %q: %w", cfg.ID, err)
	}
	if err := validateSources(cfg.Binding); err != nil {
		return nil, fmt.Errorf("git trigger %q: %w", cfg.ID, err)
	}

	events := cfg.Events
	if len(events) == 0 {
		events = DefaultEvents
	}
	t := &Trigger{
		id:       cfg.ID,
		tenantID: cfg.TenantID,
		secret:   []byte(cfg.Secret),
		binding:  cfg.Binding,
		pipeline: cfg.Pipeline,
		events:   make(map[string]struct{}, len(events)),
		maxBody:  cfg.MaxBodyBytes,
		listener: cfg.Listener,
		addr:     cfg.Addr,
		onError:  cfg.OnError,
	}
	for _, e := range events {
		t.events[strings.ToLower(strings.TrimSpace(e))] = struct{}{}
	}
	if t.maxBody <= 0 {
		t.maxBody = DefaultMaxBodyBytes
	}
	return t, nil
}

// validateSources rejects a binding drawing on a field a git event does not
// carry.
func validateSources(b trigger.Binding) error {
	known := eventFields("", "", "", push{})
	for input, field := range b.InputMapping {
		if _, ok := known[field]; !ok {
			return fmt.Errorf(
				"binding fills input %q from %q, which a git event does not carry (it carries: %s)",
				input, field, strings.Join(fieldNames(), ", "))
		}
	}
	return nil
}

// Kind implements trigger.Trigger.
func (t *Trigger) Kind() string { return Kind }

// Handler is the webhook endpoint, for mounting on a control plane's own mux.
func (t *Trigger) Handler(sink trigger.Sink) nethttp.Handler {
	return nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
		t.serve(w, r, sink)
	})
}

// Start serves the webhook until ctx is done, then returns ctx's error.
func (t *Trigger) Start(ctx context.Context, sink trigger.Sink) error {
	if sink == nil {
		return fmt.Errorf("git trigger %q: a sink is required", t.id)
	}
	ln := t.listener
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", t.addr); err != nil {
			return fmt.Errorf("git trigger %q: listening on %q: %w", t.id, t.addr, err)
		}
	}
	return httptrigger.Serve(ctx, ln, t.Handler(sink), t.onError)
}

// serve is one webhook delivery, in the only order that is safe: identify the
// forge from headers, bound and read the body, VERIFY it, and only then look
// at what it says.
func (t *Trigger) serve(w nethttp.ResponseWriter, r *nethttp.Request, sink trigger.Sink) {
	if r.Method != nethttp.MethodPost {
		w.Header().Set("Allow", nethttp.MethodPost)
		httptrigger.WriteError(w, nethttp.StatusMethodNotAllowed,
			fmt.Errorf("a webhook is delivered with %s, not %s", nethttp.MethodPost, r.Method))
		return
	}

	flavour, event, ok := identify(r.Header)
	if !ok {
		httptrigger.WriteError(w, nethttp.StatusBadRequest, errors.New(
			"the request carries no GitHub, Gitea or Forgejo event header"))
		return
	}

	body, err := httptrigger.ReadBounded(w, r, t.maxBody)
	if err != nil {
		httptrigger.WriteError(w, httptrigger.StatusFor(err), err)
		return
	}

	// Nothing below this line may look at the body's CONTENT before it is
	// verified: an unverified payload is an attacker's payload.
	if err := verifySignature(t.secret, body, signatureHeader(r.Header)); err != nil {
		httptrigger.WriteError(w, nethttp.StatusUnauthorized, err)
		return
	}

	if _, ok := t.events[strings.ToLower(event)]; !ok {
		w.WriteHeader(nethttp.StatusNoContent)
		return
	}

	payload, err := parse(flavour, body)
	if err != nil {
		httptrigger.WriteError(w, nethttp.StatusBadRequest, err)
		return
	}

	inputs, err := t.inputs(flavour, event, payload)
	if err != nil {
		httptrigger.WriteError(w, nethttp.StatusBadRequest, err)
		return
	}
	if err := trigger.ValidateInputs(t.pipeline, inputs); err != nil {
		httptrigger.WriteError(w, nethttp.StatusBadRequest, err)
		return
	}
	if err := sink.Fire(r.Context(), t.tenantID, t.binding.PipelineID, inputs); err != nil {
		if t.onError != nil {
			t.onError(err)
		}
		httptrigger.WriteError(w, nethttp.StatusInternalServerError,
			errors.New("the run could not be started"))
		return
	}
	httptrigger.WriteAccepted(w, t.binding.PipelineID)
}

// identify says which forge sent this delivery, and what it called the event.
//
// Forgejo is checked FIRST because it sends Gitea's headers too, for
// compatibility with tooling written before the fork; checking Gitea first
// would report every Forgejo delivery as Gitea.
func identify(h nethttp.Header) (flavour, event string, ok bool) {
	for _, candidate := range []struct{ flavour, header string }{
		{FlavourForgejo, "X-Forgejo-Event"},
		{FlavourGitea, "X-Gitea-Event"},
		{FlavourGitHub, "X-GitHub-Event"},
	} {
		if v := strings.TrimSpace(h.Get(candidate.header)); v != "" {
			return candidate.flavour, v, true
		}
	}
	return "", "", false
}

// signatureHeader returns the first signature header present, whatever the
// forge calls it. GitHub sends `X-Hub-Signature-256` prefixed with `sha256=`;
// Gitea and Forgejo send bare hex under their own names, and both also send
// GitHub's header.
func signatureHeader(h nethttp.Header) string {
	for _, name := range []string{
		"X-Hub-Signature-256", "X-Forgejo-Signature", "X-Gitea-Signature",
	} {
		if v := strings.TrimSpace(h.Get(name)); v != "" {
			return v
		}
	}
	return ""
}

// verifySignature checks an HMAC-SHA256 of the raw body under the shared
// secret.
//
// Two properties, and neither is optional. The comparison goes through
// hmac.Equal: `==` on the bytes, or on a hex string, returns at the first
// differing byte and hands an attacker the expected MAC one byte at a time.
// And an empty signature is a FAILURE, not a skip.
func verifySignature(secret, body []byte, provided string) error {
	provided = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(provided), "sha256="))
	if provided == "" {
		return errMissingSignature
	}
	got, err := hex.DecodeString(provided)
	if err != nil {
		return fmt.Errorf("%w: it is not hex", errBadSignature)
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	if !hmac.Equal(got, mac.Sum(nil)) {
		return errBadSignature
	}
	return nil
}

// push is the part of a forge's push payload a trigger reads. The three
// flavours agree on more than they differ on, and where they differ — the
// pusher above all — parse() chooses per flavour rather than reading whichever
// field happens to be filled.
type push struct {
	Ref        string    `json:"ref"`
	After      string    `json:"after"`
	HeadCommit *commitOf `json:"head_commit"`
	Repository *repo     `json:"repository"`
	Pusher     *identity `json:"pusher"`
}

type commitOf struct {
	ID string `json:"id"`
}

type repo struct {
	FullName string `json:"full_name"`
	CloneURL string `json:"clone_url"`
}

// identity is the pusher. GitHub's is a git identity — {name, email} — while
// Gitea's and Forgejo's is a user account.
type identity struct {
	Name     string `json:"name"`
	Login    string `json:"login"`
	Username string `json:"username"`
}

// GetFullName and GetCloneURL read through a repository the payload may not
// have carried at all.
func (r *repo) GetFullName() string {
	if r == nil {
		return ""
	}
	return r.FullName
}

func (r *repo) GetCloneURL() string {
	if r == nil {
		return ""
	}
	return r.CloneURL
}

// parse decodes a verified payload.
func parse(flavour string, body []byte) (push, error) {
	var p push
	if err := json.Unmarshal(body, &p); err != nil {
		return push{}, fmt.Errorf("the %s payload is not readable: %w", flavour, err)
	}
	if p.Ref == "" || p.Repository == nil {
		return push{}, fmt.Errorf(
			"the %s payload carries no ref and repository, so it is not a push", flavour)
	}
	return p, nil
}

// pusher is where each forge puts the identity that pushed. GitHub names it,
// Gitea and Forgejo log it in; reading only the fields all three share would
// leave the bound input empty rather than fail, which is the quiet kind of
// wrong.
func pusher(flavour string, p push) string {
	if p.Pusher == nil {
		return ""
	}
	switch flavour {
	case FlavourGitHub:
		return p.Pusher.Name
	case FlavourGitea, FlavourForgejo:
		if p.Pusher.Login != "" {
			return p.Pusher.Login
		}
		return p.Pusher.Username
	default:
		return ""
	}
}

// commit is the sha a push landed on. GitHub fills `after` and repeats it in
// `head_commit`; a forced deletion leaves `after` zeroed, which is why the
// fallback exists.
func commit(flavour string, p push) string {
	if p.After != "" {
		return p.After
	}
	if flavour == FlavourGitHub && p.HeadCommit != nil {
		return p.HeadCommit.ID
	}
	return ""
}

// inputs turns one delivery into the pipeline's declared inputs, tainted.
func (t *Trigger) inputs(flavour, event string, p push) (map[string]*structpb.Value, error) {
	fields := eventFields(t.id, flavour, event, p)
	source := fmt.Sprintf("%s:%s:%s", Kind, flavour, t.id)

	out := make(map[string]*structpb.Value, len(t.binding.InputMapping))
	for _, input := range sortedKeys(t.binding.InputMapping) {
		field := t.binding.InputMapping[input]
		v, ok := fields[field]
		if !ok {
			return nil, fmt.Errorf("input %q reads unknown event field %q", input, field)
		}
		// Everything from a webhook is untrusted, without exception and
		// without a way to configure the exception (ADR 0015).
		out[input] = trigger.MarkTainted(structpb.NewStringValue(v), source)
	}
	return out, nil
}

func eventFields(id, flavour, event string, p push) map[string]string {
	return map[string]string{
		FieldRef:        p.Ref,
		FieldBranch:     strings.TrimPrefix(p.Ref, "refs/heads/"),
		FieldCommitSHA:  commit(flavour, p),
		FieldRepository: p.Repository.GetFullName(),
		FieldCloneURL:   p.Repository.GetCloneURL(),
		FieldPusher:     pusher(flavour, p),
		FieldEvent:      event,
		FieldFlavour:    flavour,
		FieldTriggerID:  id,
		FieldKind:       Kind,
	}
}

func fieldNames() []string {
	out := make([]string, 0, 10)
	for name := range eventFields("", "", "", push{}) {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
