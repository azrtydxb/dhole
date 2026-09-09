// Package trigger is the pluggable way a run starts.
//
// A trigger does NOT start a pipeline. It supplies that pipeline's declared
// inputs, and the run follows (ADR 0007). The distinction is the whole point:
// a pipeline does not know whether a cron boundary, an inbound webhook, an
// agent or a person put its inputs on the table, so the same definition is
// reachable from all four with no change to it. Dhole is not a CI tool, and a
// git commit is one event source here, never the privileged one.
//
// The shape is deliberately the executor's shape: an interface plus
// implementations. Adding an event source is additive; nothing in the
// scheduler, the store or the definition grows a case for it.
//
// Because triggers bind to typed ports, a trigger's payload is checkable
// against what the pipeline declares — see ValidateBinding. That check belongs
// at CONFIGURATION time. A binding that names a port the pipeline does not
// have is a mistake someone made while wiring a schedule up, and finding it
// then is worth far more than finding it at 3am inside the first step of a run
// that should never have been dispatched.
package trigger

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// ErrTenantRequired is returned for a trigger configured without a tenant.
// Every fire lands in exactly one tenant; an empty scope is a bug in the
// caller, never a wildcard. The message matches runstore's so that the same
// mistake reads the same way wherever it surfaces.
var ErrTenantRequired = errors.New("tenant scope required")

// Sink is what a trigger fires into: the one call that turns an event into a
// run. It takes the tenant explicitly because a trigger is the boundary where
// scope is decided, and inputs by name because that — not a payload blob — is
// what the pipeline declared.
type Sink interface {
	Fire(ctx context.Context, tenantID, pipelineID string, inputs map[string]*structpb.Value) error
}

// SinkFunc adapts a plain function to Sink.
type SinkFunc func(
	ctx context.Context, tenantID, pipelineID string, inputs map[string]*structpb.Value,
) error

// Fire implements Sink.
func (f SinkFunc) Fire(
	ctx context.Context, tenantID, pipelineID string, inputs map[string]*structpb.Value,
) error {
	return f(ctx, tenantID, pipelineID, inputs)
}

// Trigger is one configured event source. Start runs until ctx is done and
// returns ctx's error; it owns whatever it started and must leave nothing
// behind — no goroutine, no timer — when it returns.
type Trigger interface {
	Start(ctx context.Context, sink Sink) error
	Kind() string
}

// Binding says which pipeline a trigger drives and how its event becomes that
// pipeline's inputs. The map is keyed by the PIPELINE's input name, valued by
// the field of the trigger's own event that fills it: the pipeline's vocabulary
// is the stable one, and the trigger's is the one that varies per kind.
type Binding struct {
	PipelineID   string
	InputMapping map[string]string
}

// DeclaredInputs returns the pipeline's inputs: every step input port that no
// edge feeds, by name and type.
//
// There is no separate "pipeline inputs" list to drift from the steps. A port
// with an incoming edge is fed by another step and is not the outside world's
// to supply; everything else is exactly what a trigger — or a person, or a
// test — has to provide for the run to be able to start. Two steps declaring
// the same free port name declare one pipeline input, supplied once.
func DeclaredInputs(p *dholev1.Pipeline) map[string]*dholev1.PortType {
	if p == nil {
		return nil
	}
	fed := make(map[string]struct{}, len(p.GetEdges()))
	for _, e := range p.GetEdges() {
		fed[e.GetToStep()+"\x00"+e.GetToPort()] = struct{}{}
	}
	inputs := map[string]*dholev1.PortType{}
	for _, s := range p.GetSteps() {
		for _, port := range s.GetInputs() {
			if _, ok := fed[s.GetId()+"\x00"+port.GetName()]; ok {
				continue
			}
			inputs[port.GetName()] = port.GetType()
		}
	}
	return inputs
}

// ValidateBinding checks a binding against what the pipeline actually
// declares, and is meant to be called when a trigger is CONFIGURED.
//
// It rejects a binding for the wrong pipeline, a binding that names an input
// the pipeline does not declare, and a binding onto a blob port — a trigger
// carries structured event data, and a blob has to be uploaded before anything
// can reference it.
func ValidateBinding(p *dholev1.Pipeline, b Binding) error {
	if p == nil {
		return errors.New("trigger: a binding must be validated against a pipeline")
	}
	if b.PipelineID == "" {
		return errors.New("trigger: a binding must name a pipeline")
	}
	if p.GetId() != b.PipelineID {
		return fmt.Errorf("trigger: binding names pipeline %q but was checked against %q",
			b.PipelineID, p.GetId())
	}

	declared := DeclaredInputs(p)
	for name := range b.InputMapping {
		port, ok := declared[name]
		if !ok {
			return fmt.Errorf(
				"trigger: binding names input %q, which pipeline %q does not declare (declared: %s)",
				name, b.PipelineID, strings.Join(names(declared), ", "))
		}
		if port.GetStructured() == nil {
			return fmt.Errorf(
				"trigger: binding names input %q of pipeline %q, which is not a structured port; "+
					"a trigger supplies structured event data",
				name, b.PipelineID)
		}
	}
	return nil
}

func names(m map[string]*dholev1.PortType) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	if len(out) == 0 {
		return []string{"none"}
	}
	return out
}

// ValidateInputs checks the VALUES a trigger produced against the pipeline's
// declared inputs, and is meant to be called on every fire.
//
// ValidateBinding runs once, when somebody wires a trigger up, and can only
// check names. This runs on every event and checks what actually arrived: a
// webhook body is somebody else's data, and the whole benefit of typed ports
// (ADR 0007) is that a malformed payload is a refusal at the boundary with a
// reason instead of a run that dies inside its first step — or, worse, one
// that succeeds on the wrong values.
//
// The three triggers of Task 41 share this rather than each checking its own
// payload, because three copies of the rules are three places for them to
// drift apart. Tainted values are checked on what they carry, not on their
// wrapper: a mark is not a change to the data (ADR 0015).
func ValidateInputs(p *dholev1.Pipeline, inputs map[string]*structpb.Value) error {
	if p == nil {
		return errors.New("trigger: inputs must be validated against a pipeline")
	}
	declared := DeclaredInputs(p)

	// Sorted so that a body failing two ports always names the same one
	// first: an error that moves between identical requests is unreadable.
	supplied := make([]string, 0, len(inputs))
	for name := range inputs {
		supplied = append(supplied, name)
	}
	sort.Strings(supplied)

	for _, name := range supplied {
		port, ok := declared[name]
		if !ok {
			return fmt.Errorf(
				"trigger: pipeline %q does not declare an input %q (declared: %s)",
				p.GetId(), name, strings.Join(names(declared), ", "))
		}
		structured := port.GetStructured()
		if structured == nil {
			return fmt.Errorf(
				"trigger: input %q of pipeline %q is not a structured port; "+
					"a trigger supplies structured event data", name, p.GetId())
		}
		value := UntaintedValue(inputs[name])
		if value == nil {
			return fmt.Errorf("trigger: input %q of pipeline %q was supplied with no value",
				name, p.GetId())
		}
		if err := validateAgainstSchema(structured, name, value); err != nil {
			return fmt.Errorf("trigger: input %q of pipeline %q: %w", name, p.GetId(), err)
		}
	}
	return nil
}

// validateAgainstSchema checks one value against its port's declared JSON
// Schema. A port that carries no schema source is not checked here — the
// schema registry is not this package's to consult — and that is a gap that
// closes when the port has its document, not a reason to accept nothing.
func validateAgainstSchema(t *dholev1.StructType, name string, v *structpb.Value) error {
	if strings.TrimSpace(t.GetSchema()) == "" {
		return nil
	}
	schema, err := compileSchema(t.GetSchema())
	if err != nil {
		return fmt.Errorf("port %q declares a schema that does not compile: %w", name, err)
	}
	raw, err := v.MarshalJSON()
	if err != nil {
		return fmt.Errorf("value is not representable as JSON: %w", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("value is not valid JSON: %w", err)
	}
	if err := schema.Validate(instance); err != nil {
		return fmt.Errorf("value does not satisfy the port's schema %s: %w",
			t.GetSchemaId(), schemaFailure(err))
	}
	return nil
}

// schemaFailure flattens a validation error into one line naming the keywords
// that failed. The library's own multi-line output is fine in a terminal and
// unreadable in an HTTP response body, and the keyword is the part somebody
// fixing their payload needs.
func schemaFailure(err error) error {
	var v *jsonschema.ValidationError
	if !errors.As(err, &v) {
		return err
	}
	var parts []string
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) == 0 {
			at := strings.Join(e.InstanceLocation, "/")
			if at == "" {
				at = "the value"
			}
			parts = append(parts, fmt.Sprintf("%s: %s: %s",
				at, strings.Join(e.ErrorKind.KeywordPath(), "/"), oneLine(e.Error())))
			return
		}
		for _, c := range e.Causes {
			walk(c)
		}
	}
	walk(v)
	return errors.New(strings.Join(parts, "; "))
}

// oneLine flattens the library's indented, multi-line rendering.
func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

// compiled caches one compiled schema per schema source. Compiling a JSON
// Schema on every inbound webhook is a parser run per request on a public
// endpoint, which is a denial-of-service amplifier as well as a waste.
var compiled sync.Map

func compileSchema(source string) (*jsonschema.Schema, error) {
	if s, ok := compiled.Load(source); ok {
		switch v := s.(type) {
		case *jsonschema.Schema:
			return v, nil
		case error:
			return nil, v
		}
	}
	schema, err := doCompile(source)
	if err != nil {
		compiled.Store(source, err)
		return nil, err
	}
	compiled.Store(source, schema)
	return schema, nil
}

func doCompile(source string) (*jsonschema.Schema, error) {
	parsed, err := jsonschema.UnmarshalJSON(strings.NewReader(source))
	if err != nil {
		return nil, fmt.Errorf("not valid JSON: %w", err)
	}
	c := jsonschema.NewCompiler()
	// Offline, like the catalog's: a port's schema must not be able to make
	// the control plane fetch a URL while serving a webhook.
	c.UseLoader(offlineLoader{})
	const resource = "dhole:port-schema"
	if err := c.AddResource(resource, parsed); err != nil {
		return nil, err
	}
	return c.Compile(resource)
}

// offlineLoader refuses every remote reference.
type offlineLoader struct{}

func (offlineLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("remote schema reference %q is not allowed", url)
}
