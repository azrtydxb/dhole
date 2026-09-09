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
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

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
