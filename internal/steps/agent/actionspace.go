package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/go-ai-sdk/ai"
)

// ActionSpace is the set of steps one agent may invoke, and the ONLY answer to
// "may it run this".
//
// It is a closed set built from named grants, not a filter over everything
// that exists, because the two fail in opposite directions: a filter that is
// wrong grants too much, and a closed set that is wrong grants too little.
// Only one of those is an incident.
type ActionSpace struct {
	// granted maps a granted name to the step definition it names. The
	// definition is held, not just the name, because the effect class decides
	// whether an invocation needs an approval gate and whether tainted data
	// may reach it — and an action whose class is unknown cannot be decided
	// at all.
	granted map[string]*dholev1.Step
	// names, sorted, so every refusal reads the same way twice.
	names []string
}

// NewActionSpace builds the space from the names granted and the catalogue of
// steps that exist.
//
// A grant naming a step the catalogue does not define is refused HERE. The
// alternative — treating it as an unknown step with an unspecified effect
// class — would give the agent an action whose danger nothing can assess, and
// an unspecified class is exactly what taint.Check and the approval rule below
// both treat as effectful. A configuration error is the better failure.
func NewActionSpace(granted []string, catalogue []*dholev1.Step) (*ActionSpace, error) {
	defined := make(map[string]*dholev1.Step, len(catalogue))
	for _, s := range catalogue {
		if s.GetId() == "" {
			return nil, errors.New("agent: the catalogue contains a step with no id")
		}
		defined[s.GetId()] = s
	}

	a := &ActionSpace{granted: make(map[string]*dholev1.Step, len(granted))}
	for _, name := range granted {
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, errors.New("agent: a granted step with an empty name grants nothing")
		}
		step, ok := defined[name]
		if !ok {
			return nil, fmt.Errorf(
				"agent: step %q was granted but is not defined; an agent cannot be given an action "+
					"whose effect class nothing knows", name)
		}
		if _, dup := a.granted[name]; dup {
			continue
		}
		a.granted[name] = step
		a.names = append(a.names, name)
	}
	sort.Strings(a.names)
	return a, nil
}

// Names lists the granted steps, sorted.
func (a *ActionSpace) Names() []string {
	out := make([]string, len(a.names))
	copy(out, a.names)
	return out
}

// Action returns the definition of a granted step.
func (a *ActionSpace) Action(name string) (*dholev1.Step, bool) {
	s, ok := a.granted[name]
	return s, ok
}

// Check is THE action-space check, and every path to running a step goes
// through it: the tool list is built from it, each tool re-asks it as it
// executes, and a name the model invented that never appeared in any tool list
// is answered by it too.
//
// That repetition is deliberate. A check performed only while the tool list is
// built is a check on what the agent was OFFERED, and the thing that has to be
// refused is what the agent ASKED FOR.
//
// The refusal names both sides. "Denied" alone sends whoever reads it to
// reconstruct the grant list by hand from the pipeline definition.
func (a *ActionSpace) Check(name string) error {
	if _, ok := a.granted[name]; ok {
		return nil
	}
	held := "nothing"
	if len(a.names) > 0 {
		held = strings.Join(a.names, ", ")
	}
	return fmt.Errorf("%w: this agent asked to invoke %q; it was granted only [%s]",
		ErrOutsideActionSpace, name, held)
}

// actionSchema is the argument schema every action tool advertises. A granted
// step's real input schema is its ports' business (ADR 0001) and is validated
// where the step runs; the agent's job is to decide WHETHER the call may
// happen, not to re-type-check it.
var actionSchema = json.RawMessage(`{"type":"object","additionalProperties":true}`)

// actionTool exposes one granted step to the model. Its Execute goes through
// Step.Invoke, which is the only path to running anything.
type actionTool struct {
	step   *Step
	action string
	desc   string
	runID  string
	stepID string
	guard  *guard
	// window is the gate's standing decision for the calls a parked loop was
	// stopped on, and nil for a loop that was never parked.
	window *resumeWindow
}

// Compile-time proof an action is the Tool the SDK's loop consumes.
var _ ai.Tool = (*actionTool)(nil)

func (t *actionTool) Name() string                     { return t.action }
func (t *actionTool) Description() string              { return t.desc }
func (t *actionTool) Schema() json.RawMessage          { return actionSchema }
func (t *actionTool) Strict() bool                     { return false }
func (t *actionTool) InputExamples() []json.RawMessage { return nil }
func (t *actionTool) InputCallbacks() ai.ToolInputCallbacks {
	return ai.ToolInputCallbacks{}
}

// Execute runs the action — after Invoke has re-checked the grant, the taint
// and the approval rule. A refusal is recorded on the guard as well as
// returned, because the SDK hands a tool error back to the model as a result
// and carries on: without the guard the agent would be told "no" and left free
// to try the next thing, and the run would end successfully.
func (t *actionTool) Execute(ctx context.Context, args json.RawMessage) (any, error) {
	out, err := t.step.invoke(ctx, Invocation{
		RunID: t.runID, StepID: t.stepID, Action: t.action, Args: args,
	}, t.window)
	if err != nil {
		t.guard.record(err)
		return nil, err
	}
	return out, nil
}
