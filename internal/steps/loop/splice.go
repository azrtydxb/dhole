package loop

import (
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/dag"
)

// The splicing half of this package: ADR 0022.
//
// A loop body used to be ONE builtin reference, because the definition format
// has no syntax for a subgraph and no run can contain another — so a body that
// dispatched work to an engine had nowhere to put it. The obvious repair is a
// child run per iteration, and that is a change to what a run IS: ADR 0003
// makes a run one state machine over one event log, and the terminal-event
// index, the fair queue, `open_runs` and the SSE stream all assume it.
//
// So an iteration is spliced into the SAME run instead, through the generator
// machinery that already grows a graph at runtime (internal/dynamic). Three
// things follow, and each is a decision rather than an accident.
//
// THE BODY IS A FRAGMENT, NOT A REFERENCE. `config.body` is a dhole.v1.Pipeline
// as JSON — the same document `dhole pipeline create --definition` takes — so a
// body step is an ORDINARY step: it may name an image, an engine type, a
// `command:` reference, anything the scheduler can dispatch.
//
// EVERY ITERATION CARRIES ITS OWN IDS. Step ids are unique within a run and
// dynamic.Splice refuses a duplicate by name, so a body step `build` inside
// loop `refine` becomes `refine.3.build` at the third pass. It is uglier than a
// nested run's naming would have been and it is the price of one graph.
//
// THE NEXT ITERATION IS DECIDED BY A STEP IN THIS ONE. A loop step runs once,
// like every other step, so the fragment it realises ENDS in another loop step
// — the controller for the next pass — wired after the body by a real edge.
// Without that edge the next controller would be a root of the graph and the
// scheduler would run it beside the body it is supposed to follow, realising
// every iteration at once. That is also why an exit step of the body must
// declare an output: an edge needs a port at both ends.

// The keys a loop step carries in its config. They are a public contract —
// what a pipeline author types — so they are added to, never renamed.
const (
	// ConfigBody is the body, as a dhole.v1.Pipeline in JSON.
	ConfigBody = "body"
	// ConfigMaxIterations is the hard ceiling. Positive, always. Since ADR
	// 0022 it bounds the SIZE OF THE GRAPH and not merely the number of
	// attempts: each iteration adds its body to the run.
	ConfigMaxIterations = "max_iterations"
	// ConfigExitCondition is the CEL expression asked after each pass.
	ConfigExitCondition = "exit_condition"
	// ConfigLoop names the AUTHORED loop step a spliced controller belongs to,
	// so every event of one loop lands under one step id however many
	// controllers the run grew. The plane writes it; an author does not.
	ConfigLoop = "loop"
	// ConfigIteration is which pass a controller decides, 1-based. Absent
	// means the first, which is the only one an author ever writes.
	ConfigIteration = "iteration"
)

// Spec is one loop controller as it stands in a run: the bound, the condition,
// the body, and which pass this particular controller decides.
type Spec struct {
	// Loop is the AUTHORED loop step's id, which every controller of the same
	// loop shares. It is what iteration ids are prefixed with.
	Loop string
	// Iteration is the pass this controller decides, 1-based. It may be one
	// past Max: that controller realises nothing and reports the ceiling.
	Iteration int
	// Max is the ceiling from the config.
	Max int
	// ExitCondition is the author's CEL expression, already compiled.
	ExitCondition *Condition
	// Body is the fragment one iteration realises.
	Body *dholev1.Pipeline
	// EffectClass is the authored loop's, carried onto every controller: an
	// AT_MOST_ONCE loop whose continuation was idempotent would have its
	// ceiling retried automatically, which is exactly what that class refuses.
	EffectClass dholev1.EffectClass
}

// ParseSpec reads a loop controller's configuration off the step.
//
// Everything that can be refused is refused HERE — the ceiling, the body, the
// condition — rather than at the pass that trips over it. A body that only
// fails to parse when iteration three is realised is a run that dies halfway
// through with two thirds of its work already done.
func ParseSpec(step *dholev1.Step) (Spec, error) {
	cfg := step.GetConfig()
	spec := Spec{
		Loop:        strings.TrimSpace(cfg[ConfigLoop]),
		Iteration:   1,
		EffectClass: step.GetEffectClass(),
	}
	if spec.Loop == "" {
		// The authored step: it is its own loop, and the first controller.
		spec.Loop = step.GetId()
	}
	if raw := strings.TrimSpace(cfg[ConfigIteration]); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return Spec{}, fmt.Errorf("loop: %q is not an iteration number of loop %q",
				raw, spec.Loop)
		}
		spec.Iteration = n
	}

	ceiling, err := strconv.Atoi(strings.TrimSpace(cfg[ConfigMaxIterations]))
	if err != nil {
		return Spec{}, fmt.Errorf("%w: a loop step's %s %q: %s",
			ErrUnbounded, ConfigMaxIterations, cfg[ConfigMaxIterations], err.Error())
	}
	if ceiling <= 0 {
		return Spec{}, fmt.Errorf("%w: %d is not a ceiling", ErrUnbounded, ceiling)
	}
	spec.Max = ceiling

	condition, err := NewCondition(cfg[ConfigExitCondition])
	if err != nil {
		return Spec{}, err
	}
	spec.ExitCondition = condition

	body, err := ParseBody(cfg[ConfigBody])
	if err != nil {
		return Spec{}, err
	}
	spec.Body = body
	return spec, nil
}

// ParseBody decodes and validates a loop body.
//
// The body is validated on its OWN, as a pipeline, which is what keeps a cycle
// inside a body from being a cycle nobody checked: it is spliced into the run
// one iteration at a time, and by then the authored graph has long since
// passed its own check.
func ParseBody(raw string) (*dholev1.Pipeline, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("%w: a loop with no `%s` has nothing to repeat", ErrSubgraphInvalid, ConfigBody)
	}
	body := &dholev1.Pipeline{}
	if err := protojson.Unmarshal([]byte(raw), body); err != nil {
		return nil, fmt.Errorf("%w: `%s` is not a dhole.v1.Pipeline as JSON: %s",
			ErrSubgraphInvalid, ConfigBody, err.Error())
	}
	if len(body.GetSteps()) == 0 {
		return nil, fmt.Errorf("%w: the body has no steps", ErrSubgraphInvalid)
	}
	if _, err := dag.Build(body); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrSubgraphInvalid, err.Error())
	}
	if diags := dag.TypeCheck(body); len(diags) > 0 {
		return nil, fmt.Errorf("%w: %s.%s: %s", ErrSubgraphInvalid,
			diags[0].StepID, diags[0].PortName, diags[0].Message)
	}
	return body, nil
}

// ControllerID is the step id of the controller that decides iteration n. The
// first is the AUTHORED step itself, so an ordinary one-pass loop grows no
// extra id at all.
func ControllerID(loopID string, n int) string {
	if n <= 1 {
		return loopID
	}
	return fmt.Sprintf("%s.%d", loopID, n)
}

// BodyStepID is what a body step is called in the run, at this iteration.
//
// The three parts are what make it unique: the loop, the pass, and the
// author's own id. Two ids from one loop can only collide if the pass numbers
// match, and a pass is realised once — dynamic.Realise reads the fragment back
// out of the log rather than emitting a second one. A collision with an
// AUTHORED step of the same name is still possible in principle, and
// dynamic.Splice refuses it by name rather than overwriting somebody's step.
func (s Spec) BodyStepID(local string) string {
	return fmt.Sprintf("%s.%d.%s", s.Loop, s.Iteration, local)
}

// At is this same loop at another pass. It is what lets a controller ask about
// the iteration BEFORE it — the one whose state the exit condition is about —
// without reparsing a config it already holds.
func (s Spec) At(n int) Spec {
	s.Iteration = n
	return s
}

// ExitSteps are the body steps nothing inside the body consumes. They are what
// the next iteration waits for.
func (s Spec) ExitSteps() []*dholev1.Step {
	consumed := make(map[string]bool, len(s.Body.GetEdges()))
	for _, e := range s.Body.GetEdges() {
		consumed[e.GetFromStep()] = true
	}
	var exits []*dholev1.Step
	for _, step := range s.Body.GetSteps() {
		if !consumed[step.GetId()] {
			exits = append(exits, step)
		}
	}
	return exits
}

// Fragment is what this iteration adds to the run: the body under this
// iteration's ids, plus the controller that will decide the next pass.
//
// It is a fragment in internal/dynamic's sense and goes through that package's
// Realise, so it is recorded in the run log before it is scheduled and a
// restarted plane rebuilds the same graph from the record instead of realising
// the body a second time.
func (s Spec) Fragment() (*dholev1.Pipeline, error) {
	if s.Iteration > s.Max {
		return nil, fmt.Errorf("%w: loop %q has no iteration %d to realise",
			ErrIterationCeiling, s.Loop, s.Iteration)
	}

	fragment := &dholev1.Pipeline{
		Id:     fmt.Sprintf("%s.%d", s.Loop, s.Iteration),
		Tenant: s.Body.GetTenant(),
	}
	for _, step := range s.Body.GetSteps() {
		cloned, ok := proto.Clone(step).(*dholev1.Step)
		if !ok {
			return nil, fmt.Errorf("loop: cloning body step %q did not yield a step", step.GetId())
		}
		cloned.Id = s.BodyStepID(step.GetId())
		fragment.Steps = append(fragment.Steps, cloned)
	}
	for _, edge := range s.Body.GetEdges() {
		cloned, ok := proto.Clone(edge).(*dholev1.Edge)
		if !ok {
			return nil, fmt.Errorf("loop: cloning a body edge did not yield an edge")
		}
		cloned.FromStep = s.BodyStepID(edge.GetFromStep())
		cloned.ToStep = s.BodyStepID(edge.GetToStep())
		fragment.Edges = append(fragment.Edges, cloned)
	}

	next, err := s.controller()
	if err != nil {
		return nil, err
	}
	fragment.Steps = append(fragment.Steps, next)
	for _, exit := range s.ExitSteps() {
		port := exit.GetOutputs()[0]
		fragment.Edges = append(fragment.Edges, &dholev1.Edge{
			FromStep: s.BodyStepID(exit.GetId()), FromPort: port.GetName(),
			ToStep: next.GetId(), ToPort: exit.GetId(),
		})
	}
	return fragment, nil
}

// controller builds the loop step that decides the pass after this one.
//
// Its inputs are the body's exits, one port each, typed exactly as the output
// they are fed from — the edge is the ONLY thing that orders the next
// iteration after this one, and dag.TypeCheck refuses an edge whose ends do
// not line up. A body exit that declares no output is therefore refused here,
// where the author can still be told which step it was.
func (s Spec) controller() (*dholev1.Step, error) {
	cfg := map[string]string{
		ConfigBody:          "",
		ConfigMaxIterations: strconv.Itoa(s.Max),
		ConfigExitCondition: s.ExitCondition.Expression(),
		ConfigLoop:          s.Loop,
		ConfigIteration:     strconv.Itoa(s.Iteration + 1),
	}
	encoded, err := protojson.Marshal(s.Body)
	if err != nil {
		return nil, fmt.Errorf("loop: encoding the body of %q: %w", s.Loop, err)
	}
	cfg[ConfigBody] = string(encoded)

	next := &dholev1.Step{
		Id:          ControllerID(s.Loop, s.Iteration+1),
		Name:        fmt.Sprintf("%s iteration %d", s.Loop, s.Iteration+1),
		PluginRef:   PluginRef,
		EffectClass: s.EffectClass,
		Config:      cfg,
	}
	for _, exit := range s.ExitSteps() {
		outputs := exit.GetOutputs()
		if len(outputs) == 0 {
			return nil, fmt.Errorf(
				"%w: body step %q ends the body and declares no output, so nothing could order "+
					"the next iteration after it",
				ErrSubgraphInvalid, exit.GetId())
		}
		port, ok := proto.Clone(outputs[0]).(*dholev1.Port)
		if !ok {
			return nil, fmt.Errorf("loop: cloning port %q did not yield a port", outputs[0].GetName())
		}
		// Named after the STEP rather than after its port: two exits could
		// each declare an output called `out`, and the controller cannot
		// declare that port twice.
		port.Name = exit.GetId()
		next.Inputs = append(next.Inputs, port)
	}
	return next, nil
}
