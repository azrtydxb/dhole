package loop_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/dag"
	"github.com/azrtydxb/dhole/internal/dynamic"
	"github.com/azrtydxb/dhole/internal/steps/loop"
)

// bodyJSON is a loop body as an author writes it into `config.body`: a
// dhole.v1.Pipeline, the same document `dhole pipeline create --definition`
// takes. The step it contains dispatches to an ENGINE, which is the whole
// reason ADR 0022 exists — a single builtin reference could never name one.
const bodyJSON = `{
  "steps": [
    {
      "id": "work",
      "plugin_ref": "command:{\"args\":[\"/bin/sh\",\"-c\",\"printf '{}' > outputs/out\"]}",
      "effect_class": "EFFECT_CLASS_IDEMPOTENT",
      "outputs": [{"name": "out", "type": {"blob": {"media_type": "application/json"}}}]
    }
  ]
}`

// controllerStep is the controller as it stands in a pipeline: a bound, a condition
// and a body.
func controllerStep(id string, iteration int) *dholev1.Step {
	cfg := map[string]string{
		loop.ConfigMaxIterations: "3",
		loop.ConfigExitCondition: "state.work.done",
		loop.ConfigBody:          bodyJSON,
	}
	if iteration > 1 {
		cfg[loop.ConfigLoop] = id
		cfg[loop.ConfigIteration] = strconv.Itoa(iteration)
	}
	return &dholev1.Step{
		Id:          loop.ControllerID(id, iteration),
		PluginRef:   loop.PluginRef,
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		Config:      cfg,
	}
}

// TestAnIterationOfALoopIsRealisedAsAFragmentCarryingTheIterationsOwnStepIds is
// ADR 0022's decision in one assertion: the body is not run by the plane, it
// becomes STEPS OF THE RUN, prefixed with the loop's id and the iteration
// number so that the same body realised twice is two different steps.
func TestAnIterationOfALoopIsRealisedAsAFragmentCarryingTheIterationsOwnStepIds(t *testing.T) {
	spec, err := loop.ParseSpec(controllerStep("refine", 1))
	require.NoError(t, err)

	fragment, err := spec.Fragment()
	require.NoError(t, err)

	require.Equal(t, "refine.1.work", fragment.GetSteps()[0].GetId(),
		"an iteration's body step must carry the loop and the iteration, or a second "+
			"iteration would collide with the first")
	require.Equal(t, "command:{\"args\":[\"/bin/sh\",\"-c\",\"printf '{}' > outputs/out\"]}",
		fragment.GetSteps()[0].GetPluginRef(),
		"the body step must reach the run unchanged, or it could not dispatch to an engine")
}

// TestTheFragmentEndsInTheControllerThatDecidesTheNextIteration. Without that
// step nothing would ever ask for iteration two, and without the edge into it
// the scheduler would run it beside the body instead of after it — realising
// every iteration at once.
func TestTheFragmentEndsInTheControllerThatDecidesTheNextIteration(t *testing.T) {
	spec, err := loop.ParseSpec(controllerStep("refine", 1))
	require.NoError(t, err)

	fragment, err := spec.Fragment()
	require.NoError(t, err)

	next := stepByID(t, fragment, "refine.2")
	require.Equal(t, loop.PluginRef, next.GetPluginRef())
	require.Equal(t, "2", next.GetConfig()[loop.ConfigIteration])
	require.Equal(t, "refine", next.GetConfig()[loop.ConfigLoop])
	require.Equal(t, dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE, next.GetEffectClass(),
		"the controller inherits the loop's effect class, or the ceiling of an "+
			"AT_MOST_ONCE loop would be retried automatically")

	require.Len(t, fragment.GetEdges(), 1)
	edge := fragment.GetEdges()[0]
	require.Equal(t, "refine.1.work", edge.GetFromStep())
	require.Equal(t, "out", edge.GetFromPort())
	require.Equal(t, "refine.2", edge.GetToStep())
	require.Equal(t, "work", edge.GetToPort(),
		"the next controller takes one input per body exit step, named after it")
}

// TestTwoIterationsOfOneLoopCannotRealiseTheSameStepId is the uniqueness ADR
// 0022 requires. dynamic.Splice refuses a duplicate by name, so a scheme that
// could collide would stop a run dead at its second iteration.
func TestTwoIterationsOfOneLoopCannotRealiseTheSameStepId(t *testing.T) {
	seen := map[string]bool{}
	for n := 1; n <= 3; n++ {
		spec, err := loop.ParseSpec(controllerStep("refine", n))
		require.NoError(t, err)
		fragment, err := spec.Fragment()
		require.NoError(t, err)
		for _, s := range fragment.GetSteps() {
			require.False(t, seen[s.GetId()],
				"iteration %d realised step %q, which an earlier iteration already realised", n, s.GetId())
			seen[s.GetId()] = true
		}
	}
}

// TestEveryIterationSplicesIntoTheRunItIsRealisedIn proves the fragment
// against the machinery that will actually take it: the run's graph grows one
// iteration at a time and stays a DAG whose ports line up.
func TestEveryIterationSplicesIntoTheRunItIsRealisedIn(t *testing.T) {
	pipeline := &dholev1.Pipeline{
		Id:     "iterating",
		Tenant: &dholev1.Tenant{Id: "default"},
		Steps:  []*dholev1.Step{controllerStep("refine", 1)},
	}

	at := "refine"
	for n := 1; n <= 3; n++ {
		spec, err := loop.ParseSpec(stepByID(t, pipeline, at))
		require.NoError(t, err)
		fragment, err := spec.Fragment()
		require.NoError(t, err)

		pipeline, err = dynamic.Splice(pipeline, at, fragment)
		require.NoError(t, err, "iteration %d did not splice into the run it belongs to", n)
		at = loop.ControllerID("refine", n+1)
	}

	_, err := dag.Build(pipeline)
	require.NoError(t, err)
	require.Empty(t, dag.TypeCheck(pipeline))
	require.Len(t, pipeline.GetSteps(), 7,
		"one authored loop plus three body steps and three controllers")
}

// TestALoopBodyWhoseExitStepDeclaresNoOutputIsRefused. The next iteration is
// ordered after this one by an EDGE, and an edge needs a port at both ends: a
// body that ends in a step producing nothing would leave the next controller a
// root, and the scheduler would run every iteration at once.
func TestALoopBodyWhoseExitStepDeclaresNoOutputIsRefused(t *testing.T) {
	step := controllerStep("refine", 1)
	step.Config[loop.ConfigBody] = `{"steps":[{"id":"work","plugin_ref":"command:{}"}]}`

	spec, err := loop.ParseSpec(step)
	require.NoError(t, err)
	_, err = spec.Fragment()
	require.ErrorIs(t, err, loop.ErrSubgraphInvalid)
	require.Contains(t, err.Error(), "work")
}

// TestALoopBodyThatIsNotAPipelineIsRefusedWhereItWasTyped. A body that only
// fails to parse at the moment an iteration is realised is a run that dies
// halfway through.
func TestALoopBodyThatIsNotAPipelineIsRefusedWhereItWasTyped(t *testing.T) {
	step := controllerStep("refine", 1)
	step.Config[loop.ConfigBody] = "not a pipeline"

	_, err := loop.ParseSpec(step)
	require.ErrorIs(t, err, loop.ErrSubgraphInvalid)
}

func stepByID(t *testing.T, p *dholev1.Pipeline, id string) *dholev1.Step {
	t.Helper()
	for _, s := range p.GetSteps() {
		if s.GetId() == id {
			return s
		}
	}
	t.Fatalf("pipeline %q has no step %q", p.GetId(), id)
	return nil
}
