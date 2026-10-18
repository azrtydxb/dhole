// A loop body spliced into its own run: ADR 0022, asked of the whole plane.
//
// Every test here submits a pipeline whose loop body contains a step with a
// `command:` reference. No builtin claims one, so a body step that runs at all
// has been dispatched over the bus to an engine — which is the thing a body of
// one `builtin:` reference could never do, and the reason the decision was
// taken.
package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/dynamic"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/steps/loop"
)

// TestALoopBodyThatDispatchesToAnEngineRunsOnTheEngineOncePerIteration is the
// whole point of ADR 0022.
//
// The body is not a builtin the plane could run itself: it is a `command:`
// step, which reaches an engine or it does not run. Two iterations means two
// steps in the run's graph, under two different ids, each dispatched and each
// reporting bytes back into the content-addressed store.
func TestALoopBodyThatDispatchesToAnEngineRunsOnTheEngineOncePerIteration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)

	// The condition is about the pass that has just finished, so `iteration >=
	// 2` is two passes and no more — neither the first nor the ceiling.
	runID, err := srv.Submit(ctx, tenantID,
		engineLoopPipeline("loop-on-an-engine", "spin", 4, "iteration >= 2", `{"done":false}`, 0))
	require.NoError(t, err)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "spin.1.work")
	requireStepSucceeded(t, events, "spin.2.work")
	require.Equal(t, `{"done":false}`,
		string(outputBytes(ctx, t, srv, events, "spin.1.work", "out")),
		"the body's bytes have to come back through the CAS, or no engine ran it")

	require.False(t, hasEvent(events, "spin.3.work", runstore.StepDispatched),
		"a third iteration was realised after the condition held; log: %s", describe(events))
	requireEvent(t, events, "spin", loop.EventExited)
}

// TestMaxIterationsBoundsTheNumberOfIterationsALoopAddsToTheRunsGraph is the
// consequence ADR 0022 calls out: the ceiling now bounds the SIZE OF THE GRAPH
// and not merely the number of attempts, because every iteration adds its body
// to the run. A loop at its ceiling stops realising, and the run ends.
func TestMaxIterationsBoundsTheNumberOfIterationsALoopAddsToTheRunsGraph(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)

	// A condition the body never satisfies: this loop can only end at its
	// ceiling, which is what makes the ceiling testable. The state it is asked
	// about comes out of the ENGINE's own output.
	runID, err := srv.Submit(ctx, tenantID,
		engineLoopPipeline("loop-at-its-ceiling", "spin", 2, "state.work.done", `{"done":false}`, 0))
	require.NoError(t, err)

	awaitStepEvent(ctx, t, srv, runID, loop.ControllerID("spin", 3), runstore.StepFailed)
	events, err := srv.Events(ctx, tenantID, runID)
	require.NoError(t, err)

	requireEvent(t, events, "spin", loop.EventCeilingReached)
	require.Equal(t, 2, countEvents(events, dynamic.EventFragmentRealised),
		"max_iterations is two, so the run's graph grew twice and no more; log: %s",
		describe(events))
	requireStepSucceeded(t, events, "spin.2.work")
	require.False(t, hasEvent(events, "spin.3.work", runstore.StepDispatched),
		"the loop realised an iteration past its ceiling; log: %s", describe(events))
}

// TestARestartedPlaneContinuesALoopFromTheFragmentsInItsLogRatherThanRealisingThemAgain
// is ADR 0003 applied to a graph that grows while it runs.
//
// The plane that realised the first iterations is GONE. The one that takes
// over holds no loop, no body and no memory of the run: if it rebuilt the
// graph from the pinned definition it would find one loop step, already
// succeeded, nothing ready — and complete the run while the body steps it
// realised had never run. Green, and wrong. The proof that nothing was
// realised twice is that each controller recorded exactly one fragment.
func TestARestartedPlaneContinuesALoopFromTheFragmentsInItsLogRatherThanRealisingThemAgain(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	dir := t.TempDir()
	first := startEmbeddedOn(ctx, t, dir)

	runID, err := first.Submit(ctx, tenantID,
		engineLoopPipeline("loop-across-a-restart", "spin", 4, "iteration >= 3", `{"done":false}`, 2*time.Second))
	require.NoError(t, err)

	// Stop the plane the moment the loop has grown the run once and its
	// body step is in flight. Waiting for that dispatch rather than for the
	// step to succeed is what keeps the stop deterministic: the next thing
	// after a body success is the AT_MOST_ONCE controller, and a controller
	// killed mid-decision is a lost attempt the restarted plane is forbidden
	// to retry automatically. A lost body attempt is IDEMPOTENT and is
	// re-dispatched, which is the scenario this test is about. Everything
	// after this is the second plane's work, read off the log alone.
	awaitStepEvent(ctx, t, first, runID, "spin.1.work", runstore.StepDispatched)
	stopPlane(t, first)

	second := startEmbeddedOn(ctx, t, dir)
	events := awaitRunCompleted(ctx, t, second, runID)

	requireStepSucceeded(t, events, "spin.1.work")
	requireStepSucceeded(t, events, "spin.2.work")
	requireStepSucceeded(t, events, "spin.3.work")
	require.Equal(t, 3, countEvents(events, dynamic.EventFragmentRealised),
		"three iterations, three fragments: a fragment realised twice is a body that ran twice; "+
			"log: %s", describe(events))
	for _, controller := range []string{"spin", "spin.2", "spin.3"} {
		require.Equal(t, 1, countStepEvents(events, controller, dynamic.EventFragmentRealised),
			"controller %q realised its iteration more than once; log: %s",
			controller, describe(events))
	}
}

// engineLoopPipeline is a loop whose body is a FRAGMENT — a dhole.v1.Pipeline
// in `config.body` — containing one step no builtin can claim.
//
// linger, when non-zero, prefixes the body's write with a sleep. It exists so a
// test can stop the plane while an IDEMPOTENT body step is genuinely in flight
// rather than racing the controller that dispatches it: a controller is
// AT_MOST_ONCE, and one lost mid-decision is exactly the attempt a restarted
// plane may not retry automatically.
func engineLoopPipeline(
	id, stepID string, ceiling int, condition, answer string, linger time.Duration,
) *dholev1.Pipeline {
	// Marshalled rather than typed out: the reference carries JSON, the answer
	// carries JSON, and the body that holds both is JSON. Every level of that
	// escaping was got wrong by hand first, and the run stopped dead with the
	// scheduler unable to decode the step it had been handed.
	work := fmt.Sprintf("printf %%s %s > outputs/out", shellQuote(answer))
	if linger > 0 {
		work = fmt.Sprintf("sleep %d; %s", int(linger.Seconds()), work)
	}
	args, err := json.Marshal(map[string]any{
		"args": []string{"/bin/sh", "-c", work},
	})
	if err != nil {
		panic(err)
	}
	body := fmt.Sprintf(`{
      "steps": [
        {
          "id": "work",
          "plugin_ref": %s,
          "effect_class": "EFFECT_CLASS_IDEMPOTENT",
          "outputs": [{"name": "out", "type": {"blob": {"media_type": "application/json"}}}]
        }
      ]
    }`, quoteJSON("command:"+string(args)))

	return &dholev1.Pipeline{
		Id:     id,
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:        stepID,
			PluginRef: loop.PluginRef,
			// AT_MOST_ONCE, because a loop that spent its whole allowance
			// without its condition holding will spend it again: repeating it
			// automatically is the same ending bought twice.
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
			Config: map[string]string{
				loop.ConfigMaxIterations: fmt.Sprint(ceiling),
				loop.ConfigExitCondition: condition,
				loop.ConfigBody:          body,
			},
		}},
	}
}

// quoteJSON renders a string as a JSON string literal, which is what a
// plugin_ref carrying its own JSON needs to survive being embedded in a body
// document.
func quoteJSON(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
}

// shellQuote wraps a value for /bin/sh, where the answer's own braces and
// quotes would otherwise be the shell's business rather than the body's.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
