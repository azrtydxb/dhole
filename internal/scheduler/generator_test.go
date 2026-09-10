package scheduler_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/dynamic"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

const generatorStep = "fan-out"

// TestARestartedPlaneSchedulesTheStepsTheGeneratorRealisedInsteadOfCompletingWithoutThem
// is ADR 0003 applied to the one step type that can invent work.
//
// The plane that expanded the generator is GONE. This scheduler holds no
// fragment, no generator and no memory of the run at all: the only thing it
// has is the log. If it rebuilt the run's graph from the pinned definition
// alone it would find one step, already succeeded, nothing ready — and it
// would call the run COMPLETE while the three steps the generator realised
// had never run. That failure is silent and green, which is why it is the
// case this test exists for.
//
// Nothing here asks a generator anything. The scheduler cannot: it has no
// Emit and no way to reach one. Determinism is not a policy it follows, it is
// the only thing it can do — the graph comes out of the recorded fragment or
// it does not exist.
func TestARestartedPlaneSchedulesTheStepsTheGeneratorRealisedInsteadOfCompletingWithoutThem(
	t *testing.T,
) {
	ctx := testContext(t)
	h := newHarnessWith(ctx, t, generatorPipeline(), readyEngine("engine-1"))

	// The plane that is now gone: it expanded the generator, recorded what it
	// emitted, and saw the step through.
	realiseFragment(ctx, t, h, shardFragment("shard-a", "shard-b", "shard-c"))
	finishGenerator(ctx, t, h)

	// A second control plane over the same store, holding nothing.
	restarted := h.secondPlane(ctx, t)
	require.NoError(t, restarted.Advance(ctx, testTenant, testRun))

	require.ElementsMatch(t,
		[]string{"shard-a", "shard-b", "shard-c"}, h.drain(ctx, t),
		"the restarted plane ran the graph the run actually had")

	completed := eventsOfType(ctx, t, h, runstore.RunCompleted)
	require.Empty(t, completed,
		"a run whose realised steps have not finished is not a completed run")
}

// TestAFragmentInTheLogIsSplicedTheSameWayOnEveryAdvance. Advance is called
// after every status and by several planes at once, so the graph it derives
// has to be the same graph every time. A splice that appended a second copy
// of the fragment — or that ran at all on the second pass — would dispatch a
// step twice under one attempt, and the duplicate would look like an engine
// that reported late.
func TestAFragmentInTheLogIsSplicedTheSameWayOnEveryAdvance(t *testing.T) {
	ctx := testContext(t)
	h := newHarnessWith(ctx, t, generatorPipeline(), readyEngine("engine-1"))

	realiseFragment(ctx, t, h, shardFragment("shard-a", "shard-b"))
	finishGenerator(ctx, t, h)

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	first := h.drain(ctx, t)
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))

	require.Equal(t, first, h.drain(ctx, t),
		"advancing again over the same log dispatched nothing further")
	require.ElementsMatch(t, []string{"shard-a", "shard-b"}, first)
}

// TestARunWhoseRecordedFragmentCannotBeSplicedDoesNotRunTheAuthoredGraph
// Instead. A fragment in the log is a statement about what this run's graph
// IS. If it cannot be put back — a definition edited underneath the run, a
// payload that will not decode — the honest answer is to stop and say so.
// Falling back to the authored graph would run a DIFFERENT pipeline from the
// one the run had, under the same run id, and report it as that run.
func TestARunWhoseRecordedFragmentCannotBeSplicedDoesNotRunTheAuthoredGraphInstead(t *testing.T) {
	ctx := testContext(t)
	h := newHarnessWith(ctx, t, generatorPipeline(), readyEngine("engine-1"))

	// A fragment reusing the authored generator's own id: Splice refuses it,
	// which is the same refusal any collision gets.
	realiseFragment(ctx, t, h, shardFragment(generatorStep))
	finishGenerator(ctx, t, h)

	err := h.sched.Advance(ctx, testTenant, testRun)
	require.Error(t, err)
	require.ErrorIs(t, err, dynamic.ErrDuplicateStepID)
	require.Contains(t, err.Error(), testRun)
	require.Empty(t, h.drain(ctx, t))
}

// --- fixtures -----------------------------------------------------------

// generatorPipeline is what a person authored: one generator, declaring the
// port its fragment will hang off. Nothing in it says what will run.
func generatorPipeline() *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     testPipeline,
		Tenant: &dholev1.Tenant{Id: testTenant},
		Steps: []*dholev1.Step{{
			Id:          generatorStep,
			Name:        generatorStep,
			PluginRef:   dynamic.PluginRef,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
			Outputs:     []*dholev1.Port{blobPort("shard")},
		}},
	}
}

// shardFragment is what a generator emitted: independent steps, each
// consuming the generator's declared output port by name.
func shardFragment(ids ...string) *dholev1.Pipeline {
	p := &dholev1.Pipeline{Id: "fragment"}
	for _, id := range ids {
		p.Steps = append(p.Steps, &dholev1.Step{
			Id:          id,
			Name:        id,
			PluginRef:   "cmd://echo",
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
			Inputs:      []*dholev1.Port{blobPort("shard")},
		})
	}
	return p
}

func blobPort(name string) *dholev1.Port {
	return &dholev1.Port{Name: name, Type: &dholev1.PortType{
		Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{}},
	}}
}

// realiseFragment expands the generator the way the plane that owned this run
// did: through internal/dynamic, which is what writes the record the
// scheduler later reads. A test that appended a hand-built payload here would
// be proving that the scheduler can read a shape the plane never writes.
func realiseFragment(
	ctx context.Context, t *testing.T, h *harness, fragment *dholev1.Pipeline,
) {
	t.Helper()
	g, err := dynamic.New(dynamic.Options{
		Store: h.store, TenantID: testTenant, MaxExpansions: 4,
		Emit: func(context.Context, dynamic.Input) (*dholev1.Pipeline, error) {
			return fragment, nil
		},
	})
	require.NoError(t, err)
	// A refused splice still records nothing but the failure; the cases that
	// hand in an unspliceable fragment record it deliberately below.
	if _, err := g.Realise(ctx, testRun, generatorStep, generatorPipeline()); err != nil {
		recordRefusedFragment(ctx, t, h, fragment)
	}
}

// recordRefusedFragment writes a realised record for a fragment Splice would
// refuse. It exists for exactly one case — a log written before a definition
// changed underneath the run — and it writes the same payload internal/dynamic
// writes, so the scheduler is still reading the real shape.
func recordRefusedFragment(
	ctx context.Context, t *testing.T, h *harness, fragment *dholev1.Pipeline,
) {
	t.Helper()
	encoded, err := proto.Marshal(fragment)
	require.NoError(t, err)
	// dynamic.Record's own struct, with dynamic's own json tags: a payload
	// hand-shaped here would prove the scheduler can read something the plane
	// never writes.
	payload, err := json.Marshal(dynamic.Record{
		Generator: generatorStep,
		Steps:     []string{fragment.GetSteps()[0].GetId()},
		Encoded:   encoded,
	})
	require.NoError(t, err)
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID:   testRun,
		StepID:  generatorStep,
		Type:    dynamic.EventFragmentRealised,
		Payload: payload,
		At:      time.Now().UTC(),
	}))
}

// finishGenerator writes the generator step through to SUCCEEDED, which is
// what makes the fragment below it ready. The plane runs a builtin itself, so
// there is no engine report to wait for.
func finishGenerator(ctx context.Context, t *testing.T, h *harness) {
	t.Helper()
	dispatched, err := scheduler.MarshalDispatched(scheduler.Dispatched{Attempt: 1})
	require.NoError(t, err)
	status, err := proto.Marshal(&dholev1.JobStatus{
		RunId: testRun, StepId: generatorStep, Phase: dholev1.Phase_PHASE_SUCCEEDED,
	})
	require.NoError(t, err)
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID: testRun, StepID: generatorStep, Attempt: 1,
		Type: runstore.StepDispatched, Payload: dispatched, At: time.Now().UTC(),
	}))
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID: testRun, StepID: generatorStep, Attempt: 1,
		Type: runstore.StepSucceeded, Payload: status, At: time.Now().UTC(),
	}))
}

func eventsOfType(
	ctx context.Context, t *testing.T, h *harness, kind runstore.EventType,
) []runstore.Event {
	t.Helper()
	all, err := h.store.Replay(ctx, testTenant, testRun)
	require.NoError(t, err)
	var out []runstore.Event
	for _, e := range all {
		if e.Type == kind {
			out = append(out, e)
		}
	}
	return out
}
