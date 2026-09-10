package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/policy"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// atMostOnceStep is singleStep with the effect class that forbids an automatic
// retry. A failure of it stops the run without failing it: the step is
// recorded as waiting for a person, and the run stays open until one acts.
func atMostOnceStep() *dholev1.Pipeline {
	p := singleStep()
	p.GetSteps()[0].EffectClass = dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE
	return p
}

// TestTwoAdvancesOfAStepBlockedOnItsEffectClassRecordOneAwaitingReplay is
// migration 0020's bug one level down.
//
// recordAwaitingReplay is check-then-act by the same construction complete()
// was: it skips the write when the replay it was handed already showed a
// STEP_AWAITING_REPLAY, and that replay was taken outside the transaction the
// append runs in. Two advances that both replay before either appends both see
// nothing and both write.
//
// This one has a LONGER window to be hit in than the terminal race, not a
// shorter one. A blocked step is re-examined on every pass for as long as the
// run stays open — which is until a person decides, so possibly for days — and
// both triggers keep arriving at it: the 250ms open-run tick, and the status
// of any other step still reporting.
//
// The event is the sentence an operator reads to learn that nothing will move
// until they act. Recorded twice, with two timestamps, it reads as two
// separate standstills of one step, which is a thing that never happened.
func TestTwoAdvancesOfAStepBlockedOnItsEffectClassRecordOneAwaitingReplay(t *testing.T) {
	ctx := testContext(t)
	h := newHarnessWith(ctx, t, atMostOnceStep(), readyEngine("engine-1"))

	// The run as an operator would find it: the step was dispatched, its
	// attempt failed, and its effect class forbids another.
	dispatched, err := scheduler.MarshalDispatched(scheduler.Dispatched{Attempt: 1})
	require.NoError(t, err)
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID: testRun, StepID: "a", Attempt: 1,
		Type: runstore.StepDispatched, Payload: dispatched, At: time.Now().UTC(),
	}))
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID: testRun, StepID: "a", Attempt: 1,
		Type: runstore.StepFailed, At: time.Now().UTC(),
	}))

	sched := h.planeOverStore(ctx, t, newReplayBarrier(h.store, 2))
	requireConcurrentAdvancesSucceed(ctx, t, sched)

	require.Equal(t, 1, h.countEvents(ctx, t, scheduler.StepAwaitingReplay, "a"),
		"both advances found the step blocked; only one may say so in the log")
}

// TestTwoAdvancesOfAPolicyDeniedStepRecordOneRefusal is the third of the
// check-then-act shapes, and the one with no guard at all: deny writes
// unconditionally, and what stands in for a check is the readiness the replay
// reported. Two advances that replay the same log both find the step ready,
// both put it to policy, and both record the refusal.
//
// A refusal is the ONLY trace a denied step leaves — nothing was dispatched
// and no status will ever come back — so it is the whole of what the log can
// tell a person about why the run stopped. Two of them says the rule refused
// this step twice, and the run failed once.
func TestTwoAdvancesOfAPolicyDeniedStepRecordOneRefusal(t *testing.T) {
	ctx := testContext(t)
	h := newPolicyHarness(ctx, t, policyOptions{pipeline: unsignedOnly()})

	sched := h.policyPlaneOverStore(ctx, t, newReplayBarrier(h.store, 2))
	requireConcurrentAdvancesSucceed(ctx, t, sched)

	require.Equal(t, 1, h.countEvents(ctx, t, scheduler.StepPolicyDenied, unsignedStep),
		"both advances put the same step to the same policy; only one refusal may be recorded")
}

// unsignedOnly is one step naming the plugin the test policy refuses, so the
// first ready step is the denied one and the race is over the denial rather
// than over which step got there first.
func unsignedOnly() *dholev1.Pipeline {
	p := supplyChain()
	p.Steps = []*dholev1.Step{p.GetSteps()[1]}
	return p
}

// requireConcurrentAdvancesSucceed runs two advances of the same run at once
// and insists both return cleanly. Losing the race is not an error: the loser
// of a decision that has already been recorded needs to learn nothing.
func requireConcurrentAdvancesSucceed(
	ctx context.Context, t *testing.T, sched *scheduler.Scheduler,
) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = sched.Advance(ctx, testTenant, testRun)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
}

// policyPlaneOverStore is planeOverStore with the dispatch-time policy wired,
// so a test can put a seam in the store a POLICY decision is read through.
func (h *policyHarness) policyPlaneOverStore(
	_ context.Context, t *testing.T, store runstore.Store,
) *scheduler.Scheduler {
	t.Helper()
	engine, err := policy.New(tenantSource{tenantID: testTenant, p: signedOnlyPolicy()}, h.audit)
	require.NoError(t, err)

	sched, err := scheduler.New(scheduler.Config{
		Store:       store,
		Outbox:      h.outbox,
		Leases:      h.leases,
		Fleet:       h.fleet,
		Definitions: h.defs,
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		EnvIdentity: "sha256:env",
		Policy:      engine,
		Provenance:  h.prov,
		Revisions: staticRevisions{lockfile: map[string]string{
			signedRef:   signedDigest,
			unsignedRef: unsignedDigest,
		}},
	})
	require.NoError(t, err)
	return sched
}
