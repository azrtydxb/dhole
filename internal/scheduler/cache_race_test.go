package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// firstClaimHeld holds the FIRST claim made through it until released and
// lets every later one straight through. A cache hit is the scheduler's one
// Claim, so a plane over it stops exactly where a hit has looked the entry up,
// read the log, and is about to take the step's lease.
type firstClaimHeld struct {
	lease.Manager
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (f *firstClaimHeld) Claim(
	ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration,
) (lease.Token, error) {
	first := false
	f.once.Do(func() { first = true })
	if first {
		close(f.entered)
		<-f.release
	}
	return f.Manager.Claim(ctx, tenantID, runID, stepID, attempt, ttl)
}

// hittingPlane is a scheduler with the harness's cache wired, over leases the
// test chooses.
func (h *cacheHarness) hittingPlane(t *testing.T, leases lease.Manager) *scheduler.Scheduler {
	t.Helper()
	sched, err := scheduler.New(scheduler.Config{
		Store:       h.store,
		Outbox:      h.outbox,
		Leases:      leases,
		Fleet:       h.fleet,
		Definitions: staticDefs{pipeline: chain()},
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		Cache:       h.cache,
		Revisions:   h.revs,
		BlobRefs:    &cas.GC{Store: h.blobs, Runs: h.store, DB: h.db, Dialect: runstore.DialectSQLite},
	})
	require.NoError(t, err)
	return sched
}

// dispatchingPlane is a scheduler over the same store, outbox and fleet with
// NO cache: the pass that looked before the entry existed, so dispatches.
func (h *cacheHarness) dispatchingPlane(t *testing.T, leases lease.Manager) *scheduler.Scheduler {
	t.Helper()
	sched, err := scheduler.New(scheduler.Config{
		Store:       h.store,
		Outbox:      h.outbox,
		Leases:      leases,
		Fleet:       h.fleet,
		Definitions: staticDefs{pipeline: chain()},
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
	})
	require.NoError(t, err)
	return sched
}

// countStep counts one kind of event for one step of one run.
func (h *cacheHarness) countStep(
	ctx context.Context, t *testing.T, runID, stepID string, kinds ...runstore.EventType,
) int {
	t.Helper()
	n := 0
	for _, e := range h.events(ctx, t, runID) {
		if e.StepID != stepID {
			continue
		}
		for _, kind := range kinds {
			if e.Type == kind {
				n++
			}
		}
	}
	return n
}

// TestACacheHitRacingADispatchOfTheSameAttemptLosesNoAttempt is the race the
// offer's compare-and-set left open. One pass found no entry and dispatched a
// as attempt 1; another, which looked the entry up an instant later, serves
// attempt 1 from the cache. When the hit's claim came back after the dispatch
// committed it superseded the committed fence: the hit saw the attempt placed
// and wrote nothing, and the dispatch's engine had every report refused — the
// attempt was declared lost one TTL later and run again for nobody.
func TestACacheHitRacingADispatchOfTheSameAttemptLosesNoAttempt(t *testing.T) {
	const run = "run-race"

	t.Run("the dispatch commits first", func(t *testing.T) {
		ctx := testContext(t)
		h := newCacheHarness(ctx, t, chain())
		h.runChainForReal(ctx, t, "run-real")
		h.seed(ctx, t, run)

		held := &firstClaimHeld{Manager: h.leases, entered: make(chan struct{}), release: make(chan struct{})}
		hitter := h.hittingPlane(t, held)
		dispatcher := h.dispatchingPlane(t, h.leases)

		hit := make(chan error, 1)
		go func() { hit <- hitter.Advance(ctx, testTenant, run) }()
		<-held.entered // the hit has read the log and is claiming a

		require.NoError(t, dispatcher.Advance(ctx, testTenant, run))
		require.Equal(t, []string{"a"}, h.dispatchedSteps(ctx, t, run))
		var committed *dholev1.JobDispatch
		for _, d := range h.bus.dispatches(t) {
			if d.GetRunId() == run && d.GetStepId() == "a" {
				committed = d
			}
		}
		require.NotNil(t, committed)

		close(held.release)
		require.NoError(t, <-hit, "finding the attempt already placed is not an error")

		_, token, err := scheduler.DecodeFence(committed.GetFenceToken())
		require.NoError(t, err)
		require.NoError(t, h.leases.Validate(ctx, token),
			"a cache hit racing a dispatch of the same attempt fenced the committed dispatch out")

		// The engine runs the dispatch it was given and reports, as it would.
		require.NoError(t, dispatcher.OnStatus(ctx, &dholev1.JobStatus{
			RunId: run, StepId: "a", Attempt: committed.GetAttempt(),
			FenceToken: committed.GetFenceToken(), Phase: dholev1.Phase_PHASE_ACCEPTED,
		}))
		h.finish(ctx, t, run, "a", "bytes from a")

		require.Equal(t, 1, h.countStep(ctx, t, run, "a", runstore.StepSucceeded, runstore.StepFailed),
			"the attempt did not end in exactly one terminal outcome; log: %v", eventKinds(h.events(ctx, t, run)))
		require.Equal(t, 1, h.countStep(ctx, t, run, "a", runstore.StepDispatched),
			"one attempt, one dispatch — the hit wrote one too")
		require.Zero(t, h.countStep(ctx, t, run, "a", scheduler.StepAttemptLost))
		_, err = h.sched.SweepOrphans(ctx)
		require.NoError(t, err)
		require.Zero(t, h.countStep(ctx, t, run, "a", scheduler.StepAttemptLost),
			"the hit left a claim behind that the sweeper reads as a dead holder")
	})

	t.Run("the dispatch's offer was withdrawn as it committed", func(t *testing.T) {
		// The residual race: a sweeper that read the log an instant before the
		// commit withdraws the committed offer, and the hit's claim lands on
		// the empty key. The hit must see the attempt placed AND say that the
		// fence it carries is gone, as a dispatch's re-read does.
		ctx := testContext(t)
		h := newCacheHarness(ctx, t, chain())
		h.runChainForReal(ctx, t, "run-real")
		h.seed(ctx, t, run)

		held := &firstClaimHeld{Manager: h.leases, entered: make(chan struct{}), release: make(chan struct{})}
		hitter := h.hittingPlane(t, held)
		dispatcher := h.dispatchingPlane(t, h.leases)

		hit := make(chan error, 1)
		go func() { hit <- hitter.Advance(ctx, testTenant, run) }()
		<-held.entered

		require.NoError(t, dispatcher.Advance(ctx, testTenant, run))
		require.Equal(t, []string{"a"}, h.dispatchedSteps(ctx, t, run))
		_, token, err := scheduler.DecodeFence(h.bus.dispatches(t)[len(h.bus.dispatches(t))-1].GetFenceToken())
		require.NoError(t, err)
		require.NoError(t, h.leases.Withdraw(ctx, token))

		close(held.release)
		require.NoError(t, <-hit)

		require.Equal(t, 1, h.countStep(ctx, t, run, "a", scheduler.StepAttemptLost),
			"a dispatch whose fence the hit's claim took can never report, and nothing said so")
		require.Zero(t, h.countStep(ctx, t, run, "a", runstore.StepSucceeded),
			"the hit wrote a success over an attempt somebody else had dispatched")
	})

	t.Run("the hit claims first", func(t *testing.T) {
		ctx := testContext(t)
		h := newCacheHarness(ctx, t, chain())
		h.runChainForReal(ctx, t, "run-real")
		h.seed(ctx, t, run)

		held := &firstOfferHeld{Manager: h.leases, entered: make(chan struct{}), release: make(chan struct{})}
		dispatcher := h.dispatchingPlane(t, held)
		hitter := h.hittingPlane(t, h.leases)

		dispatch := make(chan error, 1)
		go func() { dispatch <- dispatcher.Advance(ctx, testTenant, run) }()
		<-held.entered // the dispatch has read the log and is offering a

		require.NoError(t, hitter.Advance(ctx, testTenant, run))
		close(held.release)
		require.NoError(t, <-dispatch)

		require.Empty(t, h.dispatchedSteps(ctx, t, run),
			"a step served from the cache was dispatched as well")
		require.Equal(t, 1, h.countStep(ctx, t, run, "a", runstore.StepSucceeded, runstore.StepFailed))
		require.Equal(t, 1, h.countStep(ctx, t, run, "a", runstore.StepDispatched))
	})
}

// TestACacheHitBehindAnotherPassesOfferNeitherServesNorLoops. A hit whose
// claim is refused — another pass holds this very attempt — must leave the
// step to that pass. Serving it anyway fences that pass's dispatch out; and
// counting the refusal as served re-advances the run, which finds the same
// step ready behind the same offer and recurses until the offer goes away.
func TestACacheHitBehindAnotherPassesOfferNeitherServesNorLoops(t *testing.T) {
	const run = "run-behind"
	ctx := testContext(t)
	h := newCacheHarness(ctx, t, chain())
	h.runChainForReal(ctx, t, "run-real")
	h.seed(ctx, t, run)

	// A plane offered a and has not committed — slow, or dead.
	offer, err := h.leases.Offer(ctx, testTenant, run, "a", 1, scheduler.DefaultLeaseTTL)
	require.NoError(t, err)

	require.NoError(t, h.sched.Advance(ctx, testTenant, run))
	require.Zero(t, h.countStep(ctx, t, run, "a", runstore.StepDispatched, runstore.StepSucceeded),
		"a cache hit served an attempt another pass had already offered")
	require.NoError(t, h.leases.Validate(ctx, offer), "the hit took the offering pass's fence")
	require.Empty(t, h.dispatchedSteps(ctx, t, run))

	// Once the offer is withdrawn the attempt is free, and the hit serves it.
	require.NoError(t, h.leases.Withdraw(ctx, offer))
	require.NoError(t, h.sched.Advance(ctx, testTenant, run))
	require.Equal(t, 1, h.countStep(ctx, t, run, "a", runstore.StepSucceeded))
	require.Equal(t, 1, h.countStep(ctx, t, run, "b", runstore.StepSucceeded))
	require.Empty(t, h.dispatchedSteps(ctx, t, run))
}
