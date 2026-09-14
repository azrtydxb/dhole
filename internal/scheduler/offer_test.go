package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// offerGrace is how old an unaccepted, never-dispatched offer must be before
// the sweeper withdraws it: the lease TTL, which these harnesses leave at its
// default. The tests step either side of it by a margin that no amount of
// harness start-up time can eat.
const (
	offerGrace  = scheduler.DefaultLeaseTTL
	graceMargin = 5 * time.Second
)

// realClock is a testClock that starts at the wall clock rather than at a
// fixed date. The lease manager stamps an offer with the real time — its clock
// is the server's, not a variable a test can move — so a sweeper judging that
// offer's age has to start from the same instant and be moved from there.
func realClock() *testClock {
	return &testClock{at: time.Now()}
}

func (c *testClock) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = time.Now()
}

// firstOfferHeld holds the FIRST offer made through it until released, and
// lets every later one straight through. One scheduler over it reproduces two
// Advances of one run inside one plane — the open-run tick and the status
// consumer — in the exact order that lost an attempt on kw: the first has read
// the log and is about to offer, and the second dispatches the step before the
// first's offer lands.
type firstOfferHeld struct {
	lease.Manager
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (f *firstOfferHeld) Offer(
	ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration,
) (lease.Token, error) {
	first := false
	f.once.Do(func() { first = true })
	if first {
		close(f.entered)
		<-f.release
	}
	return f.Manager.Offer(ctx, tenantID, runID, stepID, attempt, ttl)
}

// TestTwoAdvancesOfOneRunDispatchAStepOnceAndLoseNoAttempt is the production
// defect. On every step of a real run on kw the log read STEP_DISPATCHED,
// STEP_ATTEMPT_LOST, STEP_DISPATCHED, STEP_SUCCEEDED: both Advances offered
// the step, the second offer superseded the fence the first had already
// dispatched under, and the attempt had to be declared lost and run again — an
// engine running a stale copy for nobody, and for an at-most-once step a
// person asked to approve a replay nothing needed.
func TestTwoAdvancesOfOneRunDispatchAStepOnceAndLoseNoAttempt(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(ctx, t, readyEngine("e1"))

	held := &firstOfferHeld{
		Manager: h.leases,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	plane := h.planeWithLeases(ctx, t, held)

	tick := make(chan error, 1)
	go func() { tick <- plane.Advance(ctx, testTenant, testRun) }()
	<-held.entered // the tick has read the log and is offering a

	// The status consumer advances the same run and dispatches a.
	require.NoError(t, plane.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))

	close(held.release)
	require.NoError(t, <-tick, "finding the step already placed is not an error")

	require.Zero(t, h.countEvents(ctx, t, scheduler.StepAttemptLost, "a"),
		"two Advances of one run cost the step an attempt")
	require.Equal(t, 1, h.countEvents(ctx, t, runstore.StepDispatched, "a"),
		"one attempt, one dispatch")

	// The dispatch that went out is the one its engine can report on.
	committed := h.latestDispatch(t, "a")
	h.report(ctx, t, plane, committed, dholev1.Phase_PHASE_ACCEPTED)
	h.succeed(ctx, t, "a")
	require.NoError(t, plane.Advance(ctx, testTenant, testRun))
	require.Equal(t, 1, h.countEvents(ctx, t, runstore.StepSucceeded, "a"),
		"the committed dispatch's fence was taken by the offer that lost the race")
	require.Equal(t, []string{"a", "b", "c"}, h.drain(ctx, t),
		"a was dispatched again, or its success did not release b and c")
}

// TestAnOfferWhosePlaneDiedBeforeDispatchingIsWithdrawnAndTheStepDispatched is
// the gap refusing a second offer opens. A plane offered a step and died before
// committing the dispatch. Its offer will never be accepted — nothing was sent
// — and may not be replaced, since it is an offer of the very attempt every
// other plane now wants to offer. Unless the sweeper withdraws it, the step is
// never dispatched and the run waits forever.
func TestAnOfferWhosePlaneDiedBeforeDispatchingIsWithdrawnAndTheStepDispatched(t *testing.T) {
	ctx := testContext(t)
	clock := realClock()
	h := newTunedHarness(ctx, t, soloPipeline(dholev1.EffectClass_EFFECT_CLASS_PURE), tuning{now: clock.now})

	// A plane offers a and dies on the spot.
	clock.reset()
	_, err := h.leases.Offer(ctx, testTenant, testRun, "a", 1, scheduler.DefaultLeaseTTL)
	require.NoError(t, err)

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Empty(t, h.drain(ctx, t), "a step was dispatched over another plane's offer of the same attempt")

	clock.advance(offerGrace + graceMargin)
	_, err = h.sched.SweepOrphans(ctx)
	require.NoError(t, err)

	require.Equal(t, []string{"a"}, h.drain(ctx, t),
		"the step behind a dead plane's offer was never dispatched, so the run waits forever")
	d := h.latestDispatch(t, "a")
	require.Equal(t, uint32(1), d.GetAttempt(), "an attempt that never went out is not a lost one")
	require.Zero(t, h.countEvents(ctx, t, scheduler.StepAttemptLost, "a"))

	h.report(ctx, t, h.sched, d, dholev1.Phase_PHASE_ACCEPTED)
	h.succeed(ctx, t, "a")
	require.Equal(t, 1, h.countEvents(ctx, t, runstore.StepSucceeded, "a"),
		"the dispatch sent after the withdrawal carries a fence nobody honours")
}

// TestTheSweeperWithdrawsNoOfferThatWasDispatchedOrIsStillYoung is what the
// withdrawal must not cost. An offer a dispatch went out under is the fence its
// engine reports with, however long it waits for a slot; withdrawing it would
// be the original defect by another route. And an offer younger than the grace
// window may belong to a plane still on its way to committing.
func TestTheSweeperWithdrawsNoOfferThatWasDispatchedOrIsStillYoung(t *testing.T) {
	t.Run("dispatched", func(t *testing.T) {
		ctx := testContext(t)
		clock := realClock()
		h := newTunedHarness(ctx, t, soloPipeline(dholev1.EffectClass_EFFECT_CLASS_PURE), tuning{now: clock.now})

		clock.reset()
		require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
		require.Equal(t, []string{"a"}, h.drain(ctx, t))

		// The step waits in the queue, unaccepted, far past the grace window.
		clock.advance(10 * offerGrace)
		_, err := h.sched.SweepOrphans(ctx)
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, h.drain(ctx, t), "a step waiting in the queue was dispatched again")

		d := h.latestDispatch(t, "a")
		require.NoError(t, h.leases.Validate(ctx, mustFence(t, d)),
			"the sweeper withdrew the offer a queued dispatch carries")
		h.report(ctx, t, h.sched, d, dholev1.Phase_PHASE_ACCEPTED)
		h.succeed(ctx, t, "a")
		require.Equal(t, 1, h.countEvents(ctx, t, runstore.StepSucceeded, "a"))
		require.Zero(t, h.countEvents(ctx, t, scheduler.StepAttemptLost, "a"))
	})

	t.Run("young", func(t *testing.T) {
		ctx := testContext(t)
		clock := realClock()
		h := newTunedHarness(ctx, t, soloPipeline(dholev1.EffectClass_EFFECT_CLASS_PURE), tuning{now: clock.now})

		clock.reset()
		token, err := h.leases.Offer(ctx, testTenant, testRun, "a", 1, scheduler.DefaultLeaseTTL)
		require.NoError(t, err)

		clock.advance(offerGrace - graceMargin)
		_, err = h.sched.SweepOrphans(ctx)
		require.NoError(t, err)
		require.NoError(t, h.leases.Validate(ctx, token),
			"an offer younger than the grace window was withdrawn while its dispatch could still commit")
		require.Empty(t, h.drain(ctx, t))
	})
}

// TestADispatchSlowerThanHalfTheGraceWindowDoesNotCommit is what makes "old
// enough that no commit can still be on its way" true rather than hopeful. The
// sweeper withdraws an offer the log shows was never dispatched once it is
// older than the grace window; a plane that was merely slow, and committed
// just after the sweeper read the log, would have its dispatch's fence
// withdrawn from under it. So a plane refuses to commit a dispatch whose offer
// is already older than half the window, and the sweeper withdraws it later.
func TestADispatchSlowerThanHalfTheGraceWindowDoesNotCommit(t *testing.T) {
	ctx := testContext(t)
	clock := realClock()
	h := newTunedHarness(ctx, t, soloPipeline(dholev1.EffectClass_EFFECT_CLASS_PURE), tuning{
		now: clock.now,
		leases: func(kv *lease.KV) lease.Manager {
			return &slowFirstOffer{Manager: kv, clock: clock, by: offerGrace}
		},
	})

	clock.reset()
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Zero(t, h.countEvents(ctx, t, runstore.StepDispatched, "a"),
		"a dispatch whose offer was already older than half the grace window committed anyway")
	require.Empty(t, h.drain(ctx, t))

	clock.advance(graceMargin)
	_, err := h.sched.SweepOrphans(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, h.drain(ctx, t), "the abandoned offer was never withdrawn")
	require.Zero(t, h.countEvents(ctx, t, scheduler.StepAttemptLost, "a"))
}

// slowFirstOffer makes the first offer through it take `by` on the scheduler's
// clock: the plane stalled between offering and committing.
type slowFirstOffer struct {
	lease.Manager
	clock *testClock
	by    time.Duration
	once  sync.Once
}

func (s *slowFirstOffer) Offer(
	ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration,
) (lease.Token, error) {
	token, err := s.Manager.Offer(ctx, tenantID, runID, stepID, attempt, ttl)
	s.once.Do(func() { s.clock.advance(s.by) })
	return token, err
}

func mustFence(t *testing.T, d *dholev1.JobDispatch) lease.Token {
	t.Helper()
	_, token, err := scheduler.DecodeFence(d.GetFenceToken())
	require.NoError(t, err)
	return token
}
