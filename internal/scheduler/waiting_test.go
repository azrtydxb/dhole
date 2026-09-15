package scheduler_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// A dispatch that engines can run and none has taken says why (ADR 0031).
//
// A step waiting in the queue is not late — that was the kw defect — and the
// sweeper reported one only when NO engine matched it any more. The two other
// ways a dispatch sits there looked identical from the run: every matching
// engine is busy, which is a queue doing its job, or a matching engine has room
// and is not taking it — a consumer that never fetches, a queue bound under the
// wrong name, an engine that cannot reach a plane to confirm the dispatch —
// which is an outage that said nothing.

// waitingPlane is a scheduler over h's store, outbox and leases, reading the
// fleet the test controls and a clock it moves.
func waitingPlane(t *testing.T, h *harness, fleet *mutableFleet, clock *testClock) *scheduler.Scheduler {
	t.Helper()
	sched, err := scheduler.New(scheduler.Config{
		Store:       h.store,
		Outbox:      h.outbox,
		Leases:      h.leases,
		Fleet:       fleet,
		Definitions: h.defs,
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		Now:         clock.now,
	})
	require.NoError(t, err)
	return sched
}

func (h *harness) waitingReasons(ctx context.Context, t *testing.T, stepID string) []scheduler.WaitingReason {
	t.Helper()
	events, err := h.store.Replay(ctx, testTenant, testRun)
	require.NoError(t, err)
	var out []scheduler.WaitingReason
	for _, e := range events {
		if e.Type == scheduler.StepWaiting && e.StepID == stepID {
			w, err := scheduler.UnmarshalWaitingReason(e.Payload)
			require.NoError(t, err)
			out = append(out, w)
		}
	}
	return out
}

func sweep(ctx context.Context, t *testing.T, sched *scheduler.Scheduler) {
	t.Helper()
	_, err := sched.SweepOrphans(ctx)
	require.NoError(t, err)
}

// busyEngine is a ready engine whose last heartbeat listed a job in every slot.
func busyEngine(id string, slots int) registry.Instance {
	e := readyEngine(id)
	e.Slots = slots
	for i := range slots {
		e.InFlight = append(e.InFlight, registry.Job{RunID: "elsewhere", StepID: "busy", Attempt: uint32(i + 1)}) //nolint:gosec // small
	}
	return e
}

func TestADispatchMatchingEnginesHaveNotTakenSaysNothingIsConsumingItsQueue(t *testing.T) {
	ctx := testContext(t)
	clock := realClock()
	h := newTunedHarness(ctx, t, soloPipeline(dholev1.EffectClass_EFFECT_CLASS_PURE), tuning{now: clock.now})
	fleet := &mutableFleet{}
	fleet.set(readyEngine("e1")) // four slots, nothing in any of them
	plane := waitingPlane(t, h, fleet, clock)

	clock.reset()
	require.NoError(t, plane.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))

	// Inside the window: an engine may be about to take it.
	clock.advance(offerGrace - graceMargin)
	sweep(ctx, t, plane)
	require.Empty(t, h.waitingReasons(ctx, t, "a"), "a dispatch still inside the heartbeat window was reported waiting")

	clock.advance(2 * graceMargin)
	sweep(ctx, t, plane)
	reasons := h.waitingReasons(ctx, t, "a")
	require.Len(t, reasons, 1,
		"a dispatch that matching engines with free slots never took said nothing about why it waits")
	require.Equal(t, scheduler.WaitingUnconsumed, reasons[0].Cause)
	require.Contains(t, reasons[0].Reason, "job.dispatch."+testTier+".",
		"the reason names the queue nobody is consuming")
	require.Contains(t, reasons[0].Reason, "4 free slot")

	sweep(ctx, t, plane)
	require.Len(t, h.waitingReasons(ctx, t, "a"), 1, "the same wait was recorded on every sweep")
	require.Zero(t, h.countEvents(ctx, t, scheduler.StepUnschedulable, "a"),
		"a step engines can run is not unschedulable")
	require.Zero(t, h.countEvents(ctx, t, scheduler.StepAttemptLost, "a"), "waiting is not lost")

	// An engine takes it at last: nothing more is said.
	h.report(ctx, t, plane, h.latestDispatch(t, "a"), dholev1.Phase_PHASE_ACCEPTED)
	clock.advance(offerGrace)
	sweep(ctx, t, plane)
	require.Len(t, h.waitingReasons(ctx, t, "a"), 1)
}

func TestADispatchWaitingForBusyEnginesSaysItWaitsForCapacity(t *testing.T) {
	ctx := testContext(t)
	clock := realClock()
	h := newTunedHarness(ctx, t, soloPipeline(dholev1.EffectClass_EFFECT_CLASS_PURE), tuning{now: clock.now})
	fleet := &mutableFleet{}
	fleet.set(busyEngine("e1", 2), busyEngine("e2", 1))
	plane := waitingPlane(t, h, fleet, clock)

	clock.reset()
	require.NoError(t, plane.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))

	clock.advance(offerGrace + graceMargin)
	sweep(ctx, t, plane)
	reasons := h.waitingReasons(ctx, t, "a")
	require.Len(t, reasons, 1, "a dispatch waiting behind busy engines said nothing about why")
	require.Equal(t, scheduler.WaitingForCapacity, reasons[0].Cause,
		"engines with every slot busy are not engines ignoring their queue")
	require.Contains(t, reasons[0].Reason, "3 of 3")

	sweep(ctx, t, plane)
	require.Len(t, h.waitingReasons(ctx, t, "a"), 1, "the same wait was recorded on every sweep")

	// A slot frees and the dispatch is still not taken: that is a different
	// wait, and it is said.
	fleet.set(busyEngine("e1", 2), readyEngine("e2"))
	sweep(ctx, t, plane)
	reasons = h.waitingReasons(ctx, t, "a")
	require.Len(t, reasons, 2, "a wait whose cause changed was not recorded again")
	require.Equal(t, scheduler.WaitingUnconsumed, reasons[1].Cause)
}
