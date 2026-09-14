package server

import (
	"context"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// Exactly-once for a step the PLANE runs, across planes.
//
// A step dispatched to an engine is placed by a compare-and-set offer, a
// re-read of the log and a fence validated inside the commit. A builtin step
// was placed by a claim that superseded whatever held it and an append that
// re-read nothing, guarded against a second run only by a map in one process.
// Two planes that both found an llm, loop or agent step ready both ran it, and
// for a step whose effect is outside Dhole — a model call, an agent calling
// tools — that is the effect twice.

const claimTestTenant = "acme"

// hostedHarness is two or more planes' builtin dispatchers over ONE store and
// ONE bus, each with its own lease manager on its own connection, exactly as
// separate processes would have. Nothing is shared in memory between them.
type hostedHarness struct {
	store runstore.Store
	url   string
}

func newHostedHarness(t *testing.T) *hostedHarness {
	t.Helper()
	dir := t.TempDir()
	store, err := runstore.NewSQLite(filepath.Join(dir, "run.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	srv, err := bus.StartEmbedded(filepath.Join(dir, "nats"))
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	return &hostedHarness{store: store, url: srv.URL()}
}

func (h *hostedHarness) leases(ctx context.Context, t *testing.T) *lease.KV {
	t.Helper()
	conn, err := nats.Connect(h.url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	kv, err := lease.New(ctx, conn)
	require.NoError(t, err)
	return kv
}

// plane is one control plane's builtin dispatcher. Its workers are not
// started: the test takes each job off the queue and runs it, so the order
// two planes act in is the test's and not the scheduler's.
func (h *hostedHarness) plane(leases lease.Manager, ttl time.Duration) *builtins {
	return &builtins{
		log:     slog.New(slog.DiscardHandler),
		store:   h.store,
		leases:  leases,
		ttl:     ttl,
		jobs:    make(chan builtinJob, builtinQueue),
		running: map[string]bool{},
	}
}

func (h *hostedHarness) count(ctx context.Context, t *testing.T, runID string, kind runstore.EventType) int {
	t.Helper()
	events, err := h.store.Replay(ctx, claimTestTenant, runID)
	require.NoError(t, err)
	n := 0
	for _, e := range events {
		if e.StepID == "classify" && e.Type == kind {
			n++
		}
	}
	return n
}

func hostedStep() *dholev1.Step {
	return &dholev1.Step{
		Id:          "classify",
		PluginRef:   BuiltinLLM,
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
	}
}

// take is one Advance finding the step ready at attempt and handing it to the
// plane, and returns the job that plane's worker would run.
func take(ctx context.Context, t *testing.T, b *builtins, runID string, attempt uint32) builtinJob {
	t.Helper()
	taken, err := b.Take(ctx, claimTestTenant, runID, hostedStep(), attempt)
	require.NoError(t, err)
	require.True(t, taken)
	job := <-b.jobs
	require.Equal(t, attempt, job.attempt, "the job does not carry the attempt its pass planned")
	b.release(job.tenantID + "/" + job.runID + "/" + job.step.GetId())
	return job
}

// effect is a step body with an effect outside Dhole: it counts every time it
// runs, and can be held while it runs.
type effect struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newEffect() *effect {
	return &effect{entered: make(chan struct{}), release: make(chan struct{})}
}

// freeEffect is an effect nobody holds.
func freeEffect() *effect {
	e := newEffect()
	close(e.release)
	return e
}

func (e *effect) do(ctx context.Context, _ builtinJob, _ uint32) ([]*dholev1.OutputRef, error) {
	e.calls.Add(1)
	e.once.Do(func() { close(e.entered) })
	select {
	case <-e.release:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// claimHeld holds the first Claim through it, AFTER the claim has been
// written, until released: a plane that has taken the step's lease and not
// yet recorded anything.
type claimHeld struct {
	lease.Manager
	once    sync.Once
	claimed chan struct{}
	release chan struct{}
}

func (c *claimHeld) Claim(
	ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration,
) (lease.Token, error) {
	token, err := c.Manager.Claim(ctx, tenantID, runID, stepID, attempt, ttl)
	first := false
	c.once.Do(func() { first = true })
	if first {
		close(c.claimed)
		select {
		case <-c.release:
		case <-ctx.Done():
			return lease.Token{}, ctx.Err()
		}
	}
	return token, err
}

func TestTwoPlanesRunAStepTheyHostExactlyOnce(t *testing.T) {
	t.Run("the second plane arrives while the first is running it", func(t *testing.T) {
		ctx := testCtx(t)
		h := newHostedHarness(t)
		first := h.plane(h.leases(ctx, t), 30*time.Second)
		second := h.plane(h.leases(ctx, t), 30*time.Second)

		// Both planes' Advances read the log before either recorded anything.
		jobA := take(ctx, t, first, "run-1", 1)
		jobB := take(ctx, t, second, "run-1", 1)

		a, b := newEffect(), freeEffect()
		done := make(chan error, 1)
		go func() {
			_, err := first.attempt(ctx, jobA, a.do)
			done <- err
		}()
		<-a.entered // the first plane recorded the dispatch and is running the step

		_, err := second.attempt(ctx, jobB, b.do)
		require.NoError(t, err, "finding the step somebody else's is not a failure to record")
		close(a.release)
		require.NoError(t, <-done)

		require.Equal(t, int32(0), b.calls.Load(), "two planes ran one step the plane hosts")
		require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepDispatched), "one attempt, one dispatch")
		require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepSucceeded),
			"the plane that ran the step had its result discarded")
		require.Zero(t, h.count(ctx, t, "run-1", runstore.StepFailed))
	})

	t.Run("both planes claim before either records", func(t *testing.T) {
		ctx := testCtx(t)
		h := newHostedHarness(t)
		held := &claimHeld{Manager: h.leases(ctx, t), claimed: make(chan struct{}), release: make(chan struct{})}
		first := h.plane(held, 30*time.Second)
		second := h.plane(h.leases(ctx, t), 30*time.Second)

		jobA := take(ctx, t, first, "run-1", 1)
		jobB := take(ctx, t, second, "run-1", 1)

		a, b := freeEffect(), freeEffect()
		done := make(chan error, 1)
		go func() {
			_, err := first.attempt(ctx, jobA, a.do)
			done <- err
		}()
		<-held.claimed // the first plane holds the lease and has recorded nothing

		_, err := second.attempt(ctx, jobB, b.do)
		require.NoError(t, err)
		close(held.release)
		require.NoError(t, <-done)

		require.Equal(t, int32(1), a.calls.Load()+b.calls.Load(), "two planes ran one step the plane hosts")
		require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepDispatched), "one attempt, one dispatch")
		require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepSucceeded))
		require.Zero(t, h.count(ctx, t, "run-1", runstore.StepFailed))
	})
}

// TestAStaleTakeOfAStepThePlaneHasFinishedRunsNothing is the same race inside
// ONE plane. Its in-flight mark is dropped the moment a job finishes, and an
// Advance that read the log before the step was dispatched can call Take in
// that instant: the job used to count its attempt off the log it read then —
// one past the attempt that just succeeded — and run the finished step again.
func TestAStaleTakeOfAStepThePlaneHasFinishedRunsNothing(t *testing.T) {
	ctx := testCtx(t)
	h := newHostedHarness(t)
	// A short TTL, so the finished step's claim can be swept too: the second
	// half is the stale job arriving after it has gone.
	kv := h.leases(ctx, t)
	plane := h.plane(kv, 200*time.Millisecond)

	stale := take(ctx, t, plane, "run-1", 1)
	ran := freeEffect()
	_, err := plane.attempt(ctx, take(ctx, t, plane, "run-1", 1), ran.do)
	require.NoError(t, err)
	require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepSucceeded))

	again := freeEffect()
	_, err = plane.attempt(ctx, stale, again.do)
	require.NoError(t, err)
	require.Equal(t, int32(0), again.calls.Load(), "a stale take ran a step that had already finished")

	// Its claim expires with nobody renewing it, and the sweep removes it.
	require.Eventually(t, func() bool {
		orphans, expErr := kv.Expire(ctx)
		require.NoError(t, expErr)
		return len(orphans) == 1
	}, 10*time.Second, 20*time.Millisecond)

	_, err = plane.attempt(ctx, stale, again.do)
	require.NoError(t, err)
	require.Equal(t, int32(0), again.calls.Load(),
		"a stale take whose claim nobody held any more ran a step the log already shows dispatched")
	require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepDispatched))
	require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepSucceeded))
}

// TestAPlaneThatDiesHoldingAHostedStepsClaimIsRecovered is what refusing a
// second claim must not cost. The first plane claims the step and dies before
// recording anything; no other plane may replace a claim of that attempt, so
// unless the claim expires the step never runs. A plane that dies AFTER
// recording the dispatch is recovered through STEP_ATTEMPT_LOST
// (TestABuiltinStepInFlightWhenThePlaneDiesIsRecoveredByANewPlane).
func TestAPlaneThatDiesHoldingAHostedStepsClaimIsRecovered(t *testing.T) {
	ctx := testCtx(t)
	h := newHostedHarness(t)
	const ttl = 200 * time.Millisecond

	dying, die := context.WithCancel(ctx)
	held := &claimHeld{Manager: h.leases(ctx, t), claimed: make(chan struct{}), release: make(chan struct{})}
	dead := h.plane(held, ttl)
	kv := h.leases(ctx, t)
	survivor := h.plane(kv, ttl)

	never := newEffect()
	job := take(ctx, t, dead, "run-1", 1)
	gone := make(chan struct{})
	go func() {
		defer close(gone)
		_, _ = dead.attempt(dying, job, never.do)
	}()
	<-held.claimed
	die()
	<-gone

	// While the dead plane's claim stands, the survivor leaves the step alone.
	recovered := freeEffect()
	_, err := survivor.attempt(ctx, take(ctx, t, survivor, "run-1", 1), recovered.do)
	require.NoError(t, err)
	require.Equal(t, int32(0), recovered.calls.Load())

	// Nobody renews it, so it expires and the sweep removes it.
	require.Eventually(t, func() bool {
		orphans, expErr := kv.Expire(ctx)
		require.NoError(t, expErr)
		return len(orphans) == 1
	}, 10*time.Second, 20*time.Millisecond, "a claim whose plane died never expired")

	_, err = survivor.attempt(ctx, take(ctx, t, survivor, "run-1", 1), recovered.do)
	require.NoError(t, err)
	require.Equal(t, int32(0), never.calls.Load())
	require.Equal(t, int32(1), recovered.calls.Load(), "the step behind a dead plane's claim never ran")
	require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepDispatched))
	require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepSucceeded))
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// partitioned is a plane cut off from the bus after it claimed: its renewals
// never arrive.
type partitioned struct{ lease.Manager }

func (partitioned) Renew(context.Context, lease.Token) error { return nil }

// firstCommitHeld holds the first transaction through it until released: a
// plane stalled between reading the log and committing on the strength of it.
type firstCommitHeld struct {
	runstore.Store
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *firstCommitHeld) WithTx(ctx context.Context, fn func(runstore.Tx) error) error {
	first := false
	s.once.Do(func() { first = true })
	if first {
		close(s.entered)
		<-s.release
	}
	return s.Store.WithTx(ctx, fn)
}

// TestAPlaneOvertakenBetweenItsClaimAndItsCommitRunsNothing is the fence
// inside the commit. A plane claims the step, re-reads the log, and stalls —
// partitioned, so its renewals stop — long enough for its claim to be swept
// and another plane to claim the attempt afresh, record it and run it. When
// the stalled plane's commit goes through at last it must be refused, or the
// step is recorded and run twice.
func TestAPlaneOvertakenBetweenItsClaimAndItsCommitRunsNothing(t *testing.T) {
	ctx := testCtx(t)
	h := newHostedHarness(t)
	const ttl = 200 * time.Millisecond

	stalled := &firstCommitHeld{Store: h.store, entered: make(chan struct{}), release: make(chan struct{})}
	slow := h.plane(partitioned{h.leases(ctx, t)}, ttl)
	slow.store = stalled
	kv := h.leases(ctx, t)
	other := h.plane(kv, ttl)

	late := freeEffect()
	job := take(ctx, t, slow, "run-1", 1)
	done := make(chan error, 1)
	go func() {
		_, err := slow.attempt(ctx, job, late.do)
		done <- err
	}()
	<-stalled.entered

	require.Eventually(t, func() bool {
		orphans, expErr := kv.Expire(ctx)
		require.NoError(t, expErr)
		return len(orphans) == 1
	}, 10*time.Second, 20*time.Millisecond, "the partitioned plane's claim never expired")

	ran := freeEffect()
	_, err := other.attempt(ctx, take(ctx, t, other, "run-1", 1), ran.do)
	require.NoError(t, err)
	require.Equal(t, int32(1), ran.calls.Load())

	close(stalled.release)
	require.NoError(t, <-done, "being overtaken is not a failure to record")

	require.Equal(t, int32(0), late.calls.Load(), "a plane whose claim was swept before it committed ran the step anyway")
	require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepDispatched), "one attempt, one dispatch")
	require.Equal(t, 1, h.count(ctx, t, "run-1", runstore.StepSucceeded))
	require.Zero(t, h.count(ctx, t, "run-1", runstore.StepFailed))
}
