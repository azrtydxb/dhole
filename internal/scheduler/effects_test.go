package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/effects"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// soloPipeline is one step of a given effect class. One step is enough for
// every retry property: what a failure does next is decided by the step's own
// class, never by its neighbours.
func soloPipeline(class dholev1.EffectClass) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     testPipeline,
		Tenant: &dholev1.Tenant{Id: testTenant},
		Steps: []*dholev1.Step{{
			Id:          "a",
			Name:        "a",
			PluginRef:   "cmd://echo",
			EffectClass: class,
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
		}},
	}
}

// testClock is the only clock these tests use. Backoff is minutes long by
// design, and a test that slept through it would be a test nobody runs; time
// is therefore an input, moved by hand, and every wait is exact rather than
// approximately long enough.
type testClock struct {
	mu sync.Mutex
	at time.Time
}

func newClock() *testClock {
	return &testClock{at: time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)}
}

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = c.at.Add(d)
}

// tuning is what a retry test needs to vary: the clock, the store and the
// lease manager.
type tuning struct {
	now    func() time.Time
	store  func(runstore.Store) runstore.Store
	leases func(*lease.KV) lease.Manager
}

// newTunedHarness builds the same harness as newHarnessWith over the same real
// SQLite store, real embedded NATS and real lease manager, with the seams a
// retry test has to reach into.
func newTunedHarness(
	ctx context.Context, t *testing.T, pipeline *dholev1.Pipeline, tune tuning,
) *harness {
	t.Helper()

	var store runstore.Store
	sqlite, err := runstore.NewSQLite(t.TempDir() + "/run.db")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlite.Close() })
	store = sqlite
	if tune.store != nil {
		store = tune.store(store)
	}

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	conn, err := nats.Connect(srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	kv, err := lease.New(ctx, conn)
	require.NoError(t, err)
	var leases lease.Manager = kv
	if tune.leases != nil {
		leases = tune.leases(kv)
	}

	recorder := &recordingBus{}
	ob := outbox.New(store, recorder, "test-plane")
	fleet := staticFleet{instances: []registry.Instance{readyEngine("e1")}}
	defs := staticDefs{pipeline: pipeline}

	now := tune.now
	if now == nil {
		now = time.Now
	}
	sched, err := scheduler.New(scheduler.Config{
		Store:       store,
		Outbox:      ob,
		Leases:      leases,
		Fleet:       fleet,
		Definitions: defs,
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		Now:         now,
	})
	require.NoError(t, err)

	h := &harness{
		store: store, bus: recorder, outbox: ob, leases: kv,
		sched: sched, url: srv.URL(), fleet: fleet, defs: defs,
	}
	h.seedRun(ctx, t)
	return h
}

// failStep reports the LATEST attempt of a step as failed, under the fence it
// was dispatched with, exactly as its engine would.
func (h *harness) failStep(ctx context.Context, t *testing.T, stepID string) {
	t.Helper()
	var latest *dholev1.JobDispatch
	for _, d := range h.bus.dispatches(t) {
		if d.GetStepId() == stepID {
			latest = d
		}
	}
	require.NotNil(t, latest, "step %q was never dispatched", stepID)
	require.NoError(t, h.sched.OnStatus(ctx, &dholev1.JobStatus{
		RunId:      latest.GetRunId(),
		StepId:     latest.GetStepId(),
		Attempt:    latest.GetAttempt(),
		FenceToken: latest.GetFenceToken(),
		Phase:      dholev1.Phase_PHASE_FAILED,
		ExitCode:   1,
		Error:      "boom",
	}))
}

// TestPureStepStopsAtMaxAttemptsAndFailsTheRun. Retrying is not free and it is
// not unbounded: a step that fails for a reason retrying cannot fix must reach
// a verdict, or the run waits for something that will never happen and no one
// is told.
func TestPureStepStopsAtMaxAttemptsAndFailsTheRun(t *testing.T) {
	ctx := testContext(t)
	clock := newClock()
	h := newTunedHarness(ctx, t, soloPipeline(dholev1.EffectClass_EFFECT_CLASS_PURE),
		tuning{now: clock.now})

	policy := effects.RetryPolicy(soloPipeline(dholev1.EffectClass_EFFECT_CLASS_PURE).GetSteps()[0])
	require.Equal(t, 3, policy.MaxAttempts)

	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
		require.Len(t, h.drain(ctx, t), attempt,
			"attempt %d should have been dispatched by now", attempt)
		h.failStep(ctx, t, "a")
		clock.advance(effects.MaxBackoff + time.Minute)
	}

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))

	require.Equal(t, 3, h.countEvents(ctx, t, runstore.StepDispatched, "a"),
		"three attempts and no more, however much time passes")
	require.Len(t, h.drain(ctx, t), 3, "and three dispatches on the bus")
	require.Equal(t, 1, h.countEvents(ctx, t, scheduler.RunFailed, ""),
		"a run whose step exhausted its retries has failed, and says so once")
	require.Equal(t, 0, h.countEvents(ctx, t, runstore.RunCompleted, ""),
		"a failed run must not be reported as completed")
}

// TestRetryIsDelayedByAGrowingBackoff. A step that failed because the far end
// was overloaded must not be retried at the rate that overloaded it, and the
// second wait must be longer than the first. Time here is an injected clock:
// nothing sleeps, and the boundary is tested to the nanosecond.
func TestRetryIsDelayedByAGrowingBackoff(t *testing.T) {
	ctx := testContext(t)
	clock := newClock()
	pipeline := soloPipeline(dholev1.EffectClass_EFFECT_CLASS_PURE)
	h := newTunedHarness(ctx, t, pipeline, tuning{now: clock.now})
	policy := effects.RetryPolicy(pipeline.GetSteps()[0])

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))
	h.failStep(ctx, t, "a")

	// One nanosecond short of the first backoff: still waiting.
	clock.advance(effects.Backoff(policy, 1) - time.Nanosecond)
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t),
		"the retry is delayed by the backoff, not issued the moment the failure lands")
	require.Equal(t, 0, h.countEvents(ctx, t, runstore.RunCompleted, ""),
		"and a run waiting out a backoff is not finished")
	require.Equal(t, 0, h.countEvents(ctx, t, scheduler.RunFailed, ""))

	clock.advance(time.Nanosecond)
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a", "a"}, h.drain(ctx, t), "the backoff elapsed, so attempt 2 goes out")

	h.failStep(ctx, t, "a")
	// The wait that was enough after the first failure is not enough after the
	// second: this is where a fixed backoff would show up as a third dispatch.
	clock.advance(effects.Backoff(policy, 1))
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a", "a"}, h.drain(ctx, t),
		"the second backoff is longer than the first")

	clock.advance(effects.Backoff(policy, 2) - effects.Backoff(policy, 1))
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a", "a", "a"}, h.drain(ctx, t))
}

// committingStore commits what a transaction was told to roll back.
//
// It exists to prove WHERE the exclusive-lease guard lives. Letting the
// dispatch be written and relying on the transaction's rollback to undo it
// works today only because the store is careful, and an at-most-once
// guarantee must not rest on the diligence of whatever is underneath. Under
// this store the only step that is not dispatched is the one the scheduler
// itself refused to build.
type committingStore struct {
	runstore.Store
}

func (s committingStore) WithTx(ctx context.Context, fn func(runstore.Tx) error) error {
	var inner error
	if err := s.Store.WithTx(ctx, func(tx runstore.Tx) error {
		inner = fn(tx)
		return nil // commit regardless: nothing here undoes a mistake
	}); err != nil {
		return err
	}
	return inner
}

// stealingLeases hands back a token that another control plane has already
// superseded — the ordinary outcome of two planes advancing the same run — by
// claiming the step again the instant the first claim returns. The supersession
// is real: it is the same KV manager and a genuinely higher fence.
type stealingLeases struct {
	lease.Manager
}

func (s stealingLeases) Claim(
	ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration,
) (lease.Token, error) {
	token, err := s.Manager.Claim(ctx, tenantID, runID, stepID, attempt, ttl)
	if err != nil {
		return token, err
	}
	if _, err := s.Manager.Claim(ctx, tenantID, runID, stepID, attempt+1, ttl); err != nil {
		return lease.Token{}, err
	}
	return token, nil
}

// TestExclusiveLeaseStepIsNotDispatchedOnAStaleFence. A step that must run at
// most once may only be dispatched by the plane that demonstrably holds its
// lease. Holding a fence that has already been superseded means another plane
// owns the step, and dispatching anyway is how the same charge goes out twice.
func TestExclusiveLeaseStepIsNotDispatchedOnAStaleFence(t *testing.T) {
	stale := tuning{
		store:  func(s runstore.Store) runstore.Store { return committingStore{Store: s} },
		leases: func(kv *lease.KV) lease.Manager { return stealingLeases{Manager: kv} },
	}

	t.Run("at-most-once", func(t *testing.T) {
		ctx := testContext(t)
		h := newTunedHarness(ctx, t, soloPipeline(dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE), stale)

		require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
		require.Empty(t, h.drain(ctx, t),
			"a step requiring an exclusive lease is not sent under a fence somebody else has taken")
		require.Equal(t, 0, h.countEvents(ctx, t, runstore.StepDispatched, "a"),
			"and nothing is written claiming it was")
	})

	t.Run("pure", func(t *testing.T) {
		ctx := testContext(t)
		h := newTunedHarness(ctx, t, soloPipeline(dholev1.EffectClass_EFFECT_CLASS_PURE), stale)

		require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
		require.Equal(t, []string{"a"}, h.drain(ctx, t),
			"the same conditions dispatch a pure step: the store really does commit, "+
				"so the at-most-once refusal above is the scheduler's own")
	})
}

// TestIdempotentRetryCarriesTheSameKeyOnTheWire is the wire half of the
// idempotency key: a key computed but never sent protects nothing, and a key
// that changes between attempts is a new request to the far end.
func TestIdempotentRetryCarriesTheSameKeyOnTheWire(t *testing.T) {
	ctx := testContext(t)
	clock := newClock()
	h := newTunedHarness(ctx, t, soloPipeline(dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT),
		tuning{now: clock.now})

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))
	h.failStep(ctx, t, "a")
	clock.advance(effects.MaxBackoff + time.Minute)
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a", "a"}, h.drain(ctx, t), "an idempotent step is retried")

	sent := h.bus.dispatches(t)
	first := sent[0].GetEnv()[scheduler.IdempotencyKeyEnv]
	second := sent[1].GetEnv()[scheduler.IdempotencyKeyEnv]
	require.Equal(t, effects.IdempotencyKey(testRun, "a", 1), first)
	require.NotEmpty(t, first)
	require.Equal(t, first, second, "the retry is the same request, so it carries the same key")
	require.Equal(t, uint32(1), sent[0].GetAttempt())
	require.Equal(t, uint32(2), sent[1].GetAttempt())
}
