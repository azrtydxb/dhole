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
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/tenancy"
)

// The queue, the budgets and the quota enforcer were all built, tested and
// left with no caller: ready steps went straight from plan() to dispatch(),
// which is a fleet with no fair share, no per-pipeline cap and no tenant
// limit. Everything here asserts the WIRING — the property seen through
// Advance and the run's log — because a component with a passing unit test and
// no call site is exactly what these tests exist to stop happening again.

// tenancy.Enforcer is what a deployment passes as Config.Quotas. The
// dependency runs scheduler -> tenancy and never back, so this is the one
// place the two are checked against each other.
var _ scheduler.Quotas = (*tenancy.Enforcer)(nil)

// fanout is two independent root steps. The diamond has ONE root, so it cannot
// show a cap, a share or a capacity limit: with a single ready step every
// possible policy dispatches it.
func fanout() *dholev1.Pipeline {
	step := func(id string) *dholev1.Step {
		return &dholev1.Step{
			Id:          id,
			Name:        id,
			PluginRef:   "cmd://echo",
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
			Outputs:     []*dholev1.Port{{Name: "out"}},
		}
	}
	return &dholev1.Pipeline{
		Id:     testPipeline,
		Tenant: &dholev1.Tenant{Id: testTenant},
		Steps:  []*dholev1.Step{step("x"), step("y")},
	}
}

// engineWithSlots is a ready engine advertising exactly n concurrent slots.
// The number is what the drain asks the queue for, so it is the test's handle
// on the fleet's capacity.
func engineWithSlots(id string, slots int) registry.Instance {
	e := readyEngine(id)
	e.Slots = slots
	return e
}

// stubQuotas is the tenant admission check with its answer in the test's hand,
// and a record of every in-flight count it was asked about — the second is
// what proves the count comes from the fleet's own bookkeeping rather than
// from a zero nobody filled in.
type stubQuotas struct {
	mu     sync.Mutex
	refuse bool
	seen   []int
}

func (q *stubQuotas) AdmitStep(_ context.Context, tenantID string, inFlight int) (tenancy.Decision, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.seen = append(q.seen, inFlight)
	if q.refuse {
		return tenancy.Decision{
			TenantID: tenantID, Quota: tenancy.QuotaMaxConcurrentSteps,
			Limit: 1, Observed: int64(inFlight), Requested: 1, Allowed: false,
			Reason: "tenant \"acme\" is at its max_concurrent_steps quota: limit 1, already used 1, requested 1",
		}, nil
	}
	return tenancy.Decision{
		TenantID: tenantID, Quota: tenancy.QuotaMaxConcurrentSteps,
		Limit: 64, Observed: int64(inFlight), Requested: 1, Allowed: true,
	}, nil
}

func (q *stubQuotas) counts() []int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]int(nil), q.seen...)
}

// wiring is what a wired harness turns on.
type wiring struct {
	pipeline  *dholev1.Pipeline
	instances []registry.Instance
	// budget is the per-pipeline cap. Zero leaves budgets unwired.
	budget int
	quotas scheduler.Quotas
	// leaseTTL is short in the orphan case: a lost attempt is only reachable
	// through a lease that really expires on a real server.
	leaseTTL time.Duration
}

// wired is a harness whose scheduler drains through a real queue, holds a real
// NATS-backed budget, and answers to whatever quota the test supplied.
type wired struct {
	*harness
	budgets *scheduler.Budgets
}

func newWired(ctx context.Context, t *testing.T, w wiring) *wired {
	t.Helper()

	store, err := runstore.NewSQLite(t.TempDir() + "/run.db")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	conn, err := nats.Connect(srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	queue, err := scheduler.NewQueue(scheduler.QueueConfig{})
	require.NoError(t, err)

	var budgets *scheduler.Budgets
	if w.budget > 0 {
		budgets, err = scheduler.NewBudgets(ctx, conn, scheduler.BudgetConfig{
			Limits:  map[scheduler.BudgetKey]int{{TenantID: testTenant, PipelineID: testPipeline}: w.budget},
			TTL:     30 * time.Second,
			PlaneID: "wired",
		})
		require.NoError(t, err)
		t.Cleanup(budgets.Close)
	}

	recorder := &recordingBus{}
	ob := outbox.New(store, recorder, "test-plane")
	fleet := staticFleet{instances: w.instances}
	defs := staticDefs{pipeline: w.pipeline}

	ttl := w.leaseTTL
	if ttl == 0 {
		ttl = scheduler.DefaultLeaseTTL
	}
	cfg := scheduler.Config{
		Store:       store,
		Outbox:      ob,
		Leases:      leases,
		Fleet:       fleet,
		Definitions: defs,
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		LeaseTTL:    ttl,
		Queue:       queue,
		Quotas:      w.quotas,
	}
	if budgets != nil {
		cfg.Budgets = budgets
	}
	sched, err := scheduler.New(cfg)
	require.NoError(t, err)

	h := &harness{
		store: store, bus: recorder, outbox: ob, leases: leases,
		sched: sched, url: srv.URL(), fleet: fleet, defs: defs,
	}
	h.seedRun(ctx, t)
	return &wired{harness: h, budgets: budgets}
}

// unschedulableReasons is every reason the run's log gives for a step that was
// ready and not placed. It is the only place a person can see that a step is
// held back rather than merely slow, which is why every refusal below is
// asserted here and not on a counter.
func (w *wired) unschedulableReasons(ctx context.Context, t *testing.T, stepID string) []string {
	t.Helper()
	events, err := w.store.Replay(ctx, testTenant, testRun)
	require.NoError(t, err)
	var out []string
	for _, e := range events {
		if e.Type != scheduler.StepUnschedulable || e.StepID != stepID {
			continue
		}
		reason, err := scheduler.UnmarshalUnschedulable(e.Payload)
		require.NoError(t, err)
		out = append(out, reason.Reason)
	}
	return out
}

// report delivers a terminal status for a step's latest dispatch, exactly as
// its engine would, with the phase the test names.
func (w *wired) report(ctx context.Context, t *testing.T, stepID string, phase dholev1.Phase) {
	t.Helper()
	var latest *dholev1.JobDispatch
	for _, d := range w.bus.dispatches(t) {
		if d.GetStepId() == stepID {
			latest = d
		}
	}
	require.NotNil(t, latest, "step %q was never dispatched", stepID)
	require.NoError(t, w.sched.OnStatus(ctx, &dholev1.JobStatus{
		RunId:      latest.GetRunId(),
		StepId:     latest.GetStepId(),
		Attempt:    latest.GetAttempt(),
		FenceToken: latest.GetFenceToken(),
		Phase:      phase,
		ExitCode:   1,
		Error:      "boom",
	}))
}

// TestReadyStepsAreDrainedThroughTheQueueAgainstTheFleetsCapacity is the gap
// itself. Dispatched inline, two ready steps both go out however small the
// fleet is, because Match asks whether an engine COULD take a step and never
// how many it is already holding. Drained through the queue against the
// fleet's capacity, a one-slot fleet takes one of them and the other waits its
// turn.
func TestReadyStepsAreDrainedThroughTheQueueAgainstTheFleetsCapacity(t *testing.T) {
	ctx := testContext(t)
	w := newWired(ctx, t, wiring{pipeline: fanout(), instances: []registry.Instance{engineWithSlots("e1", 1)}})

	require.NoError(t, w.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"x"}, w.drain(ctx, t),
		"a fleet with one slot may be handed one step, not every step that is ready")

	// The step that waited is still queued, not lost: the next pass hands it
	// the slot the queue was holding it for.
	require.NoError(t, w.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"x", "y"}, w.drain(ctx, t),
		"the queued step must reach an engine on the next pass, not sit there forever")
}

// TestAPipelineIsCappedAtItsConcurrencyBudget. The fleet has room for both
// steps and the pipeline does not: a budget of one means one in flight, and
// the step that is held back has to say so in the run's log. A step that is
// never dispatched, never fails and never explains itself makes the run look
// slow rather than capped.
func TestAPipelineIsCappedAtItsConcurrencyBudget(t *testing.T) {
	ctx := testContext(t)
	w := newWired(ctx, t, wiring{
		pipeline:  fanout(),
		instances: []registry.Instance{engineWithSlots("e1", 8)},
		budget:    1,
	})

	require.NoError(t, w.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"x"}, w.drain(ctx, t),
		"a pipeline budgeted at one step may not have two in flight")

	reasons := w.unschedulableReasons(ctx, t, "y")
	require.Len(t, reasons, 1, "a step held back by the budget must be visible in the run's log")
	require.Contains(t, reasons[0], "concurrency budget",
		"the reason must name the budget, not leave the operator guessing")
}

// TestABudgetSlotIsReleasedAfterEveryTerminalStatusAndNotOnlyAfterSuccess. A
// slot given back only on success leaks on every failure and on every
// cancellation, and a leaked slot wedges the pipeline permanently — at the
// moment something else has already gone wrong, which is the worst time to be
// debugging a semaphore.
func TestABudgetSlotIsReleasedAfterEveryTerminalStatusAndNotOnlyAfterSuccess(t *testing.T) {
	for _, tc := range []struct {
		name  string
		phase dholev1.Phase
	}{
		{"failed", dholev1.Phase_PHASE_FAILED},
		{"cancelled", dholev1.Phase_PHASE_CANCELLED},
		{"succeeded", dholev1.Phase_PHASE_SUCCEEDED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testContext(t)
			w := newWired(ctx, t, wiring{
				pipeline:  fanout(),
				instances: []registry.Instance{engineWithSlots("e1", 8)},
				budget:    1,
			})

			require.NoError(t, w.sched.Advance(ctx, testTenant, testRun))
			require.Equal(t, []string{"x"}, w.drain(ctx, t))

			w.report(ctx, t, "x", tc.phase)

			require.NoError(t, w.sched.Advance(ctx, testTenant, testRun))
			require.Contains(t, w.drain(ctx, t), "y",
				"the slot held by a %s step was never given back", tc.phase)
		})
	}
}

// TestABudgetSlotIsReleasedWhenAnAttemptIsLostWithItsEngine. The engine dies
// and no status ever arrives, so nothing on the status path can give the slot
// back. The sweeper is the only thing that knows the attempt is over, and a
// slot it does not release is held until the bucket ages it out — minutes of a
// wedged pipeline for a failure the plane already detected.
func TestABudgetSlotIsReleasedWhenAnAttemptIsLostWithItsEngine(t *testing.T) {
	ctx := testContext(t)
	w := newWired(ctx, t, wiring{
		pipeline:  fanout(),
		instances: []registry.Instance{engineWithSlots("e1", 8)},
		budget:    1,
		leaseTTL:  time.Second,
	})

	require.NoError(t, w.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"x"}, w.drain(ctx, t))

	// The engine is gone: nobody renews the lease and nobody reports.
	deadline := time.Now().Add(20 * time.Second)
	for {
		n, err := w.sched.SweepOrphans(ctx)
		require.NoError(t, err)
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the lease of a dead engine never expired")
		}
		time.Sleep(50 * time.Millisecond)
	}

	require.NoError(t, w.sched.Advance(ctx, testTenant, testRun))
	require.Greater(t, len(w.drain(ctx, t)), 1,
		"the slot of an attempt lost with its engine was never given back")
}

// TestAStepOverTheTenantQuotaIsRefusedAndTheRunSaysWhy. A quota that refuses
// silently is a run that stops with nothing to look at. The refusal names the
// quota, the limit and what was already used, and it does so where the person
// whose run stopped will find it.
func TestAStepOverTheTenantQuotaIsRefusedAndTheRunSaysWhy(t *testing.T) {
	ctx := testContext(t)
	quotas := &stubQuotas{refuse: true}
	w := newWired(ctx, t, wiring{
		pipeline:  fanout(),
		instances: []registry.Instance{engineWithSlots("e1", 8)},
		budget:    4,
		quotas:    quotas,
	})

	require.NoError(t, w.sched.Advance(ctx, testTenant, testRun))
	require.Empty(t, w.drain(ctx, t), "a step over the tenant's quota may not reach an engine")

	reasons := w.unschedulableReasons(ctx, t, "x")
	require.Len(t, reasons, 1, "a step refused by a quota must be visible in the run's log")
	require.Contains(t, reasons[0], "max_concurrent_steps quota",
		"the reason must name the quota that refused it")
}

// TestTheQuotaIsAskedAboutTheFleetsInFlightCount. The enforcer takes the
// in-flight number rather than counting one itself, so a caller that passes a
// zero it never filled in turns a fleet-wide limit into no limit at all. The
// number comes from the persisted budgets, which is the only count that is the
// fleet's and not one plane's.
func TestTheQuotaIsAskedAboutTheFleetsInFlightCount(t *testing.T) {
	ctx := testContext(t)
	quotas := &stubQuotas{}
	w := newWired(ctx, t, wiring{
		pipeline:  fanout(),
		instances: []registry.Instance{engineWithSlots("e1", 8)},
		budget:    4,
		quotas:    quotas,
	})

	require.NoError(t, w.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"x", "y"}, w.drain(ctx, t))

	counts := quotas.counts()
	require.Len(t, counts, 2, "both ready steps are admission-checked")
	require.Equal(t, 0, counts[0], "nothing was in flight when the first step was admitted")
	require.Equal(t, 1, counts[1],
		"the second step was admitted against an in-flight count of zero: "+
			"the fleet's own bookkeeping is not being read")
}
