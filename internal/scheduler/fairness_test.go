package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// dispatchTarget is the spec's promise: a ready step reaches an engine within
// one second at target load. Every latency assertion here is against this.
const dispatchTarget = time.Second

// drainBatch and drainTick are what make the saturation test mean something.
//
// A consumer that empties the queue instantly would pass under plain FIFO —
// ten thousand items drain before any clock has moved, so ordering never
// decides anything. A real fleet dispatches at a finite rate, so this one
// takes drainBatch slots every drainTick, which is 4000 steps a second. At
// that rate a FIFO queue holding tenant a's ten thousand steps ahead of
// tenant b's ten would take about two and a half seconds to reach b's first
// step, and the one-second target is missed by the clock, not by an assertion
// about order.
const (
	drainBatch = 4
	drainTick  = time.Millisecond
)

func queueContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestWeightedFairQueuingUnderSaturation is the reason this queue exists. One
// tenant enqueuing ten thousand steps must not push another tenant's ten past
// the one-second target, and must not take more than its weighted share of the
// slots while those ten are outstanding.
func TestWeightedFairQueuingUnderSaturation(t *testing.T) {
	ctx := queueContext(t)

	q, err := scheduler.NewQueue(scheduler.QueueConfig{
		Weights:       map[string]int{"a": 1, "b": 1},
		DefaultWeight: 1,
	})
	require.NoError(t, err)

	for i := range 10000 {
		require.NoError(t, q.Enqueue(ctx, queueItem("a", i)))
	}
	for i := range 10 {
		require.NoError(t, q.Enqueue(ctx, queueItem("b", i)))
	}

	var dispatchedA, dispatchedB int
	var worstB time.Duration
	start := time.Now()

	for dispatchedB < 10 {
		require.Less(t, time.Since(start), 20*time.Second, "the queue never dispatched tenant b at all")
		batch, err := q.Next(ctx, drainBatch)
		require.NoError(t, err)
		for _, got := range batch {
			switch got.TenantID {
			case "a":
				dispatchedA++
			case "b":
				dispatchedB++
				if latency := time.Since(got.EnqueuedAt); latency > worstB {
					worstB = latency
				}
			default:
				t.Fatalf("unexpected tenant %q", got.TenantID)
			}
		}
		time.Sleep(drainTick)
	}

	require.Less(t, worstB, dispatchTarget,
		"tenant b waited %s for a step to be dispatched; the target is %s", worstB, dispatchTarget)

	// Equal weights: while b had work outstanding, a may not have taken more
	// than b's share plus one round's worth of slack.
	require.LessOrEqual(t, dispatchedA, dispatchedB+drainBatch,
		"tenant a took %d slots while tenant b's %d steps were outstanding", dispatchedA, dispatchedB)
}

func queueItem(tenant string, i int) scheduler.QueueItem {
	return scheduler.QueueItem{
		TenantID:   tenant,
		RunID:      tenant + "-run",
		StepID:     fmt.Sprintf("step-%d", i),
		PipelineID: tenant + "-pipeline",
	}
}

// TestQueueIsDeterministicUnderEqualWeights pins the interleaving down to the
// exact sequence. Fairness that only holds on average is not fairness: the
// tenant that loses every coin toss is starved just the same, and an "a does
// not exceed its share" assertion alone would pass under a queue that
// dispatched a hundred of a and then a hundred of b.
func TestQueueIsDeterministicUnderEqualWeights(t *testing.T) {
	ctx := queueContext(t)

	q, err := scheduler.NewQueue(scheduler.QueueConfig{DefaultWeight: 1})
	require.NoError(t, err)

	// Enqueued in bulk, tenant by tenant: a FIFO queue would hand back all of
	// a before any of b.
	for i := range 5 {
		require.NoError(t, q.Enqueue(ctx, queueItem("a", i)))
	}
	for i := range 5 {
		require.NoError(t, q.Enqueue(ctx, queueItem("b", i)))
	}

	require.Equal(t, []string{"a", "b", "a", "b", "a", "b"}, tenantsOf(t, q, 6),
		"equal-weight tenants must interleave one for one")
	require.Equal(t, []string{"a", "b", "a", "b"}, tenantsOf(t, q, 4),
		"the round-robin position must survive across calls to Next")
}

// TestQueueRespectsWeights is the difference between round-robin and WEIGHTED
// round-robin. Without it a queue that ignored every weight would still pass
// the equal-weight case.
func TestQueueRespectsWeights(t *testing.T) {
	ctx := queueContext(t)

	q, err := scheduler.NewQueue(scheduler.QueueConfig{
		Weights:       map[string]int{"heavy": 3, "light": 1},
		DefaultWeight: 1,
	})
	require.NoError(t, err)
	for i := range 10 {
		require.NoError(t, q.Enqueue(ctx, queueItem("heavy", i)))
		require.NoError(t, q.Enqueue(ctx, queueItem("light", i)))
	}

	require.Equal(t,
		[]string{"heavy", "heavy", "heavy", "light", "heavy", "heavy", "heavy", "light"},
		tenantsOf(t, q, 8),
		"a tenant weighted three to one must take three slots to the other's one")
}

// TestDeficitResetsWhenATenantGoesIdle is the half of deficit round-robin that
// is easy to leave out and impossible to notice until it bites. A tenant that
// keeps its unspent deficit while it has nothing queued banks credit for every
// quiet round and then spends it in one burst, which is precisely the
// starvation the queue exists to prevent — arriving later and looking like a
// spike instead of a policy failure.
func TestDeficitResetsWhenATenantGoesIdle(t *testing.T) {
	ctx := queueContext(t)

	q, err := scheduler.NewQueue(scheduler.QueueConfig{
		Weights:       map[string]int{"bursty": 5, "steady": 1},
		DefaultWeight: 1,
	})
	require.NoError(t, err)

	// bursty takes one slot of its five and then goes idle: four quanta unspent.
	require.NoError(t, q.Enqueue(ctx, queueItem("bursty", 0)))
	require.Equal(t, []string{"bursty"}, tenantsOf(t, q, 4))

	for i := range 10 {
		require.NoError(t, q.Enqueue(ctx, queueItem("bursty", i)))
		require.NoError(t, q.Enqueue(ctx, queueItem("steady", i)))
	}

	got := tenantsOf(t, q, 6)
	require.Equal(t, []string{"bursty", "bursty", "bursty", "bursty", "bursty", "steady"}, got,
		"bursty must take its weight of five and no more; banked deficit would let it take nine")
}

// TestQueueEnqueueAndNextAreSafeUnderConcurrentUse covers the control plane's
// own concurrency: several goroutines feeding the queue while several drain it.
// Every step must come out exactly once — a step dispatched twice is a step run
// twice, and a step lost here never runs at all.
func TestQueueEnqueueAndNextAreSafeUnderConcurrentUse(t *testing.T) {
	ctx := queueContext(t)

	q, err := scheduler.NewQueue(scheduler.QueueConfig{DefaultWeight: 1})
	require.NoError(t, err)

	const (
		tenants  = 8
		perQueue = 250
	)
	var writers sync.WaitGroup
	for w := range tenants {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := range perQueue {
				require.NoError(t, q.Enqueue(ctx, queueItem(fmt.Sprintf("t-%d", w), i)))
			}
		}()
	}

	var (
		mu      sync.Mutex
		seen    = map[string]int{}
		readers sync.WaitGroup
		total   int
	)
	stop := make(chan struct{})
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				batch, err := q.Next(ctx, 7)
				require.NoError(t, err)
				mu.Lock()
				for _, got := range batch {
					seen[got.TenantID+"/"+got.StepID]++
					total++
				}
				done := total == tenants*perQueue
				mu.Unlock()
				if done {
					return
				}
				if len(batch) == 0 {
					select {
					case <-stop:
						return
					default:
					}
				}
			}
		}()
	}
	writers.Wait()
	readers.Wait()
	close(stop)

	require.Len(t, seen, tenants*perQueue, "every enqueued step must be dispatched exactly once")
	for key, count := range seen {
		require.Equal(t, 1, count, "%s was dispatched %d times", key, count)
	}
}

// TestQueueRefusesAnUnscopedTenant. Every record and every subject in Dhole
// carries a tenant scope; an item with none would be dispatched into a queue
// that belongs to nobody and counted against nobody's share.
func TestQueueRefusesAnUnscopedTenant(t *testing.T) {
	ctx := queueContext(t)

	q, err := scheduler.NewQueue(scheduler.QueueConfig{DefaultWeight: 1})
	require.NoError(t, err)

	err = q.Enqueue(ctx, scheduler.QueueItem{RunID: "run-1", StepID: "step-1", PipelineID: "p"})
	require.ErrorIs(t, err, runstore.ErrTenantRequired)
	require.Contains(t, err.Error(), "tenant scope required")

	batch, err := q.Next(ctx, 4)
	require.NoError(t, err)
	require.Empty(t, batch, "a refused item must not have been queued")
}

// tenantsOf drains slots items and reports which tenant each came from.
func tenantsOf(t *testing.T, q *scheduler.Queue, slots int) []string {
	t.Helper()
	batch, err := q.Next(context.Background(), slots)
	require.NoError(t, err)
	out := make([]string, 0, len(batch))
	for _, got := range batch {
		out = append(out, got.TenantID)
	}
	return out
}

// budgetTTL is the deadline a slot's holder is held to in these tests: long
// enough that a live plane's renewals keep its slot, short enough that the
// reclamation test does not have to wait.
const budgetTTL = 500 * time.Millisecond

// newBudgets starts an embedded NATS and returns a Budgets over it, plus the
// URL so a test can attach a SECOND control plane to the same bucket. The real
// KV is used rather than a fake for the same reason internal/lease's tests use
// it: a fake semaphore would decide the outcome of exactly the tests that
// matter — the cross-plane cap, the restart, and the dead holder.
func newBudgets(ctx context.Context, t *testing.T, cfg scheduler.BudgetConfig) (*scheduler.Budgets, string) {
	t.Helper()
	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	return budgetsOn(ctx, t, srv.URL(), cfg), srv.URL()
}

// budgetsOn attaches another control plane to a bus that is already running.
func budgetsOn(ctx context.Context, t *testing.T, url string, cfg scheduler.BudgetConfig) *scheduler.Budgets {
	t.Helper()
	conn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	b, err := scheduler.NewBudgets(ctx, conn, cfg)
	require.NoError(t, err)
	t.Cleanup(b.Close)
	return b
}

func budgetConfig(limit int, ttl time.Duration, plane string) scheduler.BudgetConfig {
	return scheduler.BudgetConfig{
		Default: limit,
		TTL:     ttl,
		PlaneID: plane,
	}
}

// TestConcurrencyBudgetCapsPipelineInFlight is why the budget exists: one
// pipeline must not saturate the fleet. Two slots means two, under any amount
// of concurrency.
func TestConcurrencyBudgetCapsPipelineInFlight(t *testing.T) {
	ctx := queueContext(t)
	budgets, _ := newBudgets(ctx, t, budgetConfig(2, 10*time.Second, "plane-1"))

	var (
		inFlight atomic.Int64
		peak     atomic.Int64
		done     atomic.Int64
		wg       sync.WaitGroup
	)
	// The retry loop is deadline-bounded on purpose: a budget that hands slots
	// out but never takes them back would otherwise leave this spinning until
	// the whole suite timed out, which reports the bug as an unrelated hang.
	deadline := time.Now().Add(10 * time.Second)
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				release, ok := budgets.Acquire(ctx, testTenant, testPipeline)
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				now := inFlight.Add(1)
				for {
					high := peak.Load()
					if now <= high || peak.CompareAndSwap(high, now) {
						break
					}
				}
				time.Sleep(2 * time.Millisecond)
				inFlight.Add(-1)
				release()
				done.Add(1)
				return
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(12), done.Load(),
		"only %d of 12 steps ever got a slot: a released slot is not going back", done.Load())
	require.LessOrEqual(t, peak.Load(), int64(2),
		"a pipeline budgeted at 2 had %d steps in flight at once", peak.Load())
}

// TestBudgetReleaseOnStepFailureNotOnlyOnSuccess. A slot that is only given
// back when a step succeeds leaks on every failure, and a leaked slot wedges
// the pipeline permanently — a failure mode that surfaces only after something
// else has already gone wrong, which is the worst time to be debugging a
// semaphore. The release is asserted from a SECOND control plane so that
// giving the slot back in local memory while leaving the persisted one held
// cannot pass.
func TestBudgetReleaseOnStepFailureNotOnlyOnSuccess(t *testing.T) {
	ctx := queueContext(t)
	cfg := budgetConfig(1, 10*time.Second, "plane-1")
	planeA, url := newBudgets(ctx, t, cfg)
	planeB := budgetsOn(ctx, t, url, budgetConfig(1, 10*time.Second, "plane-2"))

	release, ok := planeA.Acquire(ctx, testTenant, testPipeline)
	require.True(t, ok)
	_, ok = planeB.Acquire(ctx, testTenant, testPipeline)
	require.False(t, ok, "the single slot is held")

	// The step FAILS. The slot goes back exactly as it would on success.
	stepErr := errors.New("step exited 1")
	require.Error(t, stepErr)
	release()

	release2, ok := planeB.Acquire(ctx, testTenant, testPipeline)
	require.True(t, ok, "a failed step must give its budget slot back, not leak it")
	release2()
}

// TestBudgetSurvivesAControlPlaneRestart. The budget is fleet-wide, so it
// cannot live in one plane's memory: a plane that restarts, or a second plane
// that joins, must see the slots that are already out.
func TestBudgetSurvivesAControlPlaneRestart(t *testing.T) {
	ctx := queueContext(t)
	planeA, url := newBudgets(ctx, t, budgetConfig(2, 10*time.Second, "plane-1"))

	for range 2 {
		_, ok := planeA.Acquire(ctx, testTenant, testPipeline)
		require.True(t, ok)
	}

	restarted := budgetsOn(ctx, t, url, budgetConfig(2, 10*time.Second, "plane-2"))
	_, ok := restarted.Acquire(ctx, testTenant, testPipeline)
	require.False(t, ok, "a fresh control plane must see the slots that are already held")

	_, ok = restarted.Acquire(ctx, testTenant, "another-pipeline")
	require.True(t, ok, "the budget is per pipeline, not fleet-wide")
}

// TestBudgetSlotOfADeadControlPlaneIsReclaimed. A persisted semaphore whose
// holder vanished is exactly how a pipeline wedges forever. The slot carries a
// deadline the holder must keep renewing, in the same shape as an engine
// registration: stop proving you are alive and the slot goes back.
func TestBudgetSlotOfADeadControlPlaneIsReclaimed(t *testing.T) {
	ctx := queueContext(t)
	dying, url := newBudgets(ctx, t, budgetConfig(1, budgetTTL, "plane-doomed"))
	survivor := budgetsOn(ctx, t, url, budgetConfig(1, budgetTTL, "plane-survivor"))

	_, ok := dying.Acquire(ctx, testTenant, testPipeline)
	require.True(t, ok)
	_, ok = survivor.Acquire(ctx, testTenant, testPipeline)
	require.False(t, ok, "the slot is held by a live plane")

	// The plane DIES: it stops renewing and never releases anything. Close is
	// what a crash looks like from the bucket's side.
	dying.Close()

	deadline := time.Now().Add(15 * time.Second)
	for {
		release, got := survivor.Acquire(ctx, testTenant, testPipeline)
		if got {
			release()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the slot of a dead control plane was still held %s after it stopped renewing, "+
				"with a %s deadline", 15*time.Second, budgetTTL)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestAcquireDoesNotBlockWhenTheBudgetIsFull. Acquire reports failure rather
// than waiting, so the scheduler can move to another step instead of parking a
// dispatch loop behind one busy pipeline.
func TestAcquireDoesNotBlockWhenTheBudgetIsFull(t *testing.T) {
	ctx := queueContext(t)
	budgets, _ := newBudgets(ctx, t, budgetConfig(1, 10*time.Second, "plane-1"))

	_, ok := budgets.Acquire(ctx, testTenant, testPipeline)
	require.True(t, ok)

	returned := make(chan bool, 1)
	go func() {
		_, got := budgets.Acquire(ctx, testTenant, testPipeline)
		returned <- got
	}()
	select {
	case got := <-returned:
		require.False(t, got)
	case <-time.After(time.Second):
		t.Fatal("Acquire blocked on a full budget instead of returning ok=false")
	}
}

// TestBudgetRefusesAnUnscopedTenant. A slot with no tenant would be charged to
// nobody and shared by everybody.
func TestBudgetRefusesAnUnscopedTenant(t *testing.T) {
	ctx := queueContext(t)
	budgets, _ := newBudgets(ctx, t, budgetConfig(2, 10*time.Second, "plane-1"))

	release, ok := budgets.Acquire(ctx, "", testPipeline)
	require.False(t, ok, "an unscoped tenant must not get a budget slot")
	require.NotNil(t, release, "release must be safe to defer even when the acquire failed")
	release()

	_, ok = budgets.Acquire(ctx, testTenant, "")
	require.False(t, ok, "a slot must name the pipeline it is charged to")

	// Neither refusal may have consumed anything.
	for range 2 {
		_, ok := budgets.Acquire(ctx, testTenant, testPipeline)
		require.True(t, ok)
	}
}

// TestBudgetIsTenantScoped. Two tenants naming the same pipeline hold two
// independent budgets, exactly as they hold two independent leases.
func TestBudgetIsTenantScoped(t *testing.T) {
	ctx := queueContext(t)
	budgets, _ := newBudgets(ctx, t, budgetConfig(1, 10*time.Second, "plane-1"))

	_, ok := budgets.Acquire(ctx, "tenant-a", testPipeline)
	require.True(t, ok)
	_, ok = budgets.Acquire(ctx, "tenant-a", testPipeline)
	require.False(t, ok)
	_, ok = budgets.Acquire(ctx, "tenant-b", testPipeline)
	require.True(t, ok, "one tenant's budget must not be spent by another's steps")
}

// TestBudgetCapsAcrossConcurrentControlPlanes. The cap is fleet-wide: two
// planes racing on the same bucket must not come away with the same slot.
func TestBudgetCapsAcrossConcurrentControlPlanes(t *testing.T) {
	ctx := queueContext(t)
	planeA, url := newBudgets(ctx, t, budgetConfig(2, 10*time.Second, "plane-1"))
	planeB := budgetsOn(ctx, t, url, budgetConfig(2, 10*time.Second, "plane-2"))

	var (
		granted atomic.Int64
		wg      sync.WaitGroup
	)
	for _, plane := range []*scheduler.Budgets{planeA, planeB} {
		for range 8 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, ok := plane.Acquire(ctx, testTenant, testPipeline); ok {
					granted.Add(1)
				}
			}()
		}
	}
	wg.Wait()

	require.Equal(t, int64(2), granted.Load(),
		"two control planes handed out %d slots against a budget of 2", granted.Load())
}

// TestALiveHolderKeepsItsSlotPastTheTTL is the other half of the deadline. A
// slot that ages out under a step that is still running does not free
// capacity, it OVERSUBSCRIBES the pipeline — quietly, and only for steps that
// run longer than the deadline, which are the expensive ones. The holder has
// to keep proving it is alive, exactly as an engine registration does.
func TestALiveHolderKeepsItsSlotPastTheTTL(t *testing.T) {
	ctx := queueContext(t)
	holder, url := newBudgets(ctx, t, budgetConfig(1, budgetTTL, "plane-1"))
	other := budgetsOn(ctx, t, url, budgetConfig(1, budgetTTL, "plane-2"))

	_, ok := holder.Acquire(ctx, testTenant, testPipeline)
	require.True(t, ok)

	// A step outliving its slot's deadline several times over.
	deadline := time.Now().Add(4 * budgetTTL)
	for time.Now().Before(deadline) {
		_, taken := other.Acquire(ctx, testTenant, testPipeline)
		require.False(t, taken,
			"a second step got the slot %s into a still-running step's %s-deadline hold",
			time.Until(deadline), budgetTTL)
		time.Sleep(20 * time.Millisecond)
	}
}
