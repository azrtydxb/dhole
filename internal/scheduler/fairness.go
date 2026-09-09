package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// The spec's target is that a ready step reaches an engine within one second
// at load. Measured over the whole fleet that target is met by a system in
// which one tenant enqueues ten thousand steps and everybody else waits: the
// average is fine and every other tenant's experience is not. A queue that
// hands work out in arrival order turns one tenant's burst into everybody's
// outage, which is why what is dispatched next is decided by SHARE rather than
// by arrival.
//
// The mechanism is deficit round-robin. Each tenant has its own FIFO and a
// deficit counter; a turn grants the tenant its weight in credit and spends
// one credit per step, and a tenant with nothing queued leaves the rotation
// with its deficit reset to zero. The reset is the half that is easy to omit:
// without it an idle tenant banks a quantum every round and spends the lot in
// one burst, which is the same starvation arriving later and disguised as a
// spike.
//
// Two things this queue deliberately is NOT. It is not durable — it holds what
// one control plane has decided is ready right now, and the run's event log
// remains the only position (ADR 0003), so a plane that dies loses a dispatch
// ORDER, not any work. And it is not the fleet-wide cap: that is Budgets,
// which is persisted, because a limit that lived in one plane's memory would
// be multiplied by the number of planes.

// DefaultWeight is the share a tenant gets when the configuration names no
// weight for it. Equal weights are the only defensible default: a tenant
// nobody has made a decision about must not be quietly favoured or starved.
const DefaultWeight = 1

// QueueItem is one ready step waiting for a slot. It is a value, not a
// reference into any run state: the queue holds identifiers and hands them
// back, and the log is still the authority on what the step is.
type QueueItem struct {
	TenantID string
	RunID    string
	StepID   string
	// PipelineID is what the concurrency budget is charged against, so it
	// travels with the item rather than being looked up again at dispatch.
	PipelineID string
	// Attempt is the attempt number this dispatch would be.
	Attempt uint32
	// EnqueuedAt is stamped by Enqueue when it is zero. It is what makes the
	// one-second target measurable at the far end: dispatch latency is
	// meaningless without the moment the step became ready.
	EnqueuedAt time.Time
}

// QueueConfig configures the weights the rotation runs on.
type QueueConfig struct {
	// Weights is the per-tenant share, keyed by tenant ID. A weight of three
	// takes three slots to a weight of one's slot.
	Weights map[string]int
	// DefaultWeight applies to any tenant not named in Weights. Zero means
	// DefaultWeight.
	DefaultWeight int
	// Now is the clock, injectable for tests.
	Now func() time.Time
}

// Queue is the weighted fair queue. It is safe for concurrent use: several
// goroutines in one control plane enqueue readiness while several drain it,
// and an item comes out exactly once.
//
// It is per control plane. Two planes each hold their own rotation, and what
// stops them dispatching the same step twice is the lease, not this — a queue
// shared over the bus would put a hot lock in the dispatch path to solve a
// problem the fence already solves.
type Queue struct {
	mu       sync.Mutex
	tenants  map[string]*tenantQueue
	active   []string
	cursor   int
	turnOpen bool
	pending  int

	weights       map[string]int
	defaultWeight int
	now           func() time.Time
}

// tenantQueue is one tenant's FIFO and its standing in the rotation.
type tenantQueue struct {
	items   []QueueItem
	deficit int
	weight  int
}

// NewQueue builds a queue over the given weights.
func NewQueue(cfg QueueConfig) (*Queue, error) {
	weights := make(map[string]int, len(cfg.Weights))
	for tenant, weight := range cfg.Weights {
		if tenant == "" {
			return nil, fmt.Errorf("scheduler: queue weights: %w", runstore.ErrTenantRequired)
		}
		if weight <= 0 {
			return nil, fmt.Errorf("scheduler: queue weight for %q must be positive, got %d", tenant, weight)
		}
		weights[tenant] = weight
	}
	q := &Queue{
		tenants:       make(map[string]*tenantQueue),
		weights:       weights,
		defaultWeight: cfg.DefaultWeight,
		now:           cfg.Now,
	}
	if q.defaultWeight == 0 {
		q.defaultWeight = DefaultWeight
	}
	if q.defaultWeight < 0 {
		return nil, fmt.Errorf("scheduler: default queue weight must be positive, got %d", q.defaultWeight)
	}
	if q.now == nil {
		q.now = time.Now
	}
	return q, nil
}

// Enqueue admits a ready step to its tenant's queue.
func (q *Queue) Enqueue(ctx context.Context, item QueueItem) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if item.TenantID == "" {
		return fmt.Errorf("scheduler: enqueue %s/%s: %w", item.RunID, item.StepID, runstore.ErrTenantRequired)
	}
	if item.RunID == "" || item.StepID == "" {
		return errors.New("scheduler: enqueue: run and step are required")
	}
	if item.EnqueuedAt.IsZero() {
		item.EnqueuedAt = q.now()
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	tq, ok := q.tenants[item.TenantID]
	if !ok {
		tq = &tenantQueue{weight: q.weightFor(item.TenantID)}
		q.tenants[item.TenantID] = tq
	}
	if len(tq.items) == 0 {
		// Joining the rotation at the back rather than at the cursor: a tenant
		// that arrives mid-round waits for the tenants already in the round,
		// so a stream of short-lived tenants cannot repeatedly cut in.
		q.active = append(q.active, item.TenantID)
	}
	tq.items = append(tq.items, item)
	q.pending++
	return nil
}

// Next takes up to slots items in weighted round-robin order. It returns what
// is available immediately, which may be nothing: the dispatch loop asks for
// what the fleet has room for and gets on with whatever it is given.
func (q *Queue) Next(ctx context.Context, slots int) ([]QueueItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if slots <= 0 {
		return nil, fmt.Errorf("scheduler: next: slots must be positive, got %d", slots)
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	out := make([]QueueItem, 0, min(slots, q.pending))
	for len(out) < slots && q.pending > 0 && len(q.active) > 0 {
		if q.cursor >= len(q.active) {
			q.cursor = 0
		}
		tenant := q.active[q.cursor]
		tq := q.tenants[tenant]

		// One quantum per turn. turnOpen keeps a turn interrupted by running
		// out of slots from being credited twice on the next call.
		if !q.turnOpen {
			tq.deficit += tq.weight
			q.turnOpen = true
		}
		for tq.deficit > 0 && len(tq.items) > 0 && len(out) < slots {
			out = append(out, tq.items[0])
			tq.items = tq.items[1:]
			tq.deficit--
			q.pending--
		}

		switch {
		case len(tq.items) == 0:
			// Idle: leave the rotation and forfeit the unspent credit. Banking
			// it is what lets a quiet tenant burst past everybody later.
			tq.deficit = 0
			q.active = append(q.active[:q.cursor], q.active[q.cursor+1:]...)
			q.turnOpen = false
		case tq.deficit == 0:
			q.turnOpen = false
			q.cursor++
		default:
			// Slots ran out mid-turn. The turn stays open and the cursor stays
			// put, so the next call resumes this tenant's turn rather than
			// granting it a fresh quantum.
			return out, nil
		}
	}
	return out, nil
}

// Pending is how many steps are waiting, across every tenant. It exists for
// the dispatch loop's own backpressure decisions and for observability.
func (q *Queue) Pending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending
}

func (q *Queue) weightFor(tenantID string) int {
	if weight, ok := q.weights[tenantID]; ok {
		return weight
	}
	return q.defaultWeight
}
