package scheduler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// A fair queue decides WHOSE step goes next; it says nothing about how much of
// the fleet one pipeline may hold at once. Without that second limit a single
// pipeline with a thousand ready steps takes every engine slot the fair share
// entitles its tenant to and starves the rest of that tenant's own work — the
// fleet is saturated by one pipeline and the queue is behaving perfectly.
//
// The budget is therefore a counting semaphore, and it is PERSISTED. A limit
// kept in one control plane's memory is not a limit on a fleet: run three
// planes and a budget of two becomes six. NATS KV is where the planes already
// agree about leases and engines, and it is where they agree about this.
//
// One slot is one key. A slot is taken with Create, which the server refuses if
// the key is already there, so two planes racing for the last slot cannot both
// win — the same reason internal/lease takes the server's revision rather than
// counting locally.
//
// A holder that dies is the failure this design has to survive: a persisted
// semaphore whose holder vanished wedges its pipeline forever. The answer is
// the one internal/registry already uses for engines rather than a second
// mechanism — the bucket carries a max age, every live holder rewrites its key
// well inside that age, and a holder that stops proving it is alive has its
// slot dropped by the SERVER. Nothing here sweeps anything, so there is no
// sweeper to die alongside the plane it was supposed to clean up after.

// BudgetBucket is the KV bucket every in-flight slot lives in.
const BudgetBucket = "dhole-budgets"

// budgetPrefix keeps slot keys distinguishable from anything else that ever
// shares the bucket.
const budgetPrefix = "budget."

// DefaultBudgetTTL is how long a slot survives without its holder renewing it.
// It is the same order as the lease TTL: long enough that a busy or briefly
// partitioned plane does not lose slots it is still using, short enough that a
// dead plane's pipeline is not wedged for minutes.
const DefaultBudgetTTL = 30 * time.Second

// DefaultBudget is how many steps of one pipeline may be in flight when the
// configuration says nothing. It is a cap, not a target: it exists so that a
// deployment which has thought about none of this still cannot let one
// pipeline take the whole fleet.
const DefaultBudget = 16

// BudgetKey names one pipeline's budget. The tenant is part of it because
// every limit in Dhole is tenant-scoped: two tenants running a pipeline of the
// same name hold two independent budgets.
type BudgetKey struct {
	TenantID   string
	PipelineID string
}

// BudgetConfig configures the semaphore.
type BudgetConfig struct {
	// Limits is the per-pipeline cap.
	Limits map[BudgetKey]int
	// Default applies to any pipeline not named in Limits. Zero means
	// DefaultBudget.
	Default int
	// TTL is how long a slot lives without a renewal. Every control plane
	// sharing the bucket must agree on it: it is the bucket's max age, and the
	// last plane to bind the bucket sets it.
	TTL time.Duration
	// PlaneID identifies this control plane in the slot records. It is
	// diagnostic — the slot is owned by whoever created the key, not by whoever
	// the record names — and a random one is generated when it is empty.
	PlaneID string
}

// Budgets is the persisted concurrency budget.
type Budgets struct {
	kv     jetstream.KeyValue
	limits map[BudgetKey]int
	fallbk int
	ttl    time.Duration
	plane  string

	// renewCtx is the lifetime of this plane's renewals. Cancelling it stops
	// every holder proving it is alive, which is what makes Close indistinguishable
	// from a crash as far as the bucket is concerned.
	renewCtx context.Context
	stop     context.CancelFunc

	mu      sync.Mutex
	holders map[string]*budgetSlot
	closed  bool
	wg      sync.WaitGroup
}

// budgetSlot is one held key. The revision moves with every renewal, and the
// release deletes AT that revision: a slot this plane lost to expiry and which
// somebody else has since taken is not ours to give back.
type budgetSlot struct {
	key string

	mu       sync.Mutex
	revision uint64
	released bool
}

// slotRecord is what one held slot looks like at rest. It is diagnostic: an
// operator looking at a wedged pipeline can see which plane holds what and
// since when.
type slotRecord struct {
	TenantID   string `json:"tenant_id"`
	PipelineID string `json:"pipeline_id"`
	Slot       int    `json:"slot"`
	PlaneID    string `json:"plane_id"`
	AcquiredAt int64  `json:"acquired_at_unix_nano"`
}

// NewBudgets binds the budget bucket on conn, creating it if it is not there
// yet, with the TTL as the bucket's max age.
func NewBudgets(ctx context.Context, conn *nats.Conn, cfg BudgetConfig) (*Budgets, error) {
	ttl := cfg.TTL
	if ttl == 0 {
		ttl = DefaultBudgetTTL
	}
	if ttl < 0 {
		return nil, fmt.Errorf("scheduler: budget ttl must be positive, got %s", ttl)
	}
	fallback := cfg.Default
	if fallback == 0 {
		fallback = DefaultBudget
	}
	if fallback < 0 {
		return nil, fmt.Errorf("scheduler: default budget must be positive, got %d", fallback)
	}
	limits := make(map[BudgetKey]int, len(cfg.Limits))
	for key, limit := range cfg.Limits {
		if key.TenantID == "" {
			return nil, fmt.Errorf("scheduler: budget limits: %w", runstore.ErrTenantRequired)
		}
		if limit <= 0 {
			return nil, fmt.Errorf("scheduler: budget for %s/%s must be positive, got %d",
				key.TenantID, key.PipelineID, limit)
		}
		limits[key] = limit
	}

	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("scheduler: budgets: jetstream: %w", err)
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      BudgetBucket,
		Description: "In-flight concurrency slots. A key that stops being renewed is dropped by the server.",
		History:     1,
		TTL:         ttl,
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("scheduler: budgets: bucket %q: %w", BudgetBucket, err)
	}

	plane := cfg.PlaneID
	if plane == "" {
		plane = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	renewCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	return &Budgets{
		kv:       kv,
		limits:   limits,
		fallbk:   fallback,
		ttl:      ttl,
		plane:    plane,
		renewCtx: renewCtx,
		stop:     stop,
		holders:  make(map[string]*budgetSlot),
	}, nil
}

// Acquire takes one in-flight slot for a pipeline.
//
// It never waits. A full budget returns ok=false immediately so the caller can
// move to another step: a dispatch loop parked on a busy pipeline would turn
// one pipeline's saturation into the whole plane's, which is the failure the
// budget exists to prevent.
//
// The returned release is always non-nil and always safe to defer, including
// on the failed path. Calling it gives the slot back whatever the step did:
// there is no success-only path, because a slot leaked on failure wedges the
// pipeline permanently and does it at the moment something else has already
// gone wrong.
func (b *Budgets) Acquire(ctx context.Context, tenantID, pipelineID string) (func(), bool) {
	if tenantID == "" || pipelineID == "" || ctx.Err() != nil {
		return func() {}, false
	}
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		return func() {}, false
	}

	limit := b.limitFor(tenantID, pipelineID)
	record := slotRecord{
		TenantID:   tenantID,
		PipelineID: pipelineID,
		PlaneID:    b.plane,
		AcquiredAt: time.Now().UnixNano(),
	}
	for index := range limit {
		record.Slot = index
		data, err := json.Marshal(record)
		if err != nil {
			return func() {}, false
		}
		key := budgetKey(tenantID, pipelineID, index)
		// Create is refused by the server if the key is there: the slot is
		// taken atomically or not at all, however many planes are racing.
		revision, err := b.kv.Create(ctx, key, data)
		if err != nil {
			continue // taken, or lost the race for it
		}
		return b.hold(key, revision), true
	}
	return func() {}, false
}

// hold registers a taken slot, starts renewing it and returns its release.
func (b *Budgets) hold(key string, revision uint64) func() {
	slot := &budgetSlot{key: key, revision: revision}

	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return func() {}
	}
	b.holders[key] = slot
	b.wg.Add(1)
	b.mu.Unlock()

	go b.renew(slot)

	var once sync.Once
	return func() {
		once.Do(func() { b.release(slot) })
	}
}

// renew rewrites the slot's key well inside the bucket's max age, which is how
// a live holder keeps a slot the server would otherwise drop. It stops when the
// slot is released, when the plane closes — and when a renewal is refused,
// which means the slot expired and belongs to somebody else now.
func (b *Budgets) renew(slot *budgetSlot) {
	defer b.wg.Done()

	interval := b.ttl / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-b.renewCtx.Done():
			return
		case <-ticker.C:
			if !b.renewOnce(slot) {
				return
			}
		}
	}
}

func (b *Budgets) renewOnce(slot *budgetSlot) bool {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.released {
		return false
	}
	entry, err := b.kv.Get(b.renewCtx, slot.key)
	if err != nil || entry.Revision() != slot.revision {
		return false // expired, or taken over: not ours to keep alive
	}
	revision, err := b.kv.Update(b.renewCtx, slot.key, entry.Value(), slot.revision)
	if err != nil {
		return false
	}
	slot.revision = revision
	return true
}

// release gives the slot back. The delete is conditional on the revision this
// plane last wrote: a slot that expired and was taken over by another plane is
// no longer ours, and deleting it blindly would hand a second holder's slot
// away underneath them.
func (b *Budgets) release(slot *budgetSlot) {
	slot.mu.Lock()
	if slot.released {
		slot.mu.Unlock()
		return
	}
	slot.released = true
	revision := slot.revision
	slot.mu.Unlock()

	b.mu.Lock()
	delete(b.holders, slot.key)
	b.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.WithoutCancel(b.renewCtx), 5*time.Second)
	defer cancel()
	if err := b.kv.Delete(ctx, slot.key, jetstream.LastRevision(revision)); err != nil {
		// Losing this is not a leak that lasts: the key stops being renewed the
		// moment the slot is released, so the bucket's max age reclaims it.
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return
		}
	}
}

// InFlight is how many slots this tenant holds across the WHOLE fleet, over
// every pipeline it is running.
//
// It is counted from the bucket rather than from this plane's own holders
// because it answers a fleet-wide question: the tenant's concurrency quota.
// Counted locally, a limit of sixty-four would become sixty-four per plane,
// which is the same mistake the budget itself exists to avoid — and it would
// fail open precisely when the most is in flight.
//
// The count is of keys, so it includes slots held by other planes and excludes
// ones the server has already aged out. It is a snapshot and it is racy by
// construction: two planes admitting at once can both read the same figure and
// both be admitted. That is a cap that slips by one, not one that fails open,
// and the alternative is a fleet-wide lock in the dispatch path.
func (b *Budgets) InFlight(ctx context.Context, tenantID string) (int, error) {
	if tenantID == "" {
		return 0, fmt.Errorf("scheduler: counting in-flight slots: %w", runstore.ErrTenantRequired)
	}
	// The tenant is the first segment of every key, so the filter is exact:
	// each segment is base64url-encoded, and no encoded tenant can be the
	// prefix of another followed by a dot.
	lister, err := b.kv.ListKeysFiltered(ctx, budgetPrefix+encodeSegment(tenantID)+".>")
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("scheduler: counting in-flight slots for %q: %w", tenantID, err)
	}
	defer func() { _ = lister.Stop() }()

	count := 0
	for range lister.Keys() {
		count++
	}
	return count, nil
}

// Close stops this plane's renewals. It deliberately does NOT release the slots
// still held: an in-flight step whose plane is shutting down is still in
// flight, and its slot goes back the way a crashed plane's does — by ageing
// out — rather than being handed to a second step of the same pipeline while
// the first is still running.
func (b *Budgets) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	b.mu.Unlock()

	b.stop()
	b.wg.Wait()
}

func (b *Budgets) limitFor(tenantID, pipelineID string) int {
	if limit, ok := b.limits[BudgetKey{TenantID: tenantID, PipelineID: pipelineID}]; ok {
		return limit
	}
	return b.fallbk
}

// budgetKey is where tenant scoping is enforced. Every segment is encoded so an
// identifier containing a dot cannot pose as another pipeline's slot, and the
// tenant is always the first segment.
func budgetKey(tenantID, pipelineID string, index int) string {
	return budgetPrefix + encodeSegment(tenantID) + "." + encodeSegment(pipelineID) + "." + strconv.Itoa(index)
}

// encodeSegment keeps identifiers inside the character set NATS KV keys allow
// without losing the boundary between segments.
func encodeSegment(segment string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(segment))
}
