// Package pool keeps sandboxes warm between runs.
//
// A pool lease is the "dirty for speed" scope of ADR 0006: the sandbox handed
// to a step is one an earlier step already ran in, so the toolchain is warm,
// the image is already unpacked and the dependency cache is already there. The
// price is that the sandbox carries state nobody hashed, which is why a step
// run from here is not cacheable — this package asks cache.Eligible with the
// lease it actually hands out rather than restating that rule (see Lease.Cacheable).
//
// Two invariants matter more than the speed:
//
//   - A pool key is scoped to a tenant. Two tenants sharing a warm sandbox is
//     one tenant reading the other's build tree, which is a data leak, not a
//     cache-hit-rate tradeoff.
//   - A sandbox is leased to exactly one caller at a time. Handing the same
//     live sandbox to two steps has them writing over each other's files.
package pool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/executor"
)

// DefaultMaxSize is how many warm sandboxes one manager holds when Options
// does not say. Each one is a container or a process tree with its own disk
// and memory on the host, so the pool is bounded by default rather than
// growing to whatever concurrency happens to arrive.
const DefaultMaxSize = 16

// ErrPoolFull is returned by Acquire when the manager is at its size limit and
// every sandbox it holds is in use, so there is nothing idle to evict to make
// room. It is a refusal, not a wait: the caller can run the step under a step
// lease instead, which is both cacheable and always available.
var ErrPoolFull = errors.New("pool: at capacity and every sandbox is in use")

// ErrLeaseReleased is returned when a released lease is used again. The
// sandbox behind it may already be running another tenant's step.
var ErrLeaseReleased = errors.New("pool: lease has been released")

// Lease is what Acquire hands back: an executor.Sandbox whose Release returns
// the sandbox to the pool instead of tearing it down, plus the little the
// caller needs to reason about what it is holding.
type Lease interface {
	executor.Sandbox
	// SandboxID identifies the underlying warm sandbox. Two leases reporting
	// the same id are the same sandbox reused; they are never live at once.
	SandboxID() string
	// Scope is always executor.LeasePool. It exists so the caller asks the
	// lease what it is rather than assuming.
	Scope() executor.LeaseScope
	// Cacheable answers whether a step run in this sandbox may be cached, and
	// why not when it may not, by asking cache.Eligible about this lease's
	// scope. The answer for a pool lease is always no; the reason comes from
	// cache, so the two cannot drift apart.
	Cacheable(step *dholev1.Step, envIdentity string) (bool, string)
}

// Options configure a Manager. The zero value is usable.
type Options struct {
	// MaxSize is the greatest number of sandboxes the manager holds at once,
	// across every key. Zero means DefaultMaxSize.
	MaxSize int
	// Now is the clock idle time is measured against. Nil means time.Now; a
	// test supplies its own so the reaper is not tested against sleeps.
	Now func() time.Time
}

// entry is one warm sandbox and what the manager knows about it.
type entry struct {
	sb  executor.Sandbox
	id  string
	key string
	// inUse is true between the Acquire that handed it out and the Release
	// that gave it back. An in-use sandbox is never reaped and never evicted.
	inUse bool
	// idleSince is when it was last given back. Meaningless while inUse.
	idleSince time.Time
}

// Manager hands out warm sandboxes keyed by KeyFor.
type Manager struct {
	maxSize int
	now     func() time.Time

	mu sync.Mutex
	// entries holds EVERY sandbox the manager owns, per key, in-use ones
	// included, most recently returned last. In-use sandboxes stay in the
	// registry rather than being lifted out of it while a step holds them, so
	// that "never hand out, evict or reap something in use" is a check this
	// code makes and a test can break, not an accident of where the pointer
	// happened to be parked.
	entries map[string][]*entry
	// live counts every sandbox the manager is responsible for — idle, in use,
	// and slots reserved for a sandbox currently being created — which is what
	// maxSize bounds.
	live   int
	closed bool

	// ids names sandboxes for logs and for callers comparing two leases.
	ids atomic.Uint64
}

// New returns a manager holding no sandboxes yet.
func New(opts Options) *Manager {
	maxSize := opts.MaxSize
	if maxSize <= 0 {
		maxSize = DefaultMaxSize
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Manager{
		maxSize: maxSize,
		now:     now,
		entries: map[string][]*entry{},
	}
}

// KeyFor is the pool key: (tenant, engine kind, spec). Everything that decides
// what a sandbox IS goes in, and the tenant goes in first, because a key that
// omitted it would let one tenant's step land in a sandbox another tenant's
// step left files in.
//
// The result is a hex digest rather than a readable string so no caller is
// tempted to parse a tenant id back out of it.
func KeyFor(tenantID, engineKind string, spec executor.Spec) string {
	h := sha256.New()
	write := func(parts ...string) {
		for _, p := range parts {
			// The length prefix stops ("ab", "c") from colliding with
			// ("a", "bc") — which for a tenant id would be a cross-tenant hit.
			// hash.Hash never errors on Write.
			_, _ = fmt.Fprintf(h, "%d:%s\n", len(p), p)
		}
	}
	write("tenant", tenantID, "kind", engineKind)
	write("image", spec.Image, "workdir", spec.WorkDir, "lease", string(spec.Lease))
	write("os", spec.Requirements.OS, "arch", spec.Requirements.Arch)
	caps := make([]string, 0, len(spec.Requirements.Capabilities))
	for _, c := range spec.Requirements.Capabilities {
		caps = append(caps, c.String())
	}
	sort.Strings(caps)
	write("caps", strings.Join(caps, ","))
	envKeys := make([]string, 0, len(spec.Env))
	for k := range spec.Env {
		envKeys = append(envKeys, k)
	}
	sort.Strings(envKeys)
	for _, k := range envKeys {
		write("env", k, spec.Env[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Acquire returns a warm sandbox for key, building one with mk when the pool
// has none. The returned sandbox is leased exclusively until its Release.
//
// At the size limit it first releases the least recently used idle sandbox to
// make room; when nothing is idle it returns ErrPoolFull rather than growing
// past the limit or blocking a step behind another step's sandbox.
func (m *Manager) Acquire(ctx context.Context, key string, mk func() (executor.Sandbox, error)) (executor.Sandbox, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, errors.New("pool: manager is closed")
	}
	if e := m.claimIdleLocked(key); e != nil {
		m.mu.Unlock()
		return &lease{m: m, e: e}, nil
	}
	if m.live >= m.maxSize {
		if !m.evictOneIdleLocked(ctx) {
			m.mu.Unlock()
			return nil, fmt.Errorf("%w (limit %d)", ErrPoolFull, m.maxSize)
		}
	}
	// Reserve the slot before releasing the lock: mk can be slow, and two
	// concurrent acquires must not both decide there is room for one sandbox.
	m.live++
	m.mu.Unlock()

	sb, err := mk()
	if err != nil {
		m.mu.Lock()
		m.live--
		m.mu.Unlock()
		return nil, fmt.Errorf("pool: create sandbox: %w", err)
	}

	e := &entry{sb: sb, id: fmt.Sprintf("pool-%d", m.ids.Add(1)), key: key, inUse: true}
	m.mu.Lock()
	if m.closed {
		// Close happened while mk ran; do not leave the sandbox behind.
		m.live--
		m.mu.Unlock()
		return nil, errors.Join(errors.New("pool: manager is closed"), sb.Release(ctx))
	}
	m.entries[key] = append(m.entries[key], e)
	m.mu.Unlock()
	return &lease{m: m, e: e}, nil
}

// Reap releases every sandbox that has sat idle in the pool for at least idle,
// and returns how many it released. A sandbox currently leased out is never
// idle, however long ago it was acquired: reaping one would kill the step
// running in it.
func (m *Manager) Reap(ctx context.Context, idle time.Duration) (int, error) {
	m.mu.Lock()
	cutoff := m.now().Add(-idle)
	var doomed []*entry
	for key, entries := range m.entries {
		kept := entries[:0]
		for _, e := range entries {
			// A sandbox somebody is running a step in is not idle, however
			// long ago it was acquired. Reaping it kills that step.
			if !e.inUse && !e.idleSince.After(cutoff) {
				doomed = append(doomed, e)
				continue
			}
			kept = append(kept, e)
		}
		if len(kept) == 0 {
			delete(m.entries, key)
			continue
		}
		m.entries[key] = kept
	}
	m.live -= len(doomed)
	m.mu.Unlock()

	var errs []error
	for _, e := range doomed {
		if err := e.sb.Release(ctx); err != nil {
			errs = append(errs, fmt.Errorf("pool: release idle sandbox %s: %w", e.id, err))
		}
	}
	// The count is what was reaped, not what was reaped cleanly: a sandbox
	// whose teardown errored is gone from the pool either way.
	return len(doomed), errors.Join(errs...)
}

// Close releases every idle sandbox and stops the manager keeping anything
// warm. Leases still out stay usable; their sandboxes are torn down when they
// are given back rather than returned to a closed pool.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	var doomed []*entry
	for key, entries := range m.entries {
		kept := entries[:0]
		for _, e := range entries {
			if e.inUse {
				// Its holder is still running a step; it is torn down when
				// that lease is given back, not out from under it.
				kept = append(kept, e)
				continue
			}
			doomed = append(doomed, e)
		}
		if len(kept) == 0 {
			delete(m.entries, key)
			continue
		}
		m.entries[key] = kept
	}
	m.live -= len(doomed)
	m.mu.Unlock()

	var errs []error
	for _, e := range doomed {
		if err := e.sb.Release(ctx); err != nil {
			errs = append(errs, fmt.Errorf("pool: release sandbox %s: %w", e.id, err))
		}
	}
	return errors.Join(errs...)
}

// claimIdleLocked marks and returns the most recently returned IDLE sandbox
// for key, or nil when every one of that key's sandboxes is already leased
// out. Most recent first: it is the one most likely to still have warm caches.
func (m *Manager) claimIdleLocked(key string) *entry {
	entries := m.entries[key]
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].inUse {
			continue
		}
		entries[i].inUse = true
		return entries[i]
	}
	return nil
}

// evictOneIdleLocked releases the least recently used idle sandbox across all
// keys to free a slot, and reports whether it found one. Everything in use is
// off limits, which is why a fully busy pool refuses instead.
func (m *Manager) evictOneIdleLocked(ctx context.Context) bool {
	var victim *entry
	var victimKey string
	var victimIdx int
	for key, entries := range m.entries {
		for i, e := range entries {
			if e.inUse {
				continue
			}
			if victim == nil || e.idleSince.Before(victim.idleSince) {
				victim, victimKey, victimIdx = e, key, i
			}
		}
	}
	if victim == nil {
		return false
	}
	entries := m.entries[victimKey]
	entries = append(entries[:victimIdx], entries[victimIdx+1:]...)
	if len(entries) == 0 {
		delete(m.entries, victimKey)
	} else {
		m.entries[victimKey] = entries
	}
	m.live--
	// Released under the lock deliberately: the slot must not be handed out
	// while the old occupant is still tearing down its resources.
	_ = victim.sb.Release(ctx)
	return true
}

// put takes a sandbox back. A sandbox that is broken — or that comes back to a
// closed pool — is torn down instead of being made available to the next step.
func (m *Manager) put(ctx context.Context, e *entry, broken bool) error {
	m.mu.Lock()
	if broken || m.closed {
		m.live--
		m.forgetLocked(e)
		m.mu.Unlock()
		if err := e.sb.Release(ctx); err != nil {
			return fmt.Errorf("pool: release sandbox %s: %w", e.id, err)
		}
		return nil
	}
	e.inUse = false
	e.idleSince = m.now()
	// Move it to the back: the most recently returned sandbox is the warmest,
	// and is what the next acquire for this key takes.
	m.forgetLocked(e)
	m.entries[e.key] = append(m.entries[e.key], e)
	m.mu.Unlock()
	return nil
}

// forgetLocked drops e from the registry. It is a no-op if it is not there.
func (m *Manager) forgetLocked(e *entry) {
	entries := m.entries[e.key]
	for i, candidate := range entries {
		if candidate != e {
			continue
		}
		entries = append(entries[:i], entries[i+1:]...)
		if len(entries) == 0 {
			delete(m.entries, e.key)
			return
		}
		m.entries[e.key] = entries
		return
	}
}

// lease is one exclusive hold on a warm sandbox.
type lease struct {
	m *Manager
	e *entry

	mu sync.Mutex
	// released guards against a handle being used after the sandbox may
	// already belong to another step.
	released bool
	// broken records that the SANDBOX failed, as opposed to a command in it
	// exiting non-zero. A broken sandbox is never pooled again: the next step
	// would inherit a dead environment and blame its own command.
	broken bool
}

func (l *lease) SandboxID() string { return l.e.id }

func (l *lease) Scope() executor.LeaseScope { return executor.LeasePool }

// Cacheable defers to Task 16's rule rather than repeating it. The lease scope
// it passes is the one it hands out, so if that ever changed the answer would
// change with it instead of staying a hardcoded "no" that had stopped meaning
// anything.
func (l *lease) Cacheable(step *dholev1.Step, envIdentity string) (bool, string) {
	return cache.Eligible(step, l.Scope(), envIdentity)
}

func (l *lease) Exec(ctx context.Context, cmd executor.Cmd) (int32, error) {
	if err := l.check(); err != nil {
		return 0, err
	}
	code, err := l.e.sb.Exec(ctx, cmd)
	if err != nil {
		// A non-zero exit is a result, not a failure; only an error means the
		// sandbox could not run the command at all.
		l.markBroken()
	}
	return code, err
}

func (l *lease) Put(ctx context.Context, name string, r io.Reader) error {
	if err := l.check(); err != nil {
		return err
	}
	err := l.e.sb.Put(ctx, name, r)
	if err != nil {
		l.markBroken()
	}
	return err
}

func (l *lease) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := l.check(); err != nil {
		return nil, err
	}
	return l.e.sb.Get(ctx, name)
}

func (l *lease) Signal(ctx context.Context, sig executor.Signal) error {
	if err := l.check(); err != nil {
		return err
	}
	return l.e.sb.Signal(ctx, sig)
}

// Release gives the sandbox back. It is idempotent, so cleanup paths can be
// unconditional, matching executor.Sandbox.
func (l *lease) Release(ctx context.Context) error {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil
	}
	l.released = true
	broken := l.broken
	l.mu.Unlock()
	return l.m.put(ctx, l.e, broken)
}

func (l *lease) check() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return ErrLeaseReleased
	}
	return nil
}

func (l *lease) markBroken() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.broken = true
}
