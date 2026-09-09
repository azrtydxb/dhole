package pool_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/executortest"
	"github.com/azrtydxb/dhole/internal/executor/pool"
	"github.com/azrtydxb/dhole/internal/executor/process"
)

// realSandbox makes a sandbox from the local process backend. The pool tests
// that are about carried state use it rather than a fake: the whole point of a
// warm pool is that the NEXT occupant sees what the last one left on disk, and
// a fake sandbox would "prove" that by returning a string somebody typed into
// the test.
func realSandbox(t *testing.T, e executor.Executor) func() (executor.Sandbox, error) {
	t.Helper()
	return func() (executor.Sandbox, error) {
		return e.Acquire(t.Context(), executor.Spec{Lease: executor.LeasePool})
	}
}

// runIn runs one shell command in a sandbox and returns its trimmed stdout.
func runIn(t *testing.T, sb executor.Sandbox, script string) string {
	t.Helper()
	var out bytes.Buffer
	code, err := sb.Exec(t.Context(), executor.Cmd{
		Args:   []string{"sh", "-c", script},
		Stdout: &out,
	})
	require.NoError(t, err)
	require.Equal(t, int32(0), code, "script failed: %s", script)
	return strings.TrimSpace(out.String())
}

// TestPoolReusesSandboxAcrossRuns is the property the whole task exists for:
// the second run does not pay for a fresh sandbox, and it can see what the
// first one left behind. The identity check and the carried-file check are
// both needed — a pool that hands back a NEW sandbox with the same reported id
// would pass one of them and be useless.
func TestPoolReusesSandboxAcrossRuns(t *testing.T) {
	m := pool.New(pool.Options{})
	t.Cleanup(func() { require.NoError(t, m.Close(t.Context())) })

	mk := realSandbox(t, process.New())
	key := pool.KeyFor("tenant-a", "process", executor.Spec{Lease: executor.LeasePool})

	first, err := m.Acquire(t.Context(), key, mk)
	require.NoError(t, err)
	firstID := runIn(t, first, "pwd")
	runIn(t, first, "echo warm > marker")
	require.NoError(t, first.Release(t.Context()))

	second, err := m.Acquire(t.Context(), key, mk)
	require.NoError(t, err)
	secondID := runIn(t, second, "pwd")
	require.Equal(t, firstID, secondID, "the second run must land in the same sandbox")
	require.Equal(t, "warm", runIn(t, second, "cat marker"),
		"a pooled sandbox carries what the previous occupant left")
	require.NoError(t, second.Release(t.Context()))
}

// fakeSandbox stands in where the property under test is about the pool's
// BOOKKEEPING — how many sandboxes exist, which one was torn down, whether a
// broken one comes back — rather than about anything that happens inside a
// sandbox. It counts real Release calls, so a test cannot mistake "the pool
// forgot about it" for "the pool released it".
type fakeSandbox struct {
	id string

	mu       sync.Mutex
	releases int
	execErr  error
}

func (f *fakeSandbox) Exec(context.Context, executor.Cmd) (int32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.execErr != nil {
		return 0, f.execErr
	}
	return 0, nil
}

func (f *fakeSandbox) Put(context.Context, string, io.Reader) error { return nil }

func (f *fakeSandbox) Get(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (f *fakeSandbox) Signal(context.Context, executor.Signal) error { return nil }

func (f *fakeSandbox) Release(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.releases++
	return nil
}

func (f *fakeSandbox) releaseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.releases
}

// fakeMaker hands out a fresh fakeSandbox per call and remembers them all, so
// a test can say both "how many were created" and "which one was released".
type fakeMaker struct {
	mu   sync.Mutex
	made []*fakeSandbox
	err  error
}

func (m *fakeMaker) mk() (executor.Sandbox, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	sb := &fakeSandbox{id: fmt.Sprintf("sb-%d", len(m.made))}
	m.made = append(m.made, sb)
	return sb, nil
}

func (m *fakeMaker) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.made)
}

func (m *fakeMaker) at(i int) *fakeSandbox {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.made[i]
}

// manualClock makes idle time a value the test sets rather than something it
// waits for: a reaper tested with sleeps is a reaper tested against the
// scheduler's mood.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// underlying returns the fake a lease is wrapping, identified by the id it
// reports, so a test can say "the same sandbox" without pointer-peeking.
func idOf(t *testing.T, sb executor.Sandbox) string {
	t.Helper()
	l, ok := sb.(pool.Lease)
	require.True(t, ok, "Acquire must return a pool.Lease")
	return l.SandboxID()
}

// TestReapReleasesIdleSandboxes: a warm pool that never lets go is a leak. An
// idle sandbox past the threshold must actually be torn down — the fake counts
// Release calls, so "the pool dropped its reference" does not pass for it —
// and the next acquire must build a new one.
func TestReapReleasesIdleSandboxes(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_700_000_000, 0)}
	m := pool.New(pool.Options{Now: clock.Now})
	t.Cleanup(func() { require.NoError(t, m.Close(t.Context())) })
	maker := &fakeMaker{}
	key := pool.KeyFor("tenant-a", "fake", executor.Spec{})

	first, err := m.Acquire(t.Context(), key, maker.mk)
	require.NoError(t, err)
	firstID := idOf(t, first)
	require.NoError(t, first.Release(t.Context()))

	clock.advance(30 * time.Second)
	reaped, err := m.Reap(t.Context(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, 0, reaped, "30s idle is inside a 1m threshold")
	require.Equal(t, 0, maker.at(0).releaseCount())

	clock.advance(2 * time.Minute)
	reaped, err = m.Reap(t.Context(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)
	require.Equal(t, 1, maker.at(0).releaseCount(), "the reaped sandbox must really be released")

	second, err := m.Acquire(t.Context(), key, maker.mk)
	require.NoError(t, err)
	require.NotEqual(t, firstID, idOf(t, second), "after a reap the next acquire builds a new sandbox")
	require.Equal(t, 2, maker.count())
	require.NoError(t, second.Release(t.Context()))
}

// TestReapLeavesInUseSandboxesAlone: reaping a sandbox somebody is running a
// step in kills that step. Only sandboxes sitting idle in the pool are
// candidates, however long ago they were acquired.
func TestReapLeavesInUseSandboxesAlone(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_700_000_000, 0)}
	m := pool.New(pool.Options{Now: clock.Now})
	t.Cleanup(func() { require.NoError(t, m.Close(t.Context())) })
	maker := &fakeMaker{}

	held, err := m.Acquire(t.Context(), pool.KeyFor("t", "fake", executor.Spec{}), maker.mk)
	require.NoError(t, err)

	clock.advance(time.Hour)
	reaped, err := m.Reap(t.Context(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, 0, reaped, "a sandbox in use is never idle, however old")
	require.Equal(t, 0, maker.at(0).releaseCount())

	// It is still usable: the reaper did not tear it out from under its holder.
	code, err := held.Exec(t.Context(), executor.Cmd{Args: []string{"true"}})
	require.NoError(t, err)
	require.Equal(t, int32(0), code)
	require.NoError(t, held.Release(t.Context()))

	clock.advance(time.Hour)
	reaped, err = m.Reap(t.Context(), time.Minute)
	require.NoError(t, err)
	require.Equal(t, 1, reaped, "once returned to the pool it is reapable")
}

// TestPooledSandboxIsReportedNonCacheable holds the pool and Task 16's
// eligibility rule to the same answer. The pool does not restate the rule: it
// asks cache.Eligible with the lease it actually hands out, and this test
// requires the answer — verdict AND reason — to be identical to what
// cache.Eligible says about a pool lease, for steps that would otherwise be
// perfectly cacheable.
func TestPooledSandboxIsReportedNonCacheable(t *testing.T) {
	m := pool.New(pool.Options{})
	t.Cleanup(func() { require.NoError(t, m.Close(t.Context())) })
	maker := &fakeMaker{}

	steps := map[string]*dholev1.Step{
		"pure": {Id: "s1", EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE},
		"idempotent": {
			Id:          "s2",
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
		},
		"undeclared": {Id: "s3"},
	}
	identities := []string{"sha256:c0ffee", ""}

	for name, step := range steps {
		for _, identity := range identities {
			t.Run(name+"/"+identity, func(t *testing.T) {
				lease, err := m.Acquire(t.Context(), pool.KeyFor("t", "fake", executor.Spec{}), maker.mk)
				require.NoError(t, err)
				defer func() { require.NoError(t, lease.Release(t.Context())) }()

				l, ok := lease.(pool.Lease)
				require.True(t, ok)
				require.Equal(t, executor.LeasePool, l.Scope())

				ok2, reason := l.Cacheable(step, identity)
				wantOK, wantReason := cache.Eligible(step, executor.LeasePool, identity)
				require.False(t, wantOK, "the fixture is only meaningful if the rule says no")
				require.Equal(t, wantOK, ok2, "the pool must answer what cache.Eligible answers")
				require.Equal(t, wantReason, reason, "including the reason a run view shows")
				require.False(t, ok2)
			})
		}
	}

	// The pure step with an identity is the one that proves consultation: it
	// IS cacheable under a step lease, so an answer of false can only have
	// come from asking about the pool lease.
	cacheableUnderStepLease, _ := cache.Eligible(steps["pure"], executor.LeaseStep, "sha256:c0ffee")
	require.True(t, cacheableUnderStepLease,
		"fixture check: this step is cacheable except for the pool lease")
}

// TestPoolKeyIsScopedToTheTenant: two tenants sharing a warm sandbox is a data
// leak, not a cache-hit-rate detail. The key must separate them even when
// engine kind and spec are byte-identical.
func TestPoolKeyIsScopedToTheTenant(t *testing.T) {
	spec := executor.Spec{Image: "img:1", Lease: executor.LeasePool}
	a := pool.KeyFor("tenant-a", "process", spec)
	b := pool.KeyFor("tenant-b", "process", spec)
	require.NotEqual(t, a, b, "two tenants must never compute the same pool key")
	require.Equal(t, a, pool.KeyFor("tenant-a", "process", spec), "the key must be stable")
	require.NotEqual(t, a, pool.KeyFor("tenant-a", "kubernetes", spec), "engine kind is part of the key")
	require.NotEqual(t, a, pool.KeyFor("tenant-a", "process", executor.Spec{Image: "img:2"}),
		"the spec is part of the key")
}

// TestTwoTenantsNeverShareASandbox is the same property end to end, in real
// sandboxes with real files: whatever tenant A left on disk must be invisible
// to tenant B.
func TestTwoTenantsNeverShareASandbox(t *testing.T) {
	m := pool.New(pool.Options{})
	t.Cleanup(func() { require.NoError(t, m.Close(t.Context())) })
	mk := realSandbox(t, process.New())
	spec := executor.Spec{Lease: executor.LeasePool}

	a, err := m.Acquire(t.Context(), pool.KeyFor("tenant-a", "process", spec), mk)
	require.NoError(t, err)
	runIn(t, a, "echo tenant-a-secret > secret")
	aRoot := runIn(t, a, "pwd")
	require.NoError(t, a.Release(t.Context()))

	b, err := m.Acquire(t.Context(), pool.KeyFor("tenant-b", "process", spec), mk)
	require.NoError(t, err)
	require.NotEqual(t, aRoot, runIn(t, b, "pwd"), "tenant B must get its own sandbox")
	require.Equal(t, "missing", runIn(t, b, "cat secret 2>/dev/null || echo missing"),
		"tenant B must not see tenant A's files")
	require.NoError(t, b.Release(t.Context()))
}

// TestAnAcquiredSandboxIsNotHandedOutWhileItIsHeld is the exclusivity rule
// stated deterministically: while one caller holds the sandbox for a key, a
// second acquire for that same key must build another one rather than hand
// over the live one. The concurrent test below stresses the same rule, but a
// scheduling-dependent test alone would let this regress on a quiet machine.
func TestAnAcquiredSandboxIsNotHandedOutWhileItIsHeld(t *testing.T) {
	m := pool.New(pool.Options{})
	t.Cleanup(func() { require.NoError(t, m.Close(t.Context())) })
	maker := &fakeMaker{}
	key := pool.KeyFor("tenant-a", "fake", executor.Spec{})

	held, err := m.Acquire(t.Context(), key, maker.mk)
	require.NoError(t, err)
	other, err := m.Acquire(t.Context(), key, maker.mk)
	require.NoError(t, err)
	require.NotEqual(t, idOf(t, held), idOf(t, other),
		"a live sandbox must never be leased to a second caller")
	require.Equal(t, 2, maker.count())

	// Once given back, the same sandbox is reused rather than a third built.
	require.NoError(t, held.Release(t.Context()))
	reused, err := m.Acquire(t.Context(), key, maker.mk)
	require.NoError(t, err)
	require.Equal(t, idOf(t, held), idOf(t, reused))
	require.Equal(t, 2, maker.count())
	require.NoError(t, other.Release(t.Context()))
	require.NoError(t, reused.Release(t.Context()))
}

// TestConcurrentAcquiresNeverShareALiveSandbox: the pool may hand the same
// sandbox to one caller after another, or hand out several distinct ones, but
// never the same live sandbox to two callers at once — that is two steps
// writing over each other's files. Run under -race.
func TestConcurrentAcquiresNeverShareALiveSandbox(t *testing.T) {
	m := pool.New(pool.Options{MaxSize: 8})
	t.Cleanup(func() { require.NoError(t, m.Close(t.Context())) })
	maker := &fakeMaker{}
	key := pool.KeyFor("tenant-a", "fake", executor.Spec{})

	var mu sync.Mutex
	inUse := map[string]bool{}
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 25 {
				sb, err := m.Acquire(t.Context(), key, maker.mk)
				if err != nil {
					errs <- err
					return
				}
				l, ok := sb.(pool.Lease)
				if !ok {
					errs <- fmt.Errorf("acquire did not return a pool.Lease")
					return
				}
				id := l.SandboxID()
				mu.Lock()
				if inUse[id] {
					mu.Unlock()
					errs <- fmt.Errorf("sandbox %s handed to two callers at once", id)
					return
				}
				inUse[id] = true
				mu.Unlock()

				if _, err := sb.Exec(t.Context(), executor.Cmd{Args: []string{"true"}}); err != nil {
					errs <- err
					return
				}
				// Hold it for a moment: callers that never overlap cannot
				// detect a pool that hands the same sandbox to two of them.
				time.Sleep(50 * time.Microsecond)

				mu.Lock()
				inUse[id] = false
				mu.Unlock()
				if err := sb.Release(t.Context()); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.LessOrEqual(t, maker.count(), 8, "the pool must not create more sandboxes than its limit")
}

// TestFailedSandboxIsNotHandedOutAgain: a sandbox whose Exec failed at the
// SANDBOX level (not a non-zero exit — that is a normal result) is broken.
// Returning it to the pool would hand the next step a dead environment, so it
// is torn down on release instead.
func TestFailedSandboxIsNotHandedOutAgain(t *testing.T) {
	m := pool.New(pool.Options{})
	t.Cleanup(func() { require.NoError(t, m.Close(t.Context())) })
	maker := &fakeMaker{}
	key := pool.KeyFor("tenant-a", "fake", executor.Spec{})

	broken, err := m.Acquire(t.Context(), key, maker.mk)
	require.NoError(t, err)
	maker.at(0).execErr = errors.New("sandbox is gone")
	_, err = broken.Exec(t.Context(), executor.Cmd{Args: []string{"true"}})
	require.Error(t, err)
	brokenID := idOf(t, broken)
	require.NoError(t, broken.Release(t.Context()))
	require.Equal(t, 1, maker.at(0).releaseCount(), "a failed sandbox is torn down, not pooled")

	next, err := m.Acquire(t.Context(), key, maker.mk)
	require.NoError(t, err)
	require.NotEqual(t, brokenID, idOf(t, next), "a failed sandbox must never be handed out again")
	require.NoError(t, next.Release(t.Context()))
}

// TestReleasedLeaseIsNotUsableAgain: once a step has given its lease back the
// sandbox may already belong to another step. Using the old handle would run
// somebody else's commands in it, so the handle refuses.
func TestReleasedLeaseIsNotUsableAgain(t *testing.T) {
	m := pool.New(pool.Options{})
	t.Cleanup(func() { require.NoError(t, m.Close(t.Context())) })
	maker := &fakeMaker{}
	key := pool.KeyFor("tenant-a", "fake", executor.Spec{})

	lease, err := m.Acquire(t.Context(), key, maker.mk)
	require.NoError(t, err)
	require.NoError(t, lease.Release(t.Context()))
	require.NoError(t, lease.Release(t.Context()), "release is idempotent")

	_, err = lease.Exec(t.Context(), executor.Cmd{Args: []string{"true"}})
	require.ErrorIs(t, err, pool.ErrLeaseReleased)
	require.ErrorIs(t, lease.Put(t.Context(), "f", strings.NewReader("")), pool.ErrLeaseReleased)
	_, err = lease.Get(t.Context(), "f")
	require.ErrorIs(t, err, pool.ErrLeaseReleased)
	require.ErrorIs(t, lease.Signal(t.Context(), executor.SIGTERM), pool.ErrLeaseReleased)

	// And the sandbox itself is fine — it went back to the pool, not away.
	require.Equal(t, 0, maker.at(0).releaseCount())
	again, err := m.Acquire(t.Context(), key, maker.mk)
	require.NoError(t, err)
	require.Equal(t, 1, maker.count())
	require.NoError(t, again.Release(t.Context()))
}

// TestPoolStopsGrowingAtItsLimit pins the bound and what happens at it: the
// manager holds at most MaxSize live sandboxes across all keys. Needing a new
// one at the limit evicts the least recently used IDLE sandbox; when every
// sandbox is in use there is nothing safe to evict, so Acquire fails with
// ErrPoolFull rather than growing without bound or blocking forever.
func TestPoolStopsGrowingAtItsLimit(t *testing.T) {
	m := pool.New(pool.Options{MaxSize: 2})
	t.Cleanup(func() { require.NoError(t, m.Close(t.Context())) })
	maker := &fakeMaker{}
	keyA := pool.KeyFor("tenant-a", "fake", executor.Spec{})
	keyB := pool.KeyFor("tenant-b", "fake", executor.Spec{})
	keyC := pool.KeyFor("tenant-c", "fake", executor.Spec{})

	a, err := m.Acquire(t.Context(), keyA, maker.mk)
	require.NoError(t, err)
	b, err := m.Acquire(t.Context(), keyB, maker.mk)
	require.NoError(t, err)

	// Both in use, limit reached: a third key cannot be served.
	_, err = m.Acquire(t.Context(), keyC, maker.mk)
	require.ErrorIs(t, err, pool.ErrPoolFull)
	require.Equal(t, 2, maker.count(), "the pool must not exceed its limit")

	// Give one back; it becomes evictable, and the third key gets its slot.
	require.NoError(t, a.Release(t.Context()))
	c, err := m.Acquire(t.Context(), keyC, maker.mk)
	require.NoError(t, err)
	require.Equal(t, 3, maker.count())
	require.Equal(t, 1, maker.at(0).releaseCount(), "the idle sandbox is released to make room")

	require.NoError(t, b.Release(t.Context()))
	require.NoError(t, c.Release(t.Context()))
}

// TestCloseReleasesEverything: shutting the manager down must not leave warm
// sandboxes running on the host.
func TestCloseReleasesEverything(t *testing.T) {
	m := pool.New(pool.Options{})
	maker := &fakeMaker{}
	a, err := m.Acquire(t.Context(), pool.KeyFor("t", "fake", executor.Spec{Image: "a"}), maker.mk)
	require.NoError(t, err)
	require.NoError(t, a.Release(t.Context()))
	b, err := m.Acquire(t.Context(), pool.KeyFor("t", "fake", executor.Spec{Image: "b"}), maker.mk)
	require.NoError(t, err)

	require.NoError(t, m.Close(t.Context()))
	require.Equal(t, 1, maker.at(0).releaseCount(), "an idle sandbox is released on close")
	// The one still out is released when its holder gives it back.
	require.NoError(t, b.Release(t.Context()))
	require.Equal(t, 1, maker.at(1).releaseCount(), "a closed pool keeps nothing warm")
}

// pooledExecutor presents a warm pool as an ordinary executor.Executor, so the
// shared conformance contract can be run against it. Everything the contract
// asks for goes through a POOL LEASE rather than a fresh sandbox, which is the
// point: a sandbox handed out warm has to behave exactly like a cold one, and
// the two cases Task 54 added — a cancelled step leaving nothing running, a
// killed step reporting 137 — are the ones a reused sandbox is most likely to
// get wrong, because whatever the last occupant leaked is still in there.
type pooledExecutor struct {
	inner executor.Executor
	mgr   *pool.Manager
	key   string
}

func (p *pooledExecutor) Kind() string                         { return p.inner.Kind() }
func (p *pooledExecutor) Capabilities() []dholev1.Capability   { return p.inner.Capabilities() }
func (p *pooledExecutor) EnvironmentIdentity() (string, error) { return p.inner.EnvironmentIdentity() }

func (p *pooledExecutor) Acquire(ctx context.Context, spec executor.Spec) (executor.Sandbox, error) {
	return p.mgr.Acquire(ctx, p.key, func() (executor.Sandbox, error) {
		return p.inner.Acquire(ctx, executor.Spec{Env: spec.Env, Lease: executor.LeasePool})
	})
}

// TestPooledExecutorContract holds the pool to the same contract every backend
// passes. A pool that quietly weakened one of the executor guarantees would be
// invisible otherwise: nothing else in this package runs the shared suite.
func TestPooledExecutorContract(t *testing.T) {
	mgr := pool.New(pool.Options{})
	t.Cleanup(func() { require.NoError(t, mgr.Close(context.WithoutCancel(t.Context()))) })
	executortest.Contract(t, &pooledExecutor{
		inner: process.New(),
		mgr:   mgr,
		key:   "tenant-a/contract",
	})
}
