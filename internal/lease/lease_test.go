package lease_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/lease"
)

// testTTL is long enough that nothing expires while a test is only asserting
// on fences, and short enough that the expiry tests stay quick.
const testTTL = 5 * time.Second

// newManager brings up an embedded NATS with JetStream and a lease manager over
// it. Every server and connection is torn down through t.Cleanup so no test
// leaks a bus into the next one.
func newManager(ctx context.Context, t *testing.T) *lease.KV {
	t.Helper()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	return managerOn(ctx, t, srv.URL())
}

// managerOn attaches another manager to a bus that is already running, which is
// how the tests stand in for a second control plane.
func managerOn(ctx context.Context, t *testing.T, url string) *lease.KV {
	t.Helper()

	conn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	mgr, err := lease.New(ctx, conn)
	require.NoError(t, err)
	return mgr
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestFenceIncrementsPerAttempt is the property every at-most-once guarantee in
// the system rests on: a second attempt at the same step is fenced strictly
// above the first, so the first attempt's late report can be told apart from
// the second's.
func TestFenceIncrementsPerAttempt(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	first, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 1, testTTL)
	require.NoError(t, err)

	second, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 2, testTTL)
	require.NoError(t, err)

	require.Greater(t, second.Fence, first.Fence,
		"the fence must strictly increase per attempt")
}

// TestAtMostOnceRejectsDuplicateDeliveryByFence is the resurrection case from
// docs/wire-contract.md: an engine presumed dead comes back holding the old
// fence, and the control plane must refuse its report rather than overwrite a
// newer attempt's result.
func TestAtMostOnceRejectsDuplicateDeliveryByFence(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	stale, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 1, testTTL)
	require.NoError(t, err)
	require.NoError(t, mgr.Validate(ctx, stale))

	fresh, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 2, testTTL)
	require.NoError(t, err)
	require.NoError(t, mgr.Validate(ctx, fresh))

	err = mgr.Validate(ctx, stale)
	require.Error(t, err)
	require.ErrorIs(t, err, lease.ErrFenced)
}

// TestHeartbeatExpiryRedeliversAndCatalogPersists: a lease that stops being
// renewed expires, and the step comes back as an orphan for re-dispatch.
func TestHeartbeatExpiryRedeliversAndCatalogPersists(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	token, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 3, 100*time.Millisecond)
	require.NoError(t, err)

	orphans, err := mgr.Expire(ctx)
	require.NoError(t, err)
	require.Empty(t, orphans, "a lease inside its TTL is not an orphan")

	var found []lease.Orphan
	require.Eventually(t, func() bool {
		got, expErr := mgr.Expire(ctx)
		require.NoError(t, expErr)
		found = append(found, got...)
		return len(found) > 0
	}, 5*time.Second, 20*time.Millisecond)

	require.Len(t, found, 1)
	require.Equal(t, "tenant-a", found[0].TenantID)
	require.Equal(t, "run-1", found[0].RunID)
	require.Equal(t, "step-1", found[0].StepID)
	require.Equal(t, uint32(3), found[0].Attempt)
	require.Equal(t, token.Fence, found[0].Fence)

	// The expired holder's token is dead: its late report must be refused.
	require.ErrorIs(t, mgr.Validate(ctx, token), lease.ErrFenced)
}

// TestRenewKeepsTheFenceStable guards the subtlety that makes renewal usable at
// all: renewing must not move the fence, or the holder's own dispatch token
// would be invalidated by its own heartbeat.
func TestRenewKeepsTheFenceStable(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	token, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 1, 200*time.Millisecond)
	require.NoError(t, err)

	for range 3 {
		time.Sleep(60 * time.Millisecond)
		require.NoError(t, mgr.Renew(ctx, token))
		require.NoError(t, mgr.Validate(ctx, token))
		orphans, expErr := mgr.Expire(ctx)
		require.NoError(t, expErr)
		require.Empty(t, orphans, "a renewed lease never expires")
	}
}

// TestRenewWithStaleTokenFails: a superseded holder must not be able to extend
// the lease that the new holder now owns.
func TestRenewWithStaleTokenFails(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	stale, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 1, 100*time.Millisecond)
	require.NoError(t, err)
	fresh, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 2, 100*time.Millisecond)
	require.NoError(t, err)

	require.ErrorIs(t, mgr.Renew(ctx, stale), lease.ErrFenced)

	// And the refusal was total: the stale renew did not silently extend the
	// new holder's lease, which still expires on its own TTL.
	require.Eventually(t, func() bool {
		orphans, expErr := mgr.Expire(ctx)
		require.NoError(t, expErr)
		for _, o := range orphans {
			if o.Fence == fresh.Fence {
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)
}

// TestConcurrentClaimsGetDistinctFences: two control planes racing on the same
// step must never hand out the same fence twice. Run under -race.
func TestConcurrentClaimsGetDistinctFences(t *testing.T) {
	ctx := testContext(t)

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	const claimers = 8
	managers := make([]*lease.KV, claimers)
	for i := range managers {
		managers[i] = managerOn(ctx, t, srv.URL())
	}

	var (
		mu     sync.Mutex
		fences = map[uint64]int{}
		start  = make(chan struct{})
		wg     sync.WaitGroup
	)
	for i := range claimers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			token, claimErr := managers[i].Claim(ctx, "tenant-a", "run-1", "step-1", uint32(i+1), testTTL)
			mu.Lock()
			defer mu.Unlock()
			if claimErr != nil {
				return // a clean failure is an acceptable outcome; a shared fence is not
			}
			fences[token.Fence]++
		}(i)
	}
	close(start)
	wg.Wait()

	require.NotEmpty(t, fences, "at least one claim must succeed")
	for fence, count := range fences {
		require.Equal(t, 1, count, "fence %d was handed out %d times", fence, count)
	}
}

// TestClaimRejectsUnscopedTenant: there is no unscoped record anywhere in this
// system, and a lease is a record.
func TestClaimRejectsUnscopedTenant(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	_, err := mgr.Claim(ctx, "", "run-1", "step-1", 1, testTTL)
	require.Error(t, err)
	require.ErrorIs(t, err, lease.ErrTenantRequired)
	require.ErrorContains(t, err, "tenant scope required")
}

// TestLeasesAreTenantScoped: the same run and step in two tenants are two
// different leases. Drop the tenant from the key and one tenant's claim fences
// out the other's engine.
func TestLeasesAreTenantScoped(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	a, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 1, testTTL)
	require.NoError(t, err)
	b, err := mgr.Claim(ctx, "tenant-b", "run-1", "step-1", 1, testTTL)
	require.NoError(t, err)

	require.NotEqual(t, a.Value, b.Value)
	require.NoError(t, mgr.Validate(ctx, a), "tenant-b's claim must not fence tenant-a")
	require.NoError(t, mgr.Validate(ctx, b))
}

// TestExpireIsSafeAcrossControlPlanes: two planes sweeping at once must not
// each report the same dead step, or the scheduler re-dispatches it twice.
func TestExpireIsSafeAcrossControlPlanes(t *testing.T) {
	ctx := testContext(t)

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	claimer := managerOn(ctx, t, srv.URL())
	const steps = 5
	for i := range steps {
		_, claimErr := claimer.Claim(ctx, "tenant-a", "run-1", stepName(i), uint32(1), 100*time.Millisecond)
		require.NoError(t, claimErr)
	}
	time.Sleep(150 * time.Millisecond)

	const planes = 4
	sweepers := make([]*lease.KV, planes)
	for i := range sweepers {
		sweepers[i] = managerOn(ctx, t, srv.URL())
	}

	var (
		mu    sync.Mutex
		seen  = map[string]int{}
		start = make(chan struct{})
		wg    sync.WaitGroup
	)
	for i := range planes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			orphans, expErr := sweepers[i].Expire(ctx)
			mu.Lock()
			defer mu.Unlock()
			if expErr != nil {
				return
			}
			for _, o := range orphans {
				seen[o.TenantID+"/"+o.RunID+"/"+o.StepID]++
			}
		}(i)
	}
	close(start)
	wg.Wait()

	require.Len(t, seen, steps, "every dead step must be reported once")
	for step, count := range seen {
		require.Equal(t, 1, count, "%s was reported as an orphan %d times", step, count)
	}
}

func stepName(i int) string {
	return "step-" + string(rune('a'+i))
}

// TestSupersededRenewalDoesNotExtendTheNewLease closes the subtler half of the
// stale-renew hole. A holder that was alive and renewing when it got superseded
// leaves a renewal record behind with a far-off deadline. That record belongs to
// the OLD fence, and if expiry honoured it the new holder could die silently and
// never be re-dispatched — the step would hang forever with nobody working it.
func TestSupersededRenewalDoesNotExtendTheNewLease(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	// The first holder is alive and renewing on a long TTL.
	old, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 1, 30*time.Second)
	require.NoError(t, err)
	require.NoError(t, mgr.Renew(ctx, old))

	// It is superseded by a short-lived attempt that then dies without renewing.
	fresh, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 2, 100*time.Millisecond)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		orphans, expErr := mgr.Expire(ctx)
		require.NoError(t, expErr)
		for _, o := range orphans {
			if o.Fence == fresh.Fence {
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond,
		"the old fence's renewal must not keep the new holder's lease alive")
}
