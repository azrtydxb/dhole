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

// TestAnOfferNobodyHasAcceptedDoesNotExpire is the kw defect at the level it
// lives. A dispatch waits in a work queue for an engine slot for as long as the
// fleet is busy, and a lease whose deadline started at dispatch declared every
// such step lost after one TTL — before any engine could have started it. An
// offer has a fence from the start, and no deadline until a holder accepts it.
func TestAnOfferNobodyHasAcceptedDoesNotExpire(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	before := time.Now()
	token, err := mgr.Offer(ctx, "tenant-a", "run-1", "step-1", 1, 100*time.Millisecond)
	require.NoError(t, err)
	after := time.Now()

	time.Sleep(400 * time.Millisecond)

	orphans, err := mgr.Expire(ctx)
	require.NoError(t, err)
	require.Empty(t, orphans, "a step waiting in the queue was expired as if its holder had died")
	require.NoError(t, mgr.Validate(ctx, token), "the waiting dispatch's fence must still be current")

	waiting, err := mgr.Unaccepted(ctx)
	require.NoError(t, err)
	require.Len(t, waiting, 1)
	offeredAt := waiting[0].OfferedAt
	require.False(t, offeredAt.Before(before) || offeredAt.After(after),
		"an offer must say when it was made, or nobody can tell a dead plane's offer from a fresh one: got %v, want within [%v, %v]",
		offeredAt, before, after)
	waiting[0].OfferedAt = time.Time{}
	require.Equal(t, []lease.Waiting{{
		TenantID: "tenant-a", RunID: "run-1", StepID: "step-1", Attempt: 1, Fence: token.Fence,
	}}, waiting, "the scheduler must be able to see what is still waiting")
}

// TestAnAcceptedOfferExpiresOneTTLAfterItsLastRenewal: the first renewal is the
// acceptance, and from then on the offer is a lease like any other. An engine
// that accepted a step after a long wait and died straight after must be lost
// one heartbeat window later — the wait before acceptance buys it nothing.
func TestAnAcceptedOfferExpiresOneTTLAfterItsLastRenewal(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	const ttl = 200 * time.Millisecond
	token, err := mgr.Offer(ctx, "tenant-a", "run-1", "step-1", 1, ttl)
	require.NoError(t, err)
	time.Sleep(3 * ttl)

	require.NoError(t, mgr.Renew(ctx, token))
	accepted := time.Now()

	waiting, err := mgr.Unaccepted(ctx)
	require.NoError(t, err)
	require.Empty(t, waiting, "an accepted offer is no longer waiting")

	var found []lease.Orphan
	require.Eventually(t, func() bool {
		got, expErr := mgr.Expire(ctx)
		require.NoError(t, expErr)
		found = append(found, got...)
		return len(found) > 0
	}, 5*time.Second, 20*time.Millisecond, "an accepted offer whose holder went silent never expired")
	require.GreaterOrEqual(t, time.Since(accepted), ttl,
		"an accepted offer expired before its heartbeat window had passed")
	require.Equal(t, token.Fence, found[0].Fence)
}

// TestASupersededHoldersRenewalDoesNotAcceptTheOfferThatReplacedIt. Fencing
// has to hold for acceptance as it does for results: an engine that accepted
// attempt 1 and was presumed dead keeps heartbeating the old fence, and if that
// counted as accepting attempt 2 — still waiting in the queue — attempt 2 would
// be expired one TTL later for an engine that never had it.
func TestASupersededHoldersRenewalDoesNotAcceptTheOfferThatReplacedIt(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	const ttl = 100 * time.Millisecond
	old, err := mgr.Offer(ctx, "tenant-a", "run-1", "step-1", 1, ttl)
	require.NoError(t, err)
	require.NoError(t, mgr.Renew(ctx, old))

	fresh, err := mgr.Offer(ctx, "tenant-a", "run-1", "step-1", 2, ttl)
	require.NoError(t, err)
	require.ErrorIs(t, mgr.Renew(ctx, old), lease.ErrFenced)

	time.Sleep(4 * ttl)
	orphans, err := mgr.Expire(ctx)
	require.NoError(t, err)
	require.Empty(t, orphans, "the new offer was expired on the strength of the old holder's renewal")

	waiting, err := mgr.Unaccepted(ctx)
	require.NoError(t, err)
	require.Len(t, waiting, 1)
	require.Equal(t, fresh.Fence, waiting[0].Fence)
}

// TestAnOfferForAnAttemptAlreadyOfferedIsRefusedAndLeavesTheFenceAlone is the
// race that cost every step on kw an attempt. The open-run tick and the status
// consumer both Advance a run, both find a step ready, and both offer the same
// attempt. The winner dispatches under its fence; if the loser's offer then
// replaced that fence, every report from the engine running the committed
// dispatch would be discarded as stale and the attempt would have to be
// declared lost and run again. An offer must never supersede an offer of the
// same attempt — from this plane or another.
func TestAnOfferForAnAttemptAlreadyOfferedIsRefusedAndLeavesTheFenceAlone(t *testing.T) {
	ctx := testContext(t)
	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	mgr := managerOn(ctx, t, srv.URL())
	other := managerOn(ctx, t, srv.URL())

	first, err := mgr.Offer(ctx, "tenant-a", "run-1", "step-1", 2, testTTL)
	require.NoError(t, err)

	for name, m := range map[string]*lease.KV{"the same plane": mgr, "another plane": other} {
		_, err = m.Offer(ctx, "tenant-a", "run-1", "step-1", 2, testTTL)
		require.ErrorIs(t, err, lease.ErrAlreadyOffered,
			"%s offered attempt 2 again and was not refused", name)
		_, err = m.Offer(ctx, "tenant-a", "run-1", "step-1", 1, testTTL)
		require.ErrorIs(t, err, lease.ErrAlreadyOffered,
			"%s offered an EARLIER attempt over attempt 2 and was not refused", name)
	}
	require.NoError(t, mgr.Validate(ctx, first),
		"a refused offer moved the fence the first offer's dispatch carries")

	// A later attempt is a genuine re-dispatch, and still supersedes.
	next, err := other.Offer(ctx, "tenant-a", "run-1", "step-1", 3, testTTL)
	require.NoError(t, err)
	require.Greater(t, next.Fence, first.Fence)
	require.ErrorIs(t, mgr.Validate(ctx, first), lease.ErrFenced)
}

// TestConcurrentOffersOfOneAttemptLeaveExactlyOneFence: the refusal is a
// compare-and-set on the server, not a read followed by a write, so planes
// offering at the same instant cannot both come away with a token.
func TestConcurrentOffersOfOneAttemptLeaveExactlyOneFence(t *testing.T) {
	ctx := testContext(t)
	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	const planes = 8
	managers := make([]*lease.KV, planes)
	for i := range managers {
		managers[i] = managerOn(ctx, t, srv.URL())
	}

	start := make(chan struct{})
	tokens := make(chan lease.Token, planes)
	refusals := make(chan error, planes)
	var wg sync.WaitGroup
	for _, m := range managers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			token, err := m.Offer(ctx, "tenant-a", "run-1", "step-1", 1, testTTL)
			if err != nil {
				refusals <- err
				return
			}
			tokens <- token
		}()
	}
	close(start)
	wg.Wait()
	close(tokens)
	close(refusals)

	for err := range refusals {
		require.ErrorIs(t, err, lease.ErrAlreadyOffered, "losing the race is a refusal, not a failure")
	}
	var won []lease.Token
	for token := range tokens {
		won = append(won, token)
	}
	require.Len(t, won, 1, "planes offering one attempt at once must leave exactly one of them holding it")
	require.NoError(t, managers[0].Validate(ctx, won[0]))
}

// TestWithdrawingAnUnacceptedOfferLetsTheAttemptBeOfferedAgain is the other
// half of refusing a second offer. A plane that offered and died before
// committing its dispatch leaves an offer nobody will ever accept and nobody
// may replace; unless it can be withdrawn, the step is never dispatched.
func TestWithdrawingAnUnacceptedOfferLetsTheAttemptBeOfferedAgain(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	dead, err := mgr.Offer(ctx, "tenant-a", "run-1", "step-1", 1, testTTL)
	require.NoError(t, err)

	require.NoError(t, mgr.Withdraw(ctx, dead))
	require.ErrorIs(t, mgr.Validate(ctx, dead), lease.ErrFenced,
		"a withdrawn offer is still the step's lease")

	again, err := mgr.Offer(ctx, "tenant-a", "run-1", "step-1", 1, testTTL)
	require.NoError(t, err, "the attempt whose offer was withdrawn could not be offered again")
	require.Greater(t, again.Fence, dead.Fence)
	require.ErrorIs(t, mgr.Withdraw(ctx, dead), lease.ErrFenced,
		"withdrawing an old fence removed the offer that replaced it")
	require.NoError(t, mgr.Validate(ctx, again))
}

// TestWithdrawRemovesNothingButTheOfferItNames. A sweeper decides from what it
// read, and the record can change before it acts: a later attempt offered, or
// an engine accepting the offer. Removing either would fence out a dispatch
// that is out and being worked on.
func TestWithdrawRemovesNothingButTheOfferItNames(t *testing.T) {
	t.Run("superseded", func(t *testing.T) {
		ctx := testContext(t)
		mgr := newManager(ctx, t)
		old, err := mgr.Offer(ctx, "tenant-a", "run-1", "step-1", 1, testTTL)
		require.NoError(t, err)
		fresh, err := mgr.Offer(ctx, "tenant-a", "run-1", "step-1", 2, testTTL)
		require.NoError(t, err)

		require.ErrorIs(t, mgr.Withdraw(ctx, old), lease.ErrFenced)
		require.NoError(t, mgr.Validate(ctx, fresh), "withdrawing a superseded fence removed the current offer")
	})
	t.Run("accepted", func(t *testing.T) {
		ctx := testContext(t)
		mgr := newManager(ctx, t)
		token, err := mgr.Offer(ctx, "tenant-a", "run-1", "step-1", 1, testTTL)
		require.NoError(t, err)
		require.NoError(t, mgr.Renew(ctx, token))

		require.ErrorIs(t, mgr.Withdraw(ctx, token), lease.ErrFenced)
		require.NoError(t, mgr.Validate(ctx, token), "an offer an engine had accepted was withdrawn from under it")
	})
	t.Run("claimed", func(t *testing.T) {
		ctx := testContext(t)
		mgr := newManager(ctx, t)
		token, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 1, testTTL)
		require.NoError(t, err)

		require.ErrorIs(t, mgr.Withdraw(ctx, token), lease.ErrFenced)
		require.NoError(t, mgr.Validate(ctx, token), "a claim has a holder from the start and is not an offer to withdraw")
	})
}

// TestAClaimOfAnAttemptAlreadyLeasedIsRefusedAndLeavesTheFenceAlone is the
// offer's rule held by every lease. A cache hit and a plane-hosted step claim
// rather than offer, and a claim that superseded the fence a dispatch of the
// same attempt had already gone out under cost that attempt exactly as a
// second offer did: the dispatch's engine reports into a fence nobody honours.
func TestAClaimOfAnAttemptAlreadyLeasedIsRefusedAndLeavesTheFenceAlone(t *testing.T) {
	ctx := testContext(t)
	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	mgr := managerOn(ctx, t, srv.URL())
	other := managerOn(ctx, t, srv.URL())

	offered, err := mgr.Offer(ctx, "tenant-a", "run-1", "offered", 2, testTTL)
	require.NoError(t, err)
	claimed, err := mgr.Claim(ctx, "tenant-a", "run-1", "claimed", 2, testTTL)
	require.NoError(t, err)

	for step, held := range map[string]lease.Token{"offered": offered, "claimed": claimed} {
		for name, m := range map[string]*lease.KV{"the same plane": mgr, "another plane": other} {
			_, err = m.Claim(ctx, "tenant-a", "run-1", step, 2, testTTL)
			require.ErrorIs(t, err, lease.ErrAlreadyOffered,
				"%s claimed attempt 2 of a step already %s at attempt 2 and was not refused", name, step)
			_, err = m.Claim(ctx, "tenant-a", "run-1", step, 1, testTTL)
			require.ErrorIs(t, err, lease.ErrAlreadyOffered,
				"%s claimed an EARLIER attempt over a step %s at attempt 2 and was not refused", name, step)
		}
		require.NoError(t, mgr.Validate(ctx, held), "a refused claim moved the %s lease's fence", step)

		// A later attempt is a genuine retry, and still supersedes.
		next, err := other.Claim(ctx, "tenant-a", "run-1", step, 3, testTTL)
		require.NoError(t, err)
		require.Greater(t, next.Fence, held.Fence)
		require.ErrorIs(t, mgr.Validate(ctx, held), lease.ErrFenced)
	}
}

// TestAClaimWhoseHolderDiedExpiresAndTheAttemptCanBeClaimedAgain is what the
// refusal must not cost. A plane that claims a step and dies before recording
// anything leaves a claim no other plane may replace; it has a holder from the
// first instant, so it expires when that holder stops renewing, and the sweep
// that removes it frees the attempt for the next plane.
func TestAClaimWhoseHolderDiedExpiresAndTheAttemptCanBeClaimedAgain(t *testing.T) {
	ctx := testContext(t)
	mgr := newManager(ctx, t)

	dead, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 1, 100*time.Millisecond)
	require.NoError(t, err)
	_, err = mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 1, testTTL)
	require.ErrorIs(t, err, lease.ErrAlreadyOffered)

	require.Eventually(t, func() bool {
		orphans, expErr := mgr.Expire(ctx)
		require.NoError(t, expErr)
		return len(orphans) == 1
	}, 5*time.Second, 20*time.Millisecond, "a claim nobody renewed never expired")

	again, err := mgr.Claim(ctx, "tenant-a", "run-1", "step-1", 1, testTTL)
	require.NoError(t, err, "the attempt behind a swept claim could not be claimed again")
	require.Greater(t, again.Fence, dead.Fence)
}
