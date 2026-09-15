package secrets_test

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/secrets"
)

// sharedValue is the step secret every multi-plane case resolves. It is long
// and unlikely so that a scan for it has one honest answer.
const sharedValue = "multiplane-correcthorsebatterystaple"

// modelValue is the plane's own credential, resolved by the other kind of
// handle.
const modelValue = "multiplane-model-key-tr0ub4dor"

var harborRef = secrets.Reference{
	Source: secrets.SourceStep, Secret: "harbor-robot", Binding: "REGISTRY_PASSWORD",
}

// plane is one control-plane replica as far as secrets go: its own broker,
// its own connection to the bucket, and its own responder.
type plane struct {
	broker *secrets.Broker
	steps  *secrets.StepIssuer
	bus    *bus.NATS
	served *countingResponder
}

// twoPlanes stands two replicas up over ONE embedded bus, each with its own
// connections and its own broker, sharing nothing but what the bus holds —
// which is exactly what two pods of `controlPlane.replicas: 2` share.
func twoPlanes(ctx context.Context, t *testing.T) (a, b *plane, srv *bus.Embedded) {
	t.Helper()
	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	return newPlane(ctx, t, srv.URL()), newPlane(ctx, t, srv.URL()), srv
}

func newPlane(ctx context.Context, t *testing.T, url string) *plane {
	t.Helper()
	conn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	handles, err := secrets.NewKVHandles(ctx, conn)
	require.NoError(t, err)

	broker := secrets.NewBroker(secrets.WithHandles(handles))
	steps := secrets.NewMapSource()
	steps.Set("acme", "harbor-robot", sharedValue)
	steps.Set("globex", "harbor-robot", "globex-"+sharedValue)
	issuer := secrets.NewStepIssuer(broker, steps)
	models := secrets.NewMapSource()
	models.Set("acme", "ANTHROPIC_API_KEY", modelValue)

	planeBus, err := bus.Connect(ctx, url)
	require.NoError(t, err)
	t.Cleanup(planeBus.Close)
	secrets.NewPlaneResolver(broker, models, planeBus, bus.SubjectSecretRedeem())
	return &plane{broker: broker, steps: issuer, bus: planeBus, served: &countingResponder{inner: planeBus}}
}

func (p *plane) serve(ctx context.Context, t *testing.T) {
	t.Helper()
	stop, err := secrets.ServeTenants(ctx, p.served, p.broker, bus.SubjectSecretRedeem())
	require.NoError(t, err)
	t.Cleanup(stop)
}

// countingResponder counts the requests a plane's responder actually handled.
type countingResponder struct {
	inner    secrets.SubjectResponder
	answered atomic.Int32
}

func (c *countingResponder) RespondRawSubjectQueue(
	ctx context.Context, subject, queue string, fn func(subject string, body []byte) []byte,
) (func(), error) {
	return c.inner.RespondRawSubjectQueue(ctx, subject, queue, func(s string, body []byte) []byte {
		c.answered.Add(1)
		return fn(s, body)
	})
}

var acmeAttempt = secrets.Scope{TenantID: "acme", RunID: "run-1", StepID: "push", Attempt: 1}

// TestAHandleIssuedOnOnePlaneIsRedeemedOnAnother is ADR 0031's first case. A
// handle held in the issuing plane's memory was refused by every other
// replica, so with two replicas a step received its credential only when its
// engine's request happened to reach the issuer.
func TestAHandleIssuedOnOnePlaneIsRedeemedOnAnother(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a, b, srv := twoPlanes(ctx, t)

	ref, err := a.broker.Issue(ctx, acmeAttempt, harborRef, time.Minute)
	require.NoError(t, err)
	value, err := b.broker.RedeemFor(ctx, "acme", ref.GetHandle())
	require.NoError(t, err, "a handle issued on plane A was refused by plane B")
	require.Equal(t, sharedValue, value)

	// And over the bus, with only the OTHER plane answering: this is the
	// engine's request landing on the replica that did not issue.
	b.serve(ctx, t)
	engine, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(engine.Close)
	fresh, err := a.broker.Issue(ctx, acmeAttempt, harborRef, time.Minute)
	require.NoError(t, err)
	value, err = secrets.NewBusRedeemer(engine, bus.SubjectSecretRedeem()).Redeem(ctx, "acme", fresh)
	require.NoError(t, err, "an engine's redemption answered by the replica that did not issue was refused")
	require.Equal(t, sharedValue, value)

	// The plane's own kind of handle travels the same way.
	own, err := a.broker.Issue(ctx, secrets.Scope{TenantID: "acme"},
		secrets.Reference{Source: secrets.SourcePlane, Secret: "ANTHROPIC_API_KEY", Binding: "ANTHROPIC_API_KEY"},
		time.Minute)
	require.NoError(t, err)
	value, err = b.broker.RedeemFor(ctx, "acme", own.GetHandle())
	require.NoError(t, err)
	require.Equal(t, modelValue, value, "a plane handle resolved from the step source, or not at all")
}

// TestAHandleIsSpentOnceWhicheverPlanesPresentItConcurrently: single use is
// the property that makes a handle a reference rather than a bearer
// credential, and two replicas each checking their own copy would both say
// yes. Run with -count=20.
func TestAHandleIsSpentOnceWhicheverPlanesPresentItConcurrently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a, b, _ := twoPlanes(ctx, t)

	ref, err := a.broker.Issue(ctx, acmeAttempt, harborRef, time.Minute)
	require.NoError(t, err)
	_, err = a.broker.RedeemFor(ctx, "acme", ref.GetHandle())
	require.NoError(t, err)
	_, err = b.broker.RedeemFor(ctx, "acme", ref.GetHandle())
	require.Error(t, err, "a handle spent on plane A was redeemed again on plane B")

	const presenters = 8
	for round := range 5 {
		ref, err := a.broker.Issue(ctx, acmeAttempt, harborRef, time.Minute)
		require.NoError(t, err)
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		for n := range presenters {
			// Even rounds mix the planes; odd rounds present only on the
			// plane that did NOT issue, so a winner cannot be the issuer's
			// own copy.
			broker := b.broker
			if round%2 == 0 && n%2 == 0 {
				broker = a.broker
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				// A wrong value is not a win, so it fails the count below.
				if v, err := broker.RedeemFor(ctx, "acme", ref.GetHandle()); err == nil && v == sharedValue {
					wins.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		require.Equal(t, int32(1), wins.Load(),
			"round %d: %d planes redeemed one single-use handle presented concurrently", round, wins.Load())
	}
}

// TestAHandleRevokedOnOnePlaneIsRefusedOnAnother: an attempt's end, a cancel
// and a run's end are processed by whichever replica owns the run or took the
// call, not by the one that issued.
func TestAHandleRevokedOnOnePlaneIsRefusedOnAnother(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a, b, _ := twoPlanes(ctx, t)

	byAttempt, err := b.broker.Issue(ctx, acmeAttempt, harborRef, time.Minute)
	require.NoError(t, err)
	byRun, err := b.broker.Issue(ctx, secrets.Scope{TenantID: "acme", RunID: "run-1", StepID: "sign", Attempt: 1},
		harborRef, time.Minute)
	require.NoError(t, err)
	byHandle, err := b.broker.Issue(ctx, secrets.Scope{TenantID: "acme", RunID: "run-2", StepID: "push", Attempt: 1},
		harborRef, time.Minute)
	require.NoError(t, err)
	otherTenant, err := b.broker.Issue(ctx, secrets.Scope{TenantID: "globex", RunID: "run-1", StepID: "push", Attempt: 1},
		harborRef, time.Minute)
	require.NoError(t, err)

	n, err := a.broker.RevokeAttempt(ctx, acmeAttempt)
	require.NoError(t, err)
	require.Equal(t, 1, n, "plane A revoked none of the attempt's handles plane B issued")
	_, err = b.broker.RedeemFor(ctx, "acme", byAttempt.GetHandle())
	require.Error(t, err, "a handle revoked by attempt on plane A was redeemed on plane B")

	n, err = a.broker.RevokeRun(ctx, "acme", "run-1")
	require.NoError(t, err)
	require.Equal(t, 1, n)
	_, err = b.broker.RedeemFor(ctx, "acme", byRun.GetHandle())
	require.Error(t, err, "a handle revoked by run on plane A was redeemed on plane B")

	n, err = a.broker.RevokeHandles(ctx, byHandle.GetHandle())
	require.NoError(t, err)
	require.Equal(t, 1, n)
	_, err = b.broker.RedeemFor(ctx, "acme", byHandle.GetHandle())
	require.Error(t, err, "a handle revoked by name on plane A was redeemed on plane B")

	value, err := b.broker.RedeemFor(ctx, "globex", otherTenant.GetHandle())
	require.NoError(t, err, "revoking tenant acme's run took tenant globex's run of the same id")
	require.Equal(t, "globex-"+sharedValue, value)
}

// TestAnExpiredSharedHandleIsRefused: the record carries the issuer's expiry,
// and the plane that answers enforces it whatever its own clock of issuing
// would have been.
func TestAnExpiredSharedHandleIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a, b, _ := twoPlanes(ctx, t)

	ref, err := a.broker.Issue(ctx, acmeAttempt, harborRef, 50*time.Millisecond)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	_, err = b.broker.RedeemFor(ctx, "acme", ref.GetHandle())
	require.Error(t, err, "a handle past its expiry was redeemed on another plane")

	_, err = a.broker.Issue(ctx, acmeAttempt, harborRef, secrets.HandleRetention+time.Minute)
	require.Error(t, err, "a handle outliving the bucket's retention would vanish before its expiry")
	_, err = a.broker.Issue(ctx, acmeAttempt, harborRef, 0)
	require.Error(t, err, "a handle needs a positive expiry")
}

// TestAHandleIsRefusedOnAnotherTenantAcrossPlanes: ADR 0030's refusal holds
// when the handle and the request meet on different replicas.
func TestAHandleIsRefusedOnAnotherTenantAcrossPlanes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a, b, _ := twoPlanes(ctx, t)

	ref, err := a.broker.Issue(ctx, acmeAttempt, harborRef, time.Minute)
	require.NoError(t, err)
	_, err = b.broker.RedeemFor(ctx, "globex", ref.GetHandle())
	require.Error(t, err, "tenant globex redeemed on plane B a handle plane A issued for acme")
	_, err = a.broker.RedeemFor(ctx, "acme", ref.GetHandle())
	require.Error(t, err, "a handle presented on another tenant's subject is spent by being presented")
}

// TestTheHandleBucketHoldsNoSecretValue scans every message the bucket's
// stream holds — live records, spent ones, revoked ones — for the values and
// for the handles themselves. A shared, replicated copy of either is the
// secret at rest ADR 0027 exists to avoid.
func TestTheHandleBucketHoldsNoSecretValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a, b, srv := twoPlanes(ctx, t)

	var handles []string
	for _, scope := range []secrets.Scope{
		acmeAttempt,
		{TenantID: "acme", RunID: "run-1", StepID: "sign", Attempt: 2},
		{TenantID: "globex", RunID: "run-9", StepID: "push", Attempt: 1},
	} {
		ref, err := a.broker.Issue(ctx, scope, harborRef, time.Minute)
		require.NoError(t, err)
		handles = append(handles, ref.GetHandle())
	}
	own, err := a.broker.Issue(ctx, secrets.Scope{TenantID: "acme"},
		secrets.Reference{Source: secrets.SourcePlane, Secret: "ANTHROPIC_API_KEY", Binding: "ANTHROPIC_API_KEY"},
		time.Minute)
	require.NoError(t, err)
	handles = append(handles, own.GetHandle())
	// And the way the scheduler issues: the step issuer is the one caller that
	// holds the value while it issues.
	refs, err := a.steps.Issue(ctx, secrets.Scope{TenantID: "acme", RunID: "run-3", StepID: "push", Attempt: 1},
		secretStep(&dholev1.StepSecret{Name: "harbor-robot", Env: "REGISTRY_PASSWORD"}), time.Minute)
	require.NoError(t, err)
	require.Len(t, refs, 1)
	handles = append(handles, refs[0].GetHandle())

	_, err = b.broker.RedeemFor(ctx, "acme", handles[0])
	require.NoError(t, err)
	_, err = b.broker.RevokeAttempt(ctx, secrets.Scope{TenantID: "acme", RunID: "run-1", StepID: "sign", Attempt: 2})
	require.NoError(t, err)

	conn, err := nats.Connect(srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	js, err := jetstream.New(conn)
	require.NoError(t, err)
	stream, err := js.Stream(ctx, "KV_"+secrets.HandleBucket)
	require.NoError(t, err)
	info, err := stream.Info(ctx)
	require.NoError(t, err)
	require.Positive(t, info.State.Msgs, "nothing was written to the bucket, so this scan proves nothing")

	scanned := 0
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		msg, err := stream.GetMsg(ctx, seq)
		if err != nil {
			continue
		}
		scanned++
		raw := append([]byte(msg.Subject), msg.Data...)
		for k, vs := range msg.Header {
			raw = append(raw, k...)
			for _, v := range vs {
				raw = append(raw, v...)
			}
		}
		for _, secret := range []string{sharedValue, "globex-" + sharedValue, modelValue} {
			require.False(t, bytes.Contains(raw, []byte(secret)),
				"the handle bucket holds a secret VALUE in %s", msg.Subject)
		}
		for _, h := range handles {
			require.False(t, bytes.Contains(raw, []byte(h)),
				"the handle bucket holds a redeemable HANDLE in %s", msg.Subject)
		}
	}
	require.Positive(t, scanned)
}

// TestExactlyOnePlaneAnswersARedemption: every replica subscribes to the
// redemption subject, and a plain subscription made each of them answer. The
// engine took the first reply, so a refusal from the wrong replica could beat
// the issuer's value — and the refusing replica spent nothing.
func TestExactlyOnePlaneAnswersARedemption(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a, b, srv := twoPlanes(ctx, t)
	a.serve(ctx, t)
	b.serve(ctx, t)

	engine, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(engine.Close)
	redeemer := secrets.NewBusRedeemer(engine, bus.SubjectSecretRedeem())

	const requests = 10
	for range requests {
		ref, err := a.broker.Issue(ctx, acmeAttempt, harborRef, time.Minute)
		require.NoError(t, err)
		value, err := redeemer.Redeem(ctx, "acme", ref)
		require.NoError(t, err)
		require.Equal(t, sharedValue, value)
	}
	// Late answers arrive after the first reply; give them the chance to.
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, int32(requests), a.served.answered.Load()+b.served.answered.Load(),
		"%d redemptions were answered %d times across two planes", requests,
		a.served.answered.Load()+b.served.answered.Load())
}

// TestAHandleOfARunClosedOutsideTheSchedulerIsRevokedBySweep: an approval
// denied and a halting model step write RUN_FAILED without the scheduler, so
// nothing on those paths revokes. The sweep asks each tenant's open-run index
// and revokes every handle whose run is not in it — reading the handles
// FIRST, so a run created while it sweeps is never taken for a closed one.
func TestAHandleOfARunClosedOutsideTheSchedulerIsRevokedBySweep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a, b, _ := twoPlanes(ctx, t)

	closed, err := a.broker.Issue(ctx, acmeAttempt, harborRef, time.Minute)
	require.NoError(t, err)
	open, err := a.broker.Issue(ctx, secrets.Scope{TenantID: "acme", RunID: "run-open", StepID: "push", Attempt: 1},
		harborRef, time.Minute)
	require.NoError(t, err)
	otherTenant, err := a.broker.Issue(ctx, secrets.Scope{TenantID: "globex", RunID: "run-1", StepID: "push", Attempt: 1},
		harborRef, time.Minute)
	require.NoError(t, err)
	own, err := a.broker.Issue(ctx, secrets.Scope{TenantID: "acme"},
		secrets.Reference{Source: secrets.SourcePlane, Secret: "ANTHROPIC_API_KEY", Binding: "ANTHROPIC_API_KEY"},
		time.Minute)
	require.NoError(t, err)

	var born string
	openRuns := func(ctx context.Context, tenantID string) ([]string, error) {
		if tenantID == "acme" && born == "" {
			// A run created, and issued, between the sweep listing handles
			// and reading the index: it is not in the index this returns.
			ref, err := a.broker.Issue(ctx, secrets.Scope{TenantID: "acme", RunID: "run-new", StepID: "push", Attempt: 1},
				harborRef, time.Minute)
			require.NoError(t, err)
			born = ref.GetHandle()
		}
		switch tenantID {
		case "acme":
			return []string{"run-open"}, nil
		case "globex":
			return []string{"run-1"}, nil
		}
		return nil, nil
	}
	n, err := b.broker.RevokeClosedRuns(ctx, openRuns)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the sweep revoked %d handles; only the closed run's one is closed", n)

	_, err = a.broker.RedeemFor(ctx, "acme", closed.GetHandle())
	require.Error(t, err, "a handle of a run no longer open survived the sweep")
	for name, h := range map[string]string{
		"an open run's": open.GetHandle(), "a run created during the sweep's": born, "the plane's own": own.GetHandle(),
	} {
		_, err = a.broker.RedeemFor(ctx, "acme", h)
		require.NoError(t, err, "the sweep revoked %s handle", name)
	}
	_, err = a.broker.RedeemFor(ctx, "globex", otherTenant.GetHandle())
	require.NoError(t, err, "the sweep read one tenant's index for another tenant's run")
}

// TestARedemptionIsAnsweredAfterTheContextItWasServedUnderEnds: the plane
// starts its responders under a start-up context with a deadline, and the
// responders live long after it. A redemption that did its store work under
// that context would be refused by every plane the moment start-up finished.
func TestARedemptionIsAnsweredAfterTheContextItWasServedUnderEnds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	a, _, srv := twoPlanes(ctx, t)

	startCtx, endStart := context.WithTimeout(ctx, 10*time.Second)
	a.serve(startCtx, t)
	endStart()

	engine, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(engine.Close)
	ref, err := a.broker.Issue(ctx, acmeAttempt, harborRef, time.Minute)
	require.NoError(t, err)
	value, err := secrets.NewBusRedeemer(engine, bus.SubjectSecretRedeem()).Redeem(ctx, "acme", ref)
	require.NoError(t, err, "a plane refused every redemption once its start-up context ended")
	require.Equal(t, sharedValue, value)
}
