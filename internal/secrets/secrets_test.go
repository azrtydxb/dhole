package secrets_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/secrets"
)

// TestAHandleIsRefusedTheSecondTimeItIsRedeemed holds the single-use rule from
// docs/wire-contract.md. A handle that could be redeemed twice is a bearer
// credential with a TTL rather than a one-shot reference: a dispatch is
// replayed after a lost ack, and the replay must not be able to read the value
// the first attempt already took.
func TestAHandleIsRefusedTheSecondTimeItIsRedeemed(t *testing.T) {
	ctx := context.Background()
	b := stepBroker("t1", "TOKEN", "correcthorsebatterystaple")
	ref := issue(t, b, secrets.Scope{TenantID: "t1"}, "TOKEN", time.Minute)

	value, err := b.Redeem(ctx, ref.GetHandle())
	require.NoError(t, err)
	require.Equal(t, "correcthorsebatterystaple", value)

	_, err = b.Redeem(ctx, ref.GetHandle())
	require.Error(t, err, "a handle is single-use")
}

// TestAnExpiredHandleIsRefused holds SecretRef.expires_at: the issuer enforces
// the TTL, because it is the only party that knows when it issued.
func TestAnExpiredHandleIsRefused(t *testing.T) {
	b := stepBroker("t1", "TOKEN", "value")
	ref := issue(t, b, secrets.Scope{TenantID: "t1"}, "TOKEN", time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	_, err := b.Redeem(context.Background(), ref.GetHandle())
	require.Error(t, err, "a handle past expires_at is refused")
}

// TestARefusalNamesNeitherTheHandleNorTheValue is why refusals are checked at
// all. The refusal travels back over the bus and an engine puts it in a
// JobStatus error, which is durable and archived: a handle or a value in one
// is a credential at rest in the run history.
func TestARefusalNamesNeitherTheHandleNorTheValue(t *testing.T) {
	ctx := context.Background()
	b := stepBroker("t1", "TOKEN", "correcthorsebatterystaple")
	ref := issue(t, b, secrets.Scope{TenantID: "t1"}, "TOKEN", time.Minute)
	_, err := b.Redeem(ctx, ref.GetHandle())
	require.NoError(t, err)

	_, err = b.Redeem(ctx, ref.GetHandle())
	require.Error(t, err)
	require.NotContains(t, err.Error(), ref.GetHandle())
	require.NotContains(t, err.Error(), "correcthorsebatterystaple")
}

// TestIssuingAValueThatWouldReadAsARefusalIsRejectedAtIssueTime is the price of
// the reply shape. A refusal is a reply beginning "ERR ", so a VALUE beginning
// "ERR " is indistinguishable from one. The ambiguity is resolved where it can
// be seen — at the moment somebody stores such a secret — rather than at
// redemption, where an engine would silently fail a step it could have run.
func TestIssuingAValueThatWouldReadAsARefusalIsRejectedAtIssueTime(t *testing.T) {
	ctx := context.Background()
	b := stepBroker("acme", "harbor-robot", "ERR not really an error")
	issuer := secrets.NewStepIssuer(b, nil)
	_, err := issuer.Issue(ctx, attemptOne,
		secretStep(&dholev1.StepSecret{Name: "harbor-robot", Env: "TOKEN"}), time.Minute)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "not really an error",
		"the refusal must not echo the value it refused")

	// And where the value became one after its handle was issued, the
	// redemption refuses rather than send a value that reads as an error.
	ref := issue(t, b, secrets.Scope{TenantID: "acme"}, "harbor-robot", time.Minute)
	_, err = b.Redeem(ctx, ref.GetHandle())
	require.Error(t, err)
	require.NotContains(t, err.Error(), "not really an error")
}

// TestABusRedeemerExchangesAHandleForItsValueOverTheRedemptionSubject is the
// round trip the wire contract describes: a raw request carrying the handle on
// secret.redeem.<tenant>, a raw reply carrying the value.
func TestABusRedeemerExchangesAHandleForItsValueOverTheRedemptionSubject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	plane, engineConn := redemptionBus(ctx, t)
	broker := stepBroker("t1", "TOKEN", "correcthorsebatterystaple")
	stop, err := secrets.ServeTenants(ctx, plane, broker, bus.SubjectSecretRedeem())
	require.NoError(t, err)
	t.Cleanup(stop)

	ref := issue(t, broker, secrets.Scope{TenantID: "t1"}, "TOKEN", time.Minute)

	wire := &subjectRecorder{inner: engineConn}
	redeemer := secrets.NewBusRedeemer(wire, bus.SubjectSecretRedeem())
	value, err := redeemer.Redeem(ctx, "t1", ref)
	require.NoError(t, err)
	require.Equal(t, "correcthorsebatterystaple", value)
	require.Equal(t, []string{bus.SubjectSecretRedeemFor("t1")}, wire.all(),
		"the redeemer must ask on its tenant's subject, not the unscoped one")

	// And the refusal path, over the same wire.
	_, err = redeemer.Redeem(ctx, "t1", ref)
	require.Error(t, err, "the second redemption of a handle is refused")
	require.NotContains(t, err.Error(), ref.GetHandle())
	require.NotContains(t, strings.ToLower(err.Error()), "correcthorse")

	_, err = redeemer.Redeem(ctx, "", ref)
	require.Error(t, err, "there is no unscoped redemption from an engine that knows its tenant")
}

// TestAHandleIsRefusedOnAnotherTenantsSubject is ADR 0030's plane half. The
// broker answers by handle, and before this the handle was all it ever saw: a
// handle presented by an engine of another tenant was redeemed. The subject a
// request arrives on names the tenant, so a handle issued for another is
// refused — with the same one refusal as every other, and spent by having
// been presented.
func TestAHandleIsRefusedOnAnotherTenantsSubject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	plane, engineConn := redemptionBus(ctx, t)
	broker := stepBroker("acme", "TOKEN", "acme-value")
	stop, err := secrets.ServeTenants(ctx, plane, broker, bus.SubjectSecretRedeem())
	require.NoError(t, err)
	t.Cleanup(stop)
	redeemer := secrets.NewBusRedeemer(engineConn, bus.SubjectSecretRedeem())

	acme := issue(t, broker, secrets.Scope{TenantID: "acme"}, "TOKEN", time.Minute)
	_, err = redeemer.Redeem(ctx, "globex", acme)
	require.Error(t, err, "tenant globex redeemed a handle issued for tenant acme")
	require.NotContains(t, err.Error(), "acme-value")
	require.NotContains(t, err.Error(), acme.GetHandle())

	_, err = redeemer.Redeem(ctx, "acme", acme)
	require.Error(t, err, "a handle presented on the wrong tenant's subject is spent by being presented")

	fresh := issue(t, broker, secrets.Scope{TenantID: "acme"}, "TOKEN", time.Minute)
	value, err := redeemer.Redeem(ctx, "acme", fresh)
	require.NoError(t, err, "the owning tenant was refused its own handle")
	require.Equal(t, "acme-value", value)

	_, err = broker.RedeemFor(ctx, "", fresh.GetHandle())
	require.Error(t, err, "a redemption naming no tenant is not a redemption for every tenant")
}

// TestAnEngineFallsBackToTheLegacySubjectWhenNothingServesTheScopedOne is the
// other direction of the compatibility window: an engine upgraded before its
// plane. A plane older than ADR 0030 serves only the unscoped subject, so a
// request on the scoped one has no responder at all — and the engine asks
// there instead rather than failing a step its plane could have served.
func TestAnEngineFallsBackToTheLegacySubjectWhenNothingServesTheScopedOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	plane, engineConn := redemptionBus(ctx, t)
	broker := stepBroker("acme", "TOKEN", "legacy-value")
	stop, err := secrets.Serve(ctx, plane, broker, bus.SubjectSecretRedeem())
	require.NoError(t, err)
	t.Cleanup(stop)

	ref := issue(t, broker, secrets.Scope{TenantID: "acme"}, "TOKEN", time.Minute)

	wire := &subjectRecorder{inner: engineConn}
	value, err := secrets.NewBusRedeemer(wire, bus.SubjectSecretRedeem()).Redeem(ctx, "acme", ref)
	require.NoError(t, err, "an engine could not redeem through a plane that predates the scoped subject")
	require.Equal(t, "legacy-value", value)
	require.Equal(t, []string{bus.SubjectSecretRedeemFor("acme"), bus.SubjectSecretRedeem()}, wire.all(),
		"the scoped subject is asked first and the legacy one only when nothing answers it")
}

// redemptionBus is an embedded bus with a plane connection and an engine one.
func redemptionBus(ctx context.Context, t *testing.T) (*bus.NATS, *bus.NATS) {
	t.Helper()
	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)
	engineConn, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(engineConn.Close)
	return plane, engineConn
}

// subjectRecorder notes the subject of every request that crosses it.
type subjectRecorder struct {
	inner    secrets.Requester
	mu       sync.Mutex
	subjects []string
}

func (r *subjectRecorder) RequestRaw(ctx context.Context, subject string, body []byte) ([]byte, error) {
	r.mu.Lock()
	r.subjects = append(r.subjects, subject)
	r.mu.Unlock()
	return r.inner.RequestRaw(ctx, subject, body)
}

func (r *subjectRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.subjects...)
}

// TestRevokingAnAttemptLeavesEveryOtherAttemptsHandles is ADR 0030's revocation
// rule at the broker. An attempt that ends takes its unspent handles with it,
// and nothing else: not the retry of the same step, which may already have
// been issued, not a sibling step, and not another run or another tenant's run
// of the same id.
func TestRevokingAnAttemptLeavesEveryOtherAttemptsHandles(t *testing.T) {
	ctx := context.Background()
	b := stepBroker("acme", "TOKEN", "value", "globex", "TOKEN", "value")
	issued := func(scope secrets.Scope) string {
		t.Helper()
		return issue(t, b, scope, "TOKEN", time.Minute).GetHandle()
	}
	ended := secrets.Scope{TenantID: "acme", RunID: "run-1", StepID: "push", Attempt: 1}
	endedA, endedB := issued(ended), issued(ended)
	retry := issued(secrets.Scope{TenantID: "acme", RunID: "run-1", StepID: "push", Attempt: 2})
	sibling := issued(secrets.Scope{TenantID: "acme", RunID: "run-1", StepID: "sign", Attempt: 1})
	otherRun := issued(secrets.Scope{TenantID: "acme", RunID: "run-2", StepID: "push", Attempt: 1})
	otherTenant := issued(secrets.Scope{TenantID: "globex", RunID: "run-1", StepID: "push", Attempt: 1})

	n, err := b.RevokeAttempt(ctx, ended)
	require.NoError(t, err)
	require.Equal(t, 2, n, "an ended attempt's two unspent handles were not both revoked")
	for _, h := range []string{endedA, endedB} {
		_, err := b.Redeem(ctx, h)
		require.Error(t, err, "a handle of an ended attempt is still redeemable")
	}
	n, err = b.RevokeAttempt(ctx, secrets.Scope{TenantID: "acme", RunID: "run-1", StepID: "push"})
	require.NoError(t, err)
	require.Zero(t, n, "a scope naming no attempt is not every attempt")

	n, err = b.RevokeRun(ctx, "acme", "run-1")
	require.NoError(t, err)
	require.Equal(t, 2, n, "a run's remaining handles were not revoked with it")
	for _, h := range []string{retry, sibling} {
		_, err := b.Redeem(ctx, h)
		require.Error(t, err, "a handle of a revoked run is still redeemable")
	}
	for _, h := range []string{otherRun, otherTenant} {
		_, err := b.Redeem(ctx, h)
		require.NoError(t, err, "revoking one run took a handle of another")
	}
	n, err = b.RevokeRun(ctx, "", "run-1")
	require.NoError(t, err)
	require.Zero(t, n, "a revocation naming no tenant is not every tenant's")
}

// stepBroker is an in-memory broker whose step handles resolve from a source
// holding the given tenant, name, value triples.
func stepBroker(triples ...string) *secrets.Broker {
	src := secrets.NewMapSource()
	for i := 0; i+2 < len(triples); i += 3 {
		src.Set(triples[i], triples[i+1], triples[i+2])
	}
	b := secrets.NewBroker()
	secrets.NewStepIssuer(b, src)
	return b
}

// issue mints a step handle for the secret called name, bound to the same name.
func issue(t *testing.T, b *secrets.Broker, scope secrets.Scope, name string, ttl time.Duration) *dholev1.SecretRef {
	t.Helper()
	ref, err := b.Issue(context.Background(), scope,
		secrets.Reference{Source: secrets.SourceStep, Secret: name, Binding: name}, ttl)
	require.NoError(t, err)
	return ref
}
