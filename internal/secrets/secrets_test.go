package secrets_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/secrets"
)

// TestAHandleIsRefusedTheSecondTimeItIsRedeemed holds the single-use rule from
// docs/wire-contract.md. A handle that could be redeemed twice is a bearer
// credential with a TTL rather than a one-shot reference: a dispatch is
// replayed after a lost ack, and the replay must not be able to read the value
// the first attempt already took.
func TestAHandleIsRefusedTheSecondTimeItIsRedeemed(t *testing.T) {
	b := secrets.NewBroker()
	ref, err := b.Issue("t1", "TOKEN", "correcthorsebatterystaple", time.Minute)
	require.NoError(t, err)

	value, err := b.Redeem(ref.GetHandle())
	require.NoError(t, err)
	require.Equal(t, "correcthorsebatterystaple", value)

	_, err = b.Redeem(ref.GetHandle())
	require.Error(t, err, "a handle is single-use")
}

// TestAnExpiredHandleIsRefused holds SecretRef.expires_at: the issuer enforces
// the TTL, because it is the only party that knows when it issued.
func TestAnExpiredHandleIsRefused(t *testing.T) {
	b := secrets.NewBroker()
	ref, err := b.Issue("t1", "TOKEN", "value", -time.Second)
	require.NoError(t, err)

	_, err = b.Redeem(ref.GetHandle())
	require.Error(t, err, "a handle past expires_at is refused")
}

// TestARefusalNamesNeitherTheHandleNorTheValue is why refusals are checked at
// all. The refusal travels back over the bus and an engine puts it in a
// JobStatus error, which is durable and archived: a handle or a value in one
// is a credential at rest in the run history.
func TestARefusalNamesNeitherTheHandleNorTheValue(t *testing.T) {
	b := secrets.NewBroker()
	ref, err := b.Issue("t1", "TOKEN", "correcthorsebatterystaple", time.Minute)
	require.NoError(t, err)
	_, err = b.Redeem(ref.GetHandle())
	require.NoError(t, err)

	_, err = b.Redeem(ref.GetHandle())
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
	b := secrets.NewBroker()
	_, err := b.Issue("t1", "TOKEN", "ERR not really an error", time.Minute)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "not really an error",
		"the refusal must not echo the value it refused")
}

// TestABusRedeemerExchangesAHandleForItsValueOverTheRedemptionSubject is the
// round trip the wire contract describes: a raw request carrying the handle on
// secret.redeem, a raw reply carrying the value.
func TestABusRedeemerExchangesAHandleForItsValueOverTheRedemptionSubject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)

	broker := secrets.NewBroker()
	stop, err := secrets.Serve(ctx, plane, broker, bus.SubjectSecretRedeem())
	require.NoError(t, err)
	t.Cleanup(stop)

	ref, err := broker.Issue("t1", "TOKEN", "correcthorsebatterystaple", time.Minute)
	require.NoError(t, err)

	engineConn, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(engineConn.Close)

	redeemer := secrets.NewBusRedeemer(engineConn, bus.SubjectSecretRedeem())
	value, err := redeemer.Redeem(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, "correcthorsebatterystaple", value)

	// And the refusal path, over the same wire.
	_, err = redeemer.Redeem(ctx, ref)
	require.Error(t, err, "the second redemption of a handle is refused")
	require.NotContains(t, err.Error(), ref.GetHandle())
	require.NotContains(t, strings.ToLower(err.Error()), "correcthorse")
}
