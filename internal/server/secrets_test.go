package server_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/secrets"
	"github.com/azrtydxb/dhole/internal/server"
)

// TestTheControlPlaneAnswersASecretRedemptionOnTheContractsSubject is the other
// end of the engine's redeemer. A SecretRef that nothing can redeem is not a
// feature: the engine advertises CAPABILITY_SECRETS on the strength of having
// a redemption endpoint, and this is the endpoint.
func TestTheControlPlaneAnswersASecretRedemptionOnTheContractsSubject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	dir := t.TempDir()
	srv, err := server.New(server.Config{
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		require.NoError(t, srv.Stop(stopCtx))
	})

	ref, err := srv.Secrets().Issue("t1", "DHOLE_TEST_SECRET", "correcthorsebatterystaple", time.Minute)
	require.NoError(t, err)

	// Redeemed the way an engine does it: over the bus, on the subject the
	// wire contract names, holding nothing but the handle.
	conn, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	redeemer := secrets.NewBusRedeemer(conn, bus.SubjectSecretRedeem())
	value, err := redeemer.Redeem(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, "correcthorsebatterystaple", value)

	_, err = redeemer.Redeem(ctx, ref)
	require.Error(t, err, "a handle is single-use, and the plane is what enforces it")
}
