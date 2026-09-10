package server

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestTheBrokerServesBeforeAnythingThatRedeemsFromIt holds the start-up
// ordering rule ADR 0024 names as a consequence of the plane redeeming its own
// secrets.
//
// The plane's first `builtin:llm` step is taken by the advance loop, and it
// redeems its credential over the very endpoint this process serves. A plane
// that started advancing runs before the broker was answering would fail that
// step on a race — the first LLM step of every fresh plane, intermittently,
// with a refusal that by design names nothing. The hosted engine has the same
// problem for the same reason: it advertises CAPABILITY_SECRETS on the
// strength of having a redemption endpoint.
//
// The ordering is asserted rather than left as the order of statements in
// serve, because that is an ordering the next edit reverses silently.
func TestTheBrokerServesBeforeAnythingThatRedeemsFromIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	dir := t.TempDir()
	srv, err := New(Config{
		APIAddr:  "127.0.0.1:0",
		Mode:     ModeEmbedded,
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

	order := srv.startupSequence()
	broker := slices.Index(order, "secret redemption")
	require.NotEqual(t, -1, broker, "the plane came up without serving redemptions at all: %v", order)

	for _, redeemer := range []string{"advancing runs", "builtin step types", "the hosted engine"} {
		at := slices.Index(order, redeemer)
		require.NotEqual(t, -1, at, "%q never started: %v", redeemer, order)
		require.Less(t, broker, at,
			"%q started before the broker was serving, so the first step that redeems races it: %v",
			redeemer, order)
	}
}
