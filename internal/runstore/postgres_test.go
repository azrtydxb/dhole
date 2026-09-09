package runstore_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// dsn is the live Postgres the integration suite talks to. A test that cannot
// reach its dependency skips with a reason; it never passes quietly, because
// an integration test that silently degrades to nothing reports green while
// testing nothing.
func dsn(t *testing.T) string {
	t.Helper()
	d := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if d == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set")
	}
	return d
}

// TestPostgresSatisfiesStoreContract holds the Postgres store to the same
// contract as SQLite. Two implementations of one interface are only worth
// having if both are held to one contract; anything asserted here is asserted
// of SQLite by the same helper, so behaviour cannot drift between the
// development store and the tuned target.
func TestPostgresSatisfiesStoreContract(t *testing.T) {
	ctx := context.Background()
	store, err := runstore.NewPostgres(ctx, dsn(t))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	runStoreContract(t, store)
}

// TestPostgresReplaysAcrossReconnect is the restart case for Postgres: the
// control plane dying is a new connection to the same database, and the run's
// position must come back out of the log rather than be lost with the process.
func TestPostgresReplaysAcrossReconnect(t *testing.T) {
	ctx := context.Background()
	d := dsn(t)

	store, err := runstore.NewPostgres(ctx, d)
	require.NoError(t, err)

	tenant := uniqueTenant(t)
	at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	require.NoError(t, store.Append(ctx, tenant, runstore.Event{
		RunID: "run-1", Sequence: 1, Type: runstore.RunCreated,
		Payload: []byte(`{"n":1}`), At: at,
	}))
	require.NoError(t, store.Close())

	reopened, err := runstore.NewPostgres(ctx, d)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	replayed, err := reopened.Replay(ctx, tenant, "run-1")
	require.NoError(t, err)
	require.Len(t, replayed, 1)
	require.Equal(t, []byte(`{"n":1}`), replayed[0].Payload)
	require.True(t, at.Equal(replayed[0].At))

	last, err := reopened.LastSequence(ctx, tenant)
	require.NoError(t, err)
	require.Equal(t, uint64(1), last)
}
