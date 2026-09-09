package runstore_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// TestSQLiteSatisfiesStoreContract holds the development and homelab store to
// the same contract as the tuned Postgres target. The assertions live in
// contract_test.go and are run by both, so a behaviour a user relies on in
// development cannot quietly differ in production.
func TestSQLiteSatisfiesStoreContract(t *testing.T) {
	runStoreContract(t, open(t))
}

// TestControlPlaneRestartMidRunReplaysWithoutDuplication is the reason the run
// store exists. A run is not a goroutine: its position lives in a persisted
// event log, so a control plane that dies halfway through a run rebuilds that
// position by replaying rather than losing it. Closing and reopening the store
// stands in for the restart. Its Postgres counterpart reconnects to the same
// database instead of reopening a file.
func TestControlPlaneRestartMidRunReplaysWithoutDuplication(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "runs.db")

	store, err := runstore.NewSQLite(path)
	require.NoError(t, err)

	at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	events := []runstore.Event{
		{RunID: "run-1", Sequence: 1, Type: runstore.RunCreated, Payload: []byte(`{"n":1}`), At: at},
		{RunID: "run-1", StepID: "build", Attempt: 1, Sequence: 2, Type: runstore.StepReady, At: at.Add(time.Second)},
		{RunID: "run-1", StepID: "build", Attempt: 1, Sequence: 3, Type: runstore.StepDispatched, At: at.Add(2 * time.Second)},
		{RunID: "run-1", StepID: "build", Attempt: 1, Sequence: 4, Type: runstore.StepSucceeded, At: at.Add(3 * time.Second)},
		{RunID: "run-1", Sequence: 5, Type: runstore.RunCompleted, At: at.Add(4 * time.Second)},
	}
	tenant := uniqueTenant(t)
	for _, e := range events {
		require.NoError(t, store.Append(ctx, tenant, e))
	}
	require.NoError(t, store.Close())

	reopened, err := runstore.NewSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	replayed, err := reopened.Replay(ctx, tenant, "run-1")
	require.NoError(t, err)
	require.Len(t, replayed, 5)
	for i, want := range events {
		require.Equal(t, want.Sequence, replayed[i].Sequence)
		require.Equal(t, want.Type, replayed[i].Type)
		require.True(t, want.At.Equal(replayed[i].At))
	}

	last, err := reopened.LastSequence(ctx, tenant)
	require.NoError(t, err)
	require.Equal(t, uint64(5), last)
}

func open(t *testing.T) runstore.Store {
	t.Helper()
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}
