package runstore_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/runstore"
)

const tenant = "tenant-a"

// TestControlPlaneRestartMidRunReplaysWithoutDuplication is the reason the run
// store exists. A run is not a goroutine: its position lives in a persisted
// event log, so a control plane that dies halfway through a run rebuilds that
// position by replaying rather than losing it. Closing and reopening the store
// stands in for the restart.
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
	for _, e := range events {
		require.NoError(t, store.Append(ctx, tenant, e))
	}

	last, err := store.LastSequence(ctx, tenant)
	require.NoError(t, err)
	require.Equal(t, uint64(5), last)

	require.NoError(t, store.Close())

	reopened, err := runstore.NewSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	replayed, err := reopened.Replay(ctx, tenant, "run-1")
	require.NoError(t, err)
	require.Len(t, replayed, 5)

	for i, want := range events {
		require.Equal(t, want.Sequence, replayed[i].Sequence, "events must replay in sequence order")
		require.Equal(t, want.Type, replayed[i].Type)
		require.Equal(t, want.RunID, replayed[i].RunID)
		require.Equal(t, want.StepID, replayed[i].StepID)
		require.Equal(t, want.Attempt, replayed[i].Attempt)
		require.True(t, want.At.Equal(replayed[i].At))
	}
	require.Equal(t, []byte(`{"n":1}`), replayed[0].Payload)

	last, err = reopened.LastSequence(ctx, tenant)
	require.NoError(t, err)
	require.Equal(t, uint64(5), last)
}

// TestAppendIsIdempotentOnDuplicateSequence catches a redelivered append
// duplicating history. The scheduler resumes from LastSequence after a
// restart and may re-append what it already wrote; that must be a no-op, not
// an error and not a second copy.
func TestAppendIsIdempotentOnDuplicateSequence(t *testing.T) {
	ctx := context.Background()
	store := open(t)

	e := runstore.Event{
		RunID: "run-1", StepID: "build", Attempt: 2, Sequence: 7,
		Type: runstore.StepFailed, Payload: []byte("boom"),
		At: time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC),
	}
	require.NoError(t, store.Append(ctx, tenant, e))
	require.NoError(t, store.Append(ctx, tenant, e))

	replayed, err := store.Replay(ctx, tenant, "run-1")
	require.NoError(t, err)
	require.Len(t, replayed, 1)
}

// TestReplayIsScopedToTenant catches a query that forgets its tenant filter:
// one tenant's run must never surface another's events.
func TestReplayIsScopedToTenant(t *testing.T) {
	ctx := context.Background()
	store := open(t)
	at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

	require.NoError(t, store.Append(ctx, tenant, runstore.Event{RunID: "run-1", Sequence: 1, Type: runstore.RunCreated, At: at}))
	require.NoError(t, store.Append(ctx, "tenant-b", runstore.Event{RunID: "run-1", Sequence: 1, Type: runstore.RunCreated, At: at}))

	replayed, err := store.Replay(ctx, "tenant-b", "run-1")
	require.NoError(t, err)
	require.Len(t, replayed, 1)

	last, err := store.LastSequence(ctx, "tenant-b")
	require.NoError(t, err)
	require.Equal(t, uint64(1), last)
}

// TestQueryWithoutTenantIsRejected holds the constraint that there is no
// unscoped query in this system, even while only one tenant exists. An empty
// tenant is a bug in the caller, never a wildcard.
func TestQueryWithoutTenantIsRejected(t *testing.T) {
	ctx := context.Background()
	store := open(t)

	_, err := store.Replay(ctx, "", "run-1")
	require.ErrorContains(t, err, "tenant scope required")

	_, err = store.LastSequence(ctx, "")
	require.ErrorContains(t, err, "tenant scope required")

	err = store.Append(ctx, "", runstore.Event{RunID: "run-1", Sequence: 1, Type: runstore.RunCreated})
	require.ErrorContains(t, err, "tenant scope required")
}

// TestLastSequenceOnEmptyTenantStartsAtZero: a scheduler resuming a tenant
// that has never run must get 0, not an error.
func TestLastSequenceOnEmptyTenantStartsAtZero(t *testing.T) {
	ctx := context.Background()
	store := open(t)

	last, err := store.LastSequence(ctx, tenant)
	require.NoError(t, err)
	require.Equal(t, uint64(0), last)
}

func open(t *testing.T) runstore.Store {
	t.Helper()
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}
