package runstore_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// tenantSeq and tenantRun keep every contract run in its own tenant. SQLite
// gets a fresh file per test, but Postgres is a shared, persistent database:
// reusing a tenant would make the assertions depend on what an earlier run of
// the same test left behind — and an assertion that only holds because the
// store deduplicated the leftovers is not testing what it claims to. The run
// nonce makes the isolation independent of the store's own behaviour. Tenant
// scoping is the isolation this system already guarantees, so the test uses it
// rather than truncating tables.
var (
	tenantSeq atomic.Uint64
	tenantRun = strconv.FormatInt(time.Now().UnixNano(), 36)
)

func uniqueTenant(t *testing.T) string {
	t.Helper()
	name := strings.Map(func(r rune) rune {
		if r == '/' || r == ' ' {
			return '-'
		}
		return r
	}, t.Name())
	return fmt.Sprintf("%s-%s-%d", name, tenantRun, tenantSeq.Add(1))
}

// runStoreContract is the one contract both run stores are held to. SQLite is
// the development and homelab store, Postgres the tuned target; the point of
// having two is lost the moment they disagree, so both implementations run
// exactly these assertions rather than a private copy each.
func runStoreContract(t *testing.T, s runstore.Store) {
	t.Helper()

	t.Run("ReplaysInSequenceOrderWithFieldsIntact", func(t *testing.T) {
		contractReplayOrder(t, s)
	})
	t.Run("AppendIsIdempotentOnDuplicateSequence", func(t *testing.T) {
		contractAppendIdempotent(t, s)
	})
	t.Run("ReplayIsScopedToTenant", func(t *testing.T) {
		contractTenantScope(t, s)
	})
	t.Run("QueryWithoutTenantIsRejected", func(t *testing.T) {
		contractEmptyTenantRejected(t, s)
	})
	t.Run("LastSequenceOnEmptyTenantStartsAtZero", func(t *testing.T) {
		contractLastSequenceStartsAtZero(t, s)
	})
	t.Run("WithTxCommitsEverythingOrNothing", func(t *testing.T) {
		contractWithTxIsAtomic(t, s)
	})
	t.Run("WithTxRejectsAnUnscopedTenant", func(t *testing.T) {
		contractWithTxEmptyTenantRejected(t, s)
	})
}

// contractWithTxIsAtomic is what the outbox is built on. A run event and the
// intent to publish it must commit as one act: if a caller can leave one of
// them behind, a crash between the two loses a step silently and no retry
// recovers what was never recorded (ADR 0005). Both stores are held to it,
// because a transaction that is real on Postgres and a no-op on SQLite is a
// guarantee that evaporates on a developer's machine.
func contractWithTxIsAtomic(t *testing.T, s runstore.Store) {
	ctx := context.Background()
	tenant := uniqueTenant(t)
	at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

	// A transaction that returns an error rolls back, and the error reaches
	// the caller unwrapped enough to be identified.
	sentinel := errors.New("caller aborted the transaction")
	err := s.WithTx(ctx, func(tx runstore.Tx) error {
		if err := tx.Append(ctx, tenant, runstore.Event{
			RunID: "run-1", Sequence: 1, Type: runstore.RunCreated, At: at,
		}); err != nil {
			return err
		}
		if err := tx.Append(ctx, tenant, runstore.Event{
			RunID: "run-1", Sequence: 2, Type: runstore.RunCompleted, At: at,
		}); err != nil {
			return err
		}
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)

	replayed, err := s.Replay(ctx, tenant, "run-1")
	require.NoError(t, err)
	require.Empty(t, replayed, "a rolled-back transaction must leave nothing behind")
	last, err := s.LastSequence(ctx, tenant)
	require.NoError(t, err)
	require.Equal(t, uint64(0), last)

	// A transaction that returns nil commits, all of it.
	require.NoError(t, s.WithTx(ctx, func(tx runstore.Tx) error {
		if err := tx.Append(ctx, tenant, runstore.Event{
			RunID: "run-1", Sequence: 1, Type: runstore.RunCreated, At: at,
		}); err != nil {
			return err
		}
		return tx.Append(ctx, tenant, runstore.Event{
			RunID: "run-1", Sequence: 2, Type: runstore.RunCompleted, At: at,
		})
	}))

	replayed, err = s.Replay(ctx, tenant, "run-1")
	require.NoError(t, err)
	require.Len(t, replayed, 2, "a committed transaction must leave all of its writes")
	require.Equal(t, runstore.RunCreated, replayed[0].Type)
	require.Equal(t, runstore.RunCompleted, replayed[1].Type)
}

// contractWithTxEmptyTenantRejected: the transactional path is not a way
// around the rule that there is no unscoped write in this system.
func contractWithTxEmptyTenantRejected(t *testing.T, s runstore.Store) {
	ctx := context.Background()
	err := s.WithTx(ctx, func(tx runstore.Tx) error {
		return tx.Append(ctx, "", runstore.Event{RunID: "run-1", Sequence: 1, Type: runstore.RunCreated})
	})
	require.ErrorIs(t, err, runstore.ErrTenantRequired)
}

// contractReplayOrder is the reason the run store exists. A run is not a
// goroutine: its position lives in a persisted event log, so the log must come
// back in the order it must be applied, with every field intact.
func contractReplayOrder(t *testing.T, s runstore.Store) {
	ctx := context.Background()
	tenant := uniqueTenant(t)
	at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

	events := []runstore.Event{
		{RunID: "run-1", Sequence: 1, Type: runstore.RunCreated, Payload: []byte(`{"n":1}`), At: at},
		{RunID: "run-1", StepID: "build", Attempt: 1, Sequence: 2, Type: runstore.StepReady, At: at.Add(time.Second)},
		{RunID: "run-1", StepID: "build", Attempt: 1, Sequence: 3, Type: runstore.StepDispatched, At: at.Add(2 * time.Second)},
		{RunID: "run-1", StepID: "build", Attempt: 1, Sequence: 4, Type: runstore.StepSucceeded, At: at.Add(3 * time.Second)},
		{RunID: "run-1", Sequence: 5, Type: runstore.RunCompleted, At: at.Add(4 * time.Second)},
	}
	// Appended out of order on purpose: the log's order is the sequence
	// column, not the order the scheduler happened to write the rows in.
	for _, i := range []int{4, 0, 2, 1, 3} {
		require.NoError(t, s.Append(ctx, tenant, events[i]))
	}

	replayed, err := s.Replay(ctx, tenant, "run-1")
	require.NoError(t, err)
	require.Len(t, replayed, 5)

	for i, want := range events {
		require.Equal(t, want.Sequence, replayed[i].Sequence, "events must replay in sequence order")
		require.Equal(t, want.Type, replayed[i].Type)
		require.Equal(t, want.RunID, replayed[i].RunID)
		require.Equal(t, want.StepID, replayed[i].StepID)
		require.Equal(t, want.Attempt, replayed[i].Attempt)
		require.True(t, want.At.Equal(replayed[i].At), "event %d: stored %s, got %s", i, want.At, replayed[i].At)
	}
	require.Equal(t, []byte(`{"n":1}`), replayed[0].Payload)

	last, err := s.LastSequence(ctx, tenant)
	require.NoError(t, err)
	require.Equal(t, uint64(5), last)
}

// contractAppendIdempotent catches a redelivered append duplicating history.
// The scheduler resumes from LastSequence after a restart and may re-append
// what it already wrote; that must be a no-op, not an error and not a second
// copy.
func contractAppendIdempotent(t *testing.T, s runstore.Store) {
	ctx := context.Background()
	tenant := uniqueTenant(t)

	e := runstore.Event{
		RunID: "run-1", StepID: "build", Attempt: 2, Sequence: 7,
		Type: runstore.StepFailed, Payload: []byte("boom"),
		At: time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC),
	}
	require.NoError(t, s.Append(ctx, tenant, e))
	require.NoError(t, s.Append(ctx, tenant, e))

	replayed, err := s.Replay(ctx, tenant, "run-1")
	require.NoError(t, err)
	require.Len(t, replayed, 1)
	require.Equal(t, []byte("boom"), replayed[0].Payload)

	// A redelivery must not overwrite what is already there either: the log
	// is append-only, so the stored row wins over the second copy.
	second := e
	second.Payload = []byte("different")
	require.NoError(t, s.Append(ctx, tenant, second))
	replayed, err = s.Replay(ctx, tenant, "run-1")
	require.NoError(t, err)
	require.Len(t, replayed, 1)
	require.Equal(t, []byte("boom"), replayed[0].Payload, "an append-only log never rewrites a stored event")
}

// contractTenantScope catches a query that forgets its tenant filter: one
// tenant's run must never surface another's events, even when both use the
// same run id.
func contractTenantScope(t *testing.T, s runstore.Store) {
	ctx := context.Background()
	tenantA := uniqueTenant(t)
	tenantB := uniqueTenant(t)
	at := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)

	require.NoError(t, s.Append(ctx, tenantA, runstore.Event{RunID: "run-1", Sequence: 1, Type: runstore.RunCreated, At: at}))
	require.NoError(t, s.Append(ctx, tenantA, runstore.Event{RunID: "run-1", Sequence: 2, Type: runstore.RunCompleted, At: at}))
	require.NoError(t, s.Append(ctx, tenantB, runstore.Event{RunID: "run-1", Sequence: 1, Type: runstore.RunCreated, At: at}))

	replayed, err := s.Replay(ctx, tenantB, "run-1")
	require.NoError(t, err)
	require.Len(t, replayed, 1, "replay must not return another tenant's events")
	require.Equal(t, uint64(1), replayed[0].Sequence)

	last, err := s.LastSequence(ctx, tenantB)
	require.NoError(t, err)
	require.Equal(t, uint64(1), last, "last sequence must not count another tenant's events")
}

// contractEmptyTenantRejected holds the constraint that there is no unscoped
// query in this system, even while only one tenant exists. An empty tenant is
// a bug in the caller, never a wildcard.
func contractEmptyTenantRejected(t *testing.T, s runstore.Store) {
	ctx := context.Background()

	_, err := s.Replay(ctx, "", "run-1")
	require.ErrorIs(t, err, runstore.ErrTenantRequired)
	require.ErrorContains(t, err, "tenant scope required")

	_, err = s.LastSequence(ctx, "")
	require.ErrorIs(t, err, runstore.ErrTenantRequired)
	require.ErrorContains(t, err, "tenant scope required")

	err = s.Append(ctx, "", runstore.Event{RunID: "run-1", Sequence: 1, Type: runstore.RunCreated})
	require.ErrorIs(t, err, runstore.ErrTenantRequired)
	require.ErrorContains(t, err, "tenant scope required")

	// The rejected append must not have reached the table under some empty
	// or defaulted scope.
	replayed, err := s.Replay(ctx, "-", "run-1")
	require.NoError(t, err)
	require.Empty(t, replayed)
}

// contractLastSequenceStartsAtZero: a scheduler resuming a tenant that has
// never run must get 0, not an error and not a missing row.
func contractLastSequenceStartsAtZero(t *testing.T, s runstore.Store) {
	ctx := context.Background()

	last, err := s.LastSequence(ctx, uniqueTenant(t))
	require.NoError(t, err)
	require.Equal(t, uint64(0), last)

	replayed, err := s.Replay(ctx, uniqueTenant(t), "never-ran")
	require.NoError(t, err)
	require.Empty(t, replayed)
}
