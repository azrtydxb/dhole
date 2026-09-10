package runstore_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// TestARunLogHoldsAtMostOneTerminalEvent is the store's half of the
// double-completion bug: a run whose log ended with RUN_COMPLETED twice.
//
// The scheduler decided the run was over by check-then-act — replay, see no
// terminal event, append one — and two advances of the same run replayed
// before either appended. Nothing downstream caught it, because the allocator
// gives each append its own sequence: run_events' primary key does not
// collide, so both rows were stored and both appends reported success.
//
// The store is where that has to be refused, because it is the only place the
// decision and the write are one act.
func TestARunLogHoldsAtMostOneTerminalEvent(t *testing.T) {
	ctx := context.Background()
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "terminal.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	const tenant = "acme"
	require.NoError(t, store.Append(ctx, tenant, runstore.Event{
		RunID: "run-a", Type: runstore.RunCreated, At: time.Now().UTC(),
	}))

	for range 2 {
		require.NoError(t, store.Append(ctx, tenant, runstore.Event{
			RunID: "run-a", Type: runstore.RunCompleted, At: time.Now().UTC(),
		}), "a loser learns nothing it needs to know: the run is over either way")
	}

	events, err := store.Replay(ctx, tenant, "run-a")
	require.NoError(t, err)
	require.Len(t, terminalEvents(events), 1,
		"a consumer treats the terminal event as the end of the run, "+
			"and a second one ends a stream that has already been closed")
}

// TestARunCannotBothCompleteAndFail is the same rule for two DIFFERENT
// verdicts. It is the worse shape of the bug: fail() and complete() are the
// two answers to the only question anyone asks about a run, and a log carrying
// both answers cannot be read at all.
func TestARunCannotBothCompleteAndFail(t *testing.T) {
	ctx := context.Background()
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "terminal.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	const tenant = "acme"
	require.NoError(t, store.Append(ctx, tenant, runstore.Event{
		RunID: "run-a", Type: runstore.RunCreated, At: time.Now().UTC(),
	}))
	for _, typ := range []runstore.EventType{
		runstore.RunCompleted, runstore.RunFailed, runstore.RunCancelled,
	} {
		require.NoError(t, store.Append(ctx, tenant, runstore.Event{
			RunID: "run-a", Type: typ, At: time.Now().UTC(),
		}))
	}

	events, err := store.Replay(ctx, tenant, "run-a")
	require.NoError(t, err)
	terminal := terminalEvents(events)
	require.Len(t, terminal, 1)
	require.Equal(t, runstore.RunCompleted, terminal[0].Type,
		"the first verdict is the one every reader has already acted on")
}

// TestConcurrentAppendsOfATerminalEventStoreOne is the race itself, with the
// store's own serialisation as the only thing holding it: no barrier, no
// ordering, both writers arriving at once.
func TestConcurrentAppendsOfATerminalEventStoreOne(t *testing.T) {
	ctx := context.Background()
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "terminal.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	const tenant = "acme"
	require.NoError(t, store.Append(ctx, tenant, runstore.Event{
		RunID: "run-a", Type: runstore.RunCreated, At: time.Now().UTC(),
	}))

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs[i] = store.Append(ctx, tenant, runstore.Event{
				RunID: "run-a", Type: runstore.RunCompleted, At: time.Now().UTC(),
			})
		}()
	}
	close(start)
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	events, err := store.Replay(ctx, tenant, "run-a")
	require.NoError(t, err)
	require.Len(t, terminalEvents(events), 1)
}

// TestTheOpenRunIndexStillClosesOnADuplicateTerminalEvent guards the
// consequence of refusing the second write on the append path: the index entry
// is deleted only when the LOG moved, so a run closed twice must not come back
// to the advance loop.
func TestTheOpenRunIndexStillClosesOnADuplicateTerminalEvent(t *testing.T) {
	ctx := context.Background()
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "terminal.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	const tenant = "acme"
	require.NoError(t, store.Append(ctx, tenant, runstore.Event{
		RunID: "run-a", Type: runstore.RunCreated, At: time.Now().UTC(),
	}))
	for range 2 {
		require.NoError(t, store.Append(ctx, tenant, runstore.Event{
			RunID: "run-a", Type: runstore.RunCompleted, At: time.Now().UTC(),
		}))
	}

	open, err := store.OpenRuns(ctx, tenant)
	require.NoError(t, err)
	require.Empty(t, open)
}

// TestOpeningADatabaseThatAlreadyHoldsTwoTerminalEventsRepairsIt covers the
// databases the bug already reached. The unique index cannot be created over a
// duplicate, so a deployment that had produced one would have failed to open
// from the moment the fix shipped — every start-up, not just once.
func TestOpeningADatabaseThatAlreadyHoldsTwoTerminalEventsRepairsIt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "corrupt.db")

	// The corruption, made the way the race made it: two terminal events for
	// one run, each under its own sequence. The index is dropped first because
	// it is what now makes that impossible.
	db, err := runstore.OpenSQLite(path)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `DROP INDEX run_events_one_terminal`)
	require.NoError(t, err)
	for _, sequence := range []int{2, 3} {
		_, err = db.ExecContext(ctx, `INSERT INTO run_events
			(tenant_id, run_id, step_id, attempt, sequence, type, payload, at)
			VALUES ('acme', 'run-a', '', 0, ?, 'RUN_COMPLETED', NULL, ?)`,
			sequence, time.Now().UTC().Format(runstore.TimeFormat))
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	store, err := runstore.NewSQLite(path)
	require.NoError(t, err, "a database the bug already reached must still open")
	t.Cleanup(func() { _ = store.Close() })

	events, err := store.Replay(ctx, "acme", "run-a")
	require.NoError(t, err)
	terminal := terminalEvents(events)
	require.Len(t, terminal, 1)
	require.Equal(t, uint64(2), terminal[0].Sequence,
		"the earliest terminal event survives: it is the one readers acted on")
}

// terminalEvents is the subset of a run's log that ends it.
func terminalEvents(events []runstore.Event) []runstore.Event {
	var out []runstore.Event
	for _, e := range events {
		switch e.Type {
		case runstore.RunCompleted, runstore.RunFailed, runstore.RunCancelled:
			out = append(out, e)
		}
	}
	return out
}
