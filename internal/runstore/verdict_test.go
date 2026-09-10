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

// The two step verdicts a run may hold only once per step. They are spelled
// out rather than imported from internal/scheduler, which declares them: the
// store must not depend on the package that writes into it, and the STRING is
// the persistence contract either way — an index keyed on a constant the
// scheduler could rename would stop matching the rows in the log without
// anything failing to compile.
const (
	awaitingReplay runstore.EventType = "STEP_AWAITING_REPLAY"
	policyDenied   runstore.EventType = "STEP_POLICY_DENIED"
)

// TestAStepHoldsAtMostOneVerdictOfEachKind is migration 0020's rule one level
// down, and the store's half of it.
//
// The scheduler decides both of these by check-then-act on a replay taken
// outside the transaction it appends in: it records the standstill of a
// blocked step when the replay showed none, and the refusal of a denied step
// when the replay showed it ready. Two advances of one run that replay before
// either appends both write, each under its own sequence, so run_events'
// primary key does not collide and both appends report success.
//
// The store is where that has to be refused, because it is the only place the
// decision and the write are one act.
func TestAStepHoldsAtMostOneVerdictOfEachKind(t *testing.T) {
	for _, verdict := range []runstore.EventType{awaitingReplay, policyDenied} {
		t.Run(string(verdict), func(t *testing.T) {
			ctx := context.Background()
			store := openRunStore(t)

			require.NoError(t, store.Append(ctx, "acme", runstore.Event{
				RunID: "run-a", Type: runstore.RunCreated, At: time.Now().UTC(),
			}))
			for range 2 {
				require.NoError(t, store.Append(ctx, "acme", runstore.Event{
					RunID: "run-a", StepID: "a", Attempt: 1,
					Type: verdict, At: time.Now().UTC(),
				}), "the loser of the race needs to learn nothing: the fact is already in the log")
			}

			require.Len(t, eventsOfType(replay(ctx, t, store, "acme", "run-a"), verdict), 1,
				"said twice, it reads as two separate stoppages of one step")
		})
	}
}

// TestEachStepOfARunKeepsItsOwnVerdict is the over-reach the index could
// easily have had. A fan-out where two branches both stop is TWO facts, and a
// rule written per run rather than per step would silence the second one —
// leaving the operator to fix one branch and find the run still stuck, with
// nothing in the log saying why.
func TestEachStepOfARunKeepsItsOwnVerdict(t *testing.T) {
	ctx := context.Background()
	store := openRunStore(t)

	require.NoError(t, store.Append(ctx, "acme", runstore.Event{
		RunID: "run-a", Type: runstore.RunCreated, At: time.Now().UTC(),
	}))
	for _, step := range []string{"a", "b"} {
		require.NoError(t, store.Append(ctx, "acme", runstore.Event{
			RunID: "run-a", StepID: step, Attempt: 1,
			Type: awaitingReplay, At: time.Now().UTC(),
		}))
	}

	require.Len(t, eventsOfType(replay(ctx, t, store, "acme", "run-a"), awaitingReplay), 2,
		"two branches that both stopped are two things an operator has to act on")
}

// TestTheSameStepIdInTwoRunsKeepsBothVerdicts is the other half of that: the
// index is scoped by tenant and run as well as step, so the same step id in a
// second run of the same pipeline — which is every rerun — is a separate fact.
func TestTheSameStepIdInTwoRunsKeepsBothVerdicts(t *testing.T) {
	ctx := context.Background()
	store := openRunStore(t)

	for _, run := range []string{"run-a", "run-b"} {
		require.NoError(t, store.Append(ctx, "acme", runstore.Event{
			RunID: run, Type: runstore.RunCreated, At: time.Now().UTC(),
		}))
		require.NoError(t, store.Append(ctx, "acme", runstore.Event{
			RunID: run, StepID: "a", Attempt: 1,
			Type: policyDenied, At: time.Now().UTC(),
		}))
		require.Len(t, eventsOfType(replay(ctx, t, store, "acme", run), policyDenied), 1,
			"a rerun of the same pipeline is refused on its own account")
	}
}

// TestConcurrentAppendsOfOneStepVerdictStoreOne is the race itself, with the
// store's own serialisation as the only thing holding it: no barrier, no
// ordering, every writer arriving at once.
func TestConcurrentAppendsOfOneStepVerdictStoreOne(t *testing.T) {
	ctx := context.Background()
	store := openRunStore(t)

	require.NoError(t, store.Append(ctx, "acme", runstore.Event{
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
			errs[i] = store.Append(ctx, "acme", runstore.Event{
				RunID: "run-a", StepID: "a", Attempt: 1,
				Type: awaitingReplay, At: time.Now().UTC(),
			})
		}()
	}
	close(start)
	wg.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, eventsOfType(replay(ctx, t, store, "acme", "run-a"), awaitingReplay), 1)
}

// TestOpeningADatabaseThatAlreadyHoldsTwoStepVerdictsRepairsIt covers the
// databases the bug already reached. The unique index cannot be created over a
// duplicate, so a deployment that had produced one would have failed to open
// from the moment the fix shipped — every start-up, not just once.
func TestOpeningADatabaseThatAlreadyHoldsTwoStepVerdictsRepairsIt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "corrupt.db")

	// The corruption, made the way the race made it: two of one verdict for
	// one step, each under its own sequence. The indexes are dropped first
	// because they are what now makes that impossible.
	db, err := runstore.OpenSQLite(path)
	require.NoError(t, err)
	for _, index := range []string{"run_events_one_awaiting_replay", "run_events_one_policy_denied"} {
		_, err = db.ExecContext(ctx, `DROP INDEX `+index)
		require.NoError(t, err)
	}
	for _, row := range []struct {
		sequence int
		verdict  runstore.EventType
	}{
		{2, awaitingReplay}, {3, awaitingReplay}, {4, policyDenied}, {5, policyDenied},
	} {
		_, err = db.ExecContext(ctx, `INSERT INTO run_events
			(tenant_id, run_id, step_id, attempt, sequence, type, payload, at)
			VALUES ('acme', 'run-a', 'a', 1, ?, ?, NULL, ?)`,
			row.sequence, string(row.verdict), time.Now().UTC().Format(runstore.TimeFormat))
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	store, err := runstore.NewSQLite(path)
	require.NoError(t, err, "a database the bug already reached must still open")
	t.Cleanup(func() { _ = store.Close() })

	events := replay(ctx, t, store, "acme", "run-a")
	for _, kept := range []struct {
		verdict  runstore.EventType
		earliest uint64
	}{{awaitingReplay, 2}, {policyDenied, 4}} {
		survivors := eventsOfType(events, kept.verdict)
		require.Len(t, survivors, 1)
		require.Equal(t, kept.earliest, survivors[0].Sequence,
			"the earliest of each survives: it is the one readers were already served")
	}
}

// openRunStore is a fresh SQLite run store for one test, closed with it.
func openRunStore(t *testing.T) runstore.Store {
	t.Helper()
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "verdict.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func replay(ctx context.Context, t *testing.T, s runstore.Store, tenant, run string) []runstore.Event {
	t.Helper()
	events, err := s.Replay(ctx, tenant, run)
	require.NoError(t, err)
	return events
}

// eventsOfType is the subset of a run's log with one type.
func eventsOfType(events []runstore.Event, kind runstore.EventType) []runstore.Event {
	var out []runstore.Event
	for _, e := range events {
		if e.Type == kind {
			out = append(out, e)
		}
	}
	return out
}
