package runstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// awaitingTimer is the event that arms a durable gate. It is spelled out
// rather than imported from internal/scheduler, which declares it, for the
// same reason the two verdicts above are: the store must not depend on the
// package that writes into it, and the STRING is the persistence contract
// either way — an index keyed on a constant the scheduler could rename would
// stop matching the rows in the log without anything failing to compile.
const awaitingTimer runstore.EventType = "STEP_AWAITING_TIMER"

// TestAStepArmsItsGateAtMostOnce is migration 0023, and it is migrations 0020
// and 0021 again on the event that arms a wait.
//
// Two advances of one run — the open-run tick and a status arriving from an
// engine — can both replay a log with no gate in it, both conclude the gate
// step is ready, and both arm it. The arming's own "is there a timer row
// already" check is taken inside its transaction, which serialises it on
// SQLite and does NOT on Postgres at read committed. Each append gets its own
// sequence, so run_events' primary key does not collide and both report
// success: the log then says the step began waiting twice, with two due times
// for one wait, in the one place a person looks to find out what a stopped run
// is stopped on.
func TestAStepArmsItsGateAtMostOnce(t *testing.T) {
	ctx := context.Background()
	store := openRunStore(t)

	require.NoError(t, store.Append(ctx, "acme", runstore.Event{
		RunID: "run-a", Type: runstore.RunCreated, At: time.Now().UTC(),
	}))
	for range 2 {
		require.NoError(t, store.Append(ctx, "acme", runstore.Event{
			RunID: "run-a", StepID: "hold",
			Type: awaitingTimer, At: time.Now().UTC(),
		}), "the loser of the race needs to learn nothing: the wait is armed either way")
	}

	require.Len(t, eventsOfType(replay(ctx, t, store, "acme", "run-a"), awaitingTimer), 1,
		"said twice, it reads as two waits where the run has one")
}

// TestEachGatedStepOfARunArmsItsOwn is the over-reach 0023 could easily have
// had. A fan-out may hold a gate on each of several branches, and that is
// several waits rather than one repeated; a rule written per run would hide
// every gate but the first, leaving branches that are demonstrably waiting
// with nothing in the log to say so.
func TestEachGatedStepOfARunArmsItsOwn(t *testing.T) {
	ctx := context.Background()
	store := openRunStore(t)

	require.NoError(t, store.Append(ctx, "acme", runstore.Event{
		RunID: "run-a", Type: runstore.RunCreated, At: time.Now().UTC(),
	}))
	for _, step := range []string{"hold-a", "hold-b"} {
		require.NoError(t, store.Append(ctx, "acme", runstore.Event{
			RunID: "run-a", StepID: step, Type: awaitingTimer, At: time.Now().UTC(),
		}))
	}

	require.Len(t, eventsOfType(replay(ctx, t, store, "acme", "run-a"), awaitingTimer), 2,
		"two branches that are both waiting are two waits")
}
