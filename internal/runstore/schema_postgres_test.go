package runstore_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// TestPostgresSchemaHasEveryTable checks the whole schema exists on Postgres,
// not just the tables this package's own tests touch.
//
// This is the test that was missing when four tables — blob_refs, principals,
// tokens and cache_entries — reached SQLite and silently never reached
// Postgres. Every SQLite test passed, and the Postgres contract test passed
// too because it only exercises run_events, so nothing failed until a Postgres
// deployment hit a missing table at runtime. Add new tables to this list.
func TestPostgresSchemaHasEveryTable(t *testing.T) {
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	s, err := runstore.NewPostgres(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(ctx) })

	for _, table := range []string{
		"run_events", "blob_refs", "principals", "tokens",
		"pipelines", "revisions", "catalog_entries", "cache_entries",
		"policy_audit", "artifact_signatures", "plugin_upstreams", "plugin_mirrors",
		"tenants", "run_timers", "trigger_schedules", "open_runs",
		"run_sequences", "llm_calls", "quotas", "usage_records",
		"pipeline_heads",
	} {
		var exists bool
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			 WHERE table_schema='public' AND table_name=$1)`, table).Scan(&exists))
		require.True(t, exists, "table %s is missing from the Postgres schema", table)
	}
}

// TestPostgresRefusesASecondTerminalEvent runs the double-completion rule
// against the dialect the deployment that hit it was on. The SQLite tests
// prove the migration's intent; only this proves Postgres took the partial
// unique index and that `ON CONFLICT DO NOTHING` covers it there too.
func TestPostgresRefusesASecondTerminalEvent(t *testing.T) {
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	store, err := runstore.NewPostgres(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	tenant := "terminal-" + time.Now().UTC().Format("20060102150405.000000000")
	require.NoError(t, store.Append(ctx, tenant, runstore.Event{
		RunID: "run-a", Type: runstore.RunCreated, At: time.Now().UTC(),
	}))
	for range 2 {
		require.NoError(t, store.Append(ctx, tenant, runstore.Event{
			RunID: "run-a", Type: runstore.RunCompleted, At: time.Now().UTC(),
		}))
	}

	events, err := store.Replay(ctx, tenant, "run-a")
	require.NoError(t, err)
	require.Len(t, terminalEvents(events), 1)
}

// TestPostgresRefusesASecondStepVerdict is migration 0021 against the dialect
// the deployment runs. The SQLite tests prove the migration's intent; only
// this proves Postgres took both partial unique indexes and that
// `ON CONFLICT DO NOTHING` covers them there too — a plain .sql file reaches
// both runners, so a construct only SQLite accepts would leave the deployment
// with the bug and every local test passing.
func TestPostgresRefusesASecondStepVerdict(t *testing.T) {
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set")
	}
	ctx := context.Background()
	store, err := runstore.NewPostgres(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	tenant := "verdict-" + time.Now().UTC().Format("20060102150405.000000000")
	require.NoError(t, store.Append(ctx, tenant, runstore.Event{
		RunID: "run-a", Type: runstore.RunCreated, At: time.Now().UTC(),
	}))
	for _, verdict := range []runstore.EventType{awaitingReplay, policyDenied} {
		for range 2 {
			require.NoError(t, store.Append(ctx, tenant, runstore.Event{
				RunID: "run-a", StepID: "a", Attempt: 1,
				Type: verdict, At: time.Now().UTC(),
			}))
		}
	}

	events, err := store.Replay(ctx, tenant, "run-a")
	require.NoError(t, err)
	require.Len(t, eventsOfType(events, awaitingReplay), 1)
	require.Len(t, eventsOfType(events, policyDenied), 1)

	// And the index is per step, not per run, on this dialect too.
	require.NoError(t, store.Append(ctx, tenant, runstore.Event{
		RunID: "run-a", StepID: "b", Attempt: 1,
		Type: awaitingReplay, At: time.Now().UTC(),
	}))
	events, err = store.Replay(ctx, tenant, "run-a")
	require.NoError(t, err)
	require.Len(t, eventsOfType(events, awaitingReplay), 2)
}
