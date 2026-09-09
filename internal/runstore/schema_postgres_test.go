package runstore_test

import (
	"context"
	"os"
	"testing"

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
		"run_sequences", "llm_calls",
	} {
		var exists bool
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			 WHERE table_schema='public' AND table_name=$1)`, table).Scan(&exists))
		require.True(t, exists, "table %s is missing from the Postgres schema", table)
	}
}
