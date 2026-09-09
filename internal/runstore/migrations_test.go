package runstore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMigrationsArePartitionedByDialect guards the filename convention that
// keeps the two schemas apart. Both dialects live in one embedded tree and
// both runners glob it, so the split is the only thing stopping SQLite from
// applying BYTEA and TIMESTAMPTZ.
//
// The failure this catches is quiet on the SQLite side: SQLite has type
// affinity rather than types and will happily create a table from the Postgres
// DDL, so the store keeps working while its schema is no longer the one
// 0001_init.sql describes. Nothing else in the suite notices.
func TestMigrationsArePartitionedByDialect(t *testing.T) {
	sqliteFiles, err := dialectMigrations(false)
	require.NoError(t, err)
	postgresFiles, err := dialectMigrations(true)
	require.NoError(t, err)

	require.NotEmpty(t, sqliteFiles)
	require.NotEmpty(t, postgresFiles)

	for _, name := range sqliteFiles {
		require.NotContains(t, name, postgresSuffix,
			"the SQLite runner must never apply a Postgres migration")
	}
	// Postgres does apply plain, dialect-neutral migrations — that is the
	// point of the convention. What it must never do is apply BOTH forms of
	// one number, which would run the same CREATE twice with two shapes.
	seen := map[string]string{}
	for _, name := range postgresFiles {
		number := migrationNumber(name)
		require.NotContains(t, seen, number,
			"Postgres applies two forms of migration %s: %s and %s", number, seen[number], name)
		seen[number] = name
	}

	// 0001 exists in both forms, so Postgres must take the suffixed one and
	// SQLite the plain one.
	require.Contains(t, sqliteFiles, "migrations/0001_init.sql")
	require.NotContains(t, sqliteFiles, "migrations/0001_init.postgres.sql")
	require.Contains(t, postgresFiles, "migrations/0001_init.postgres.sql")
	require.NotContains(t, postgresFiles, "migrations/0001_init.sql")

	// 0003 exists only in plain form, so BOTH runners must apply it.
	require.Contains(t, sqliteFiles, "migrations/0003_blob_refs.sql")
	require.Contains(t, postgresFiles, "migrations/0003_blob_refs.sql")
}

// TestEveryMigrationNumberReachesBothDialects is the check the partition test
// could not make. Partitioning by suffix keeps the two schemas apart, but on
// its own it also means a migration written once — plain, dialect-neutral DDL —
// reaches SQLite and never reaches Postgres at all.
//
// That failure is silent in the worst way: every SQLite test passes, the
// Postgres contract test passes because it only exercises run_events, and the
// missing table is discovered by a Postgres deployment at runtime.
func TestEveryMigrationNumberReachesBothDialects(t *testing.T) {
	sqliteFiles, err := dialectMigrations(false)
	require.NoError(t, err)
	postgresFiles, err := dialectMigrations(true)
	require.NoError(t, err)

	numbers := func(names []string) map[string]bool {
		out := map[string]bool{}
		for _, n := range names {
			base := strings.TrimPrefix(n, "migrations/")
			out[strings.SplitN(base, "_", 2)[0]] = true
		}
		return out
	}

	for number := range numbers(sqliteFiles) {
		require.True(t, numbers(postgresFiles)[number],
			"migration %s never runs on Postgres — a table that exists in "+
				"development and is missing in production", number)
	}
	for number := range numbers(postgresFiles) {
		require.True(t, numbers(sqliteFiles)[number],
			"migration %s never runs on SQLite", number)
	}
}
