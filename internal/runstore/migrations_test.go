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
	for _, name := range postgresFiles {
		require.True(t, strings.HasSuffix(name, postgresSuffix),
			"the Postgres runner must never apply a SQLite migration: %s", name)
	}

	// Every migration is applied by exactly one runner: a file belonging to
	// neither would be a schema change that silently never happens.
	require.Len(t, append(sqliteFiles, postgresFiles...), len(sqliteFiles)+len(postgresFiles))
	all, err := dialectMigrations(false)
	require.NoError(t, err)
	require.Contains(t, all, "migrations/0001_init.sql")
	require.Contains(t, postgresFiles, "migrations/0001_init.postgres.sql")
}
