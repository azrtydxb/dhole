package runstore_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// TestRebindNumbersPlaceholdersForPostgresOnly is the unit behind the
// dialect-agnostic stores. Five packages write their SQL once, with `?`, and
// this is the single place it becomes `$n` — so the cases it gets wrong are
// wrong everywhere at once.
func TestRebindNumbersPlaceholdersForPostgresOnly(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		want  string
	}{{
		name:  "numbers in order of appearance",
		query: `SELECT a FROM t WHERE tenant_id = ? AND id = ? ORDER BY at LIMIT ?`,
		want:  `SELECT a FROM t WHERE tenant_id = $1 AND id = $2 ORDER BY at LIMIT $3`,
	}, {
		// The definition store's insert has a literal '' between its
		// placeholders. Numbering a `?` inside a literal — or, worse, losing
		// count across one — silently binds the wrong column.
		name:  "skips single-quoted literals",
		query: `INSERT INTO t (a, b, c) VALUES (?, '', ?)`,
		want:  `INSERT INTO t (a, b, c) VALUES ($1, '', $2)`,
	}, {
		name:  "a question mark inside a literal is data, not a placeholder",
		query: `INSERT INTO t (a, b) VALUES (?, 'why? because')`,
		want:  `INSERT INTO t (a, b) VALUES ($1, 'why? because')`,
	}, {
		name:  "an escaped quote does not end the literal",
		query: `INSERT INTO t (a, b) VALUES (?, 'it''s ? here')`,
		want:  `INSERT INTO t (a, b) VALUES ($1, 'it''s ? here')`,
	}, {
		name:  "skips quoted identifiers",
		query: `SELECT "odd?name" FROM t WHERE id = ?`,
		want:  `SELECT "odd?name" FROM t WHERE id = $1`,
	}, {
		name:  "skips line comments",
		query: "SELECT a -- is this ? a placeholder\nFROM t WHERE id = ?",
		want:  "SELECT a -- is this ? a placeholder\nFROM t WHERE id = $1",
	}, {
		name:  "skips block comments",
		query: `SELECT a /* not ? one */ FROM t WHERE id = ?`,
		want:  `SELECT a /* not ? one */ FROM t WHERE id = $1`,
	}, {
		name:  "leaves a statement with no placeholders alone",
		query: `SELECT COUNT(*) FROM t`,
		want:  `SELECT COUNT(*) FROM t`,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, runstore.DialectPostgres.Rebind(tc.query))
			require.Equal(t, tc.query, runstore.DialectSQLite.Rebind(tc.query),
				"SQLite binds with `?` and must get its statement back untouched")
		})
	}
}
