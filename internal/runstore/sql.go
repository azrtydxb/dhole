package runstore

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	// The pgx database/sql driver, registered as "pgx". The run store itself
	// talks to Postgres through pgxpool, but every other table in this schema
	// — definitions, the catalog, cache entries, the policy audit trail, the
	// blob reference index — is read and written through database/sql, and
	// those packages must reach Postgres through the same door they reach
	// SQLite through.
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Rebind rewrites a statement written with `?` placeholders into the form this
// dialect binds with. SQLite (and anything else) gets the query back
// unchanged; Postgres gets `$1, $2, ...` numbered in order of appearance,
// because pgx rejects `?` outright with a syntax error.
//
// One rewriter in one place is deliberate. The alternative — a second copy of
// every statement per dialect — doubles the SQL in five packages and lets the
// two copies drift silently, which is a worse failure than the one this fixes:
// a drifted copy still runs. It is also why this is a plain string walk rather
// than a query builder: the statements here are hand-written and stay
// hand-written.
//
// The walk skips what is not a placeholder — text inside single-quoted
// literals, quoted identifiers, and comments — so a `?` a caller genuinely
// meant to store is stored, and a numbered placeholder never appears inside a
// string.
func (d Dialect) Rebind(query string) string {
	if d != DialectPostgres {
		return query
	}

	var out strings.Builder
	out.Grow(len(query) + 8)
	n := 0
	for i := 0; i < len(query); i++ {
		switch c := query[i]; c {
		case '?':
			n++
			out.WriteByte('$')
			out.WriteString(strconv.Itoa(n))
		case '\'', '"':
			// A literal or a quoted identifier: copy it whole, including a
			// doubled quote, which is how both dialects escape the delimiter.
			end := closingQuote(query, i, c)
			out.WriteString(query[i:end])
			i = end - 1
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				end := strings.IndexByte(query[i:], '\n')
				if end < 0 {
					out.WriteString(query[i:])
					return out.String()
				}
				out.WriteString(query[i : i+end])
				i += end - 1
				continue
			}
			out.WriteByte(c)
		case '/':
			if i+1 < len(query) && query[i+1] == '*' {
				end := strings.Index(query[i+2:], "*/")
				if end < 0 {
					out.WriteString(query[i:])
					return out.String()
				}
				out.WriteString(query[i : i+2+end+2])
				i += 2 + end + 2 - 1
				continue
			}
			out.WriteByte(c)
		default:
			out.WriteByte(c)
		}
	}
	return out.String()
}

// closingQuote returns the index just past the quoted run that starts at
// query[start], treating a doubled quote as an escaped one. An unterminated
// quote yields the end of the string: a malformed statement is the database's
// to reject, and rewriting it further would only obscure the error.
func closingQuote(query string, start int, quote byte) int {
	for i := start + 1; i < len(query); i++ {
		if query[i] != quote {
			continue
		}
		if i+1 < len(query) && query[i+1] == quote {
			i++ // An escaped delimiter, not the end.
			continue
		}
		return i + 1
	}
	return len(query)
}

// sqliteDSN is the connection string every handle on a SQLite dhole database
// opens with, so the run store and the packages sharing its file agree on the
// pragmas rather than each hand-rolling them.
//
// WAL keeps a reader from blocking the writer; the busy timeout absorbs the
// brief contention that remains. _txlock=immediate takes the write lock when a
// transaction BEGINS rather than at its first write: SQLite has no
// SELECT ... FOR UPDATE, so that lock is what serialises the outbox drainer's
// claim and the blob collector's sweep against a concurrent writer.
func sqliteDSN(path string) string {
	return "file:" + url.PathEscape(path) +
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)" +
		"&_txlock=immediate"
}

// OpenSQLite opens (creating if needed) a database/sql handle on the SQLite
// dhole database at path, with the embedded migrations applied.
//
// It is the door for everything that shares the run store's file — the
// definition store, the catalog, the cache, the policy audit trail and the
// blob reference index — and it exists so none of them has to hand-roll the
// DSN or reach for a SQLite driver of its own. Pair it with DialectSQLite.
func OpenSQLite(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	// One writer at a time: SQLite serialises writes anyway, so a single
	// connection turns lock contention into ordinary queueing.
	db.SetMaxOpenConns(1)

	if err := applySQLiteMigrations(context.Background(), db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// OpenPostgres opens a database/sql handle on the Postgres dhole database at
// dsn, with the embedded migrations applied. It is OpenSQLite's counterpart:
// the same tables, reached the same way, so a package written against one
// dialect is not written against only that one. Pair it with DialectPostgres.
func OpenPostgres(ctx context.Context, dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres database: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open postgres database: %w", err)
	}
	if err := applyPostgresMigrations(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

// applySQLiteMigrations applies every non-Postgres embedded migration in
// filename order. Each statement is idempotent, so re-applying them on reopen
// is a no-op.
func applySQLiteMigrations(ctx context.Context, db *sql.DB) error {
	names, err := dialectMigrations(false)
	if err != nil {
		return err
	}
	for _, name := range names {
		stmts, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, string(stmts)); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

// applyPostgresMigrations applies every embedded Postgres migration in
// filename order, serialised on the same advisory lock the pgxpool runner
// takes: several control-plane processes start at once, and
// `CREATE TABLE IF NOT EXISTS` racing against itself in Postgres can still
// fail with a duplicate-object error.
func applyPostgresMigrations(ctx context.Context, db *sql.DB) error {
	names, err := dialectMigrations(true)
	if err != nil {
		return err
	}

	// One session for the whole run: an advisory lock belongs to the session
	// that took it, so taking and releasing it on different pooled
	// connections would lock nothing.
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer func() { _ = conn.Close() }()

	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		// Best effort: the lock ends with the session either way.
		_, _ = conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", migrationLockKey)
	}()

	for _, name := range names {
		stmts, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		// No bind parameters, so pgx sends this through the simple protocol
		// and a file may hold several statements.
		if _, err := conn.ExecContext(ctx, string(stmts)); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}
