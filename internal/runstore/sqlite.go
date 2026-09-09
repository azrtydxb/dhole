package runstore

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"net/url"
	"sort"
	"strings"
	"time"

	// modernc.org/sqlite is the pure-Go driver: no cgo, so the single binary
	// still cross-compiles to every platform the engines target.
	_ "modernc.org/sqlite"
)

// The schema lives in .sql files rather than in string literals scattered
// through the code, and is embedded so a binary carries its own migrations.
//
//go:embed migrations/*.sql
var migrations embed.FS

// postgresSuffix marks a migration as Postgres-only. Both dialects are
// embedded in one tree and one runner would otherwise feed BYTEA and
// TIMESTAMPTZ to SQLite, so the filename is the switch: SQLite applies every
// migration except these, Postgres applies only these. A migration that needs
// to differ per dialect is written twice under the same number, once plain and
// once with this suffix.
const postgresSuffix = ".postgres.sql"

// dialectMigrations lists the embedded migrations one dialect must apply, in
// filename order — which is migration-number order, as the plan's numbering
// ledger requires.
func dialectMigrations(postgres bool) ([]string, error) {
	names, err := fs.Glob(migrations, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	out := make([]string, 0, len(names))
	for _, name := range names {
		if strings.HasSuffix(name, postgresSuffix) == postgres {
			out = append(out, name)
		}
	}
	return out, nil
}

// TimeFormat is how a timestamp is stored under SQLite. SQLite has no time
// type; RFC3339 with nanoseconds in UTC sorts lexicographically in the same
// order it sorts chronologically. It is exported because anything writing its
// own timestamp column through a Tx — the outbox — has to store it the same
// way or the ordering silently stops being chronological.
const TimeFormat = time.RFC3339Nano

// SQLiteStore is the development and homelab implementation of Store, backed
// by a single SQLite file. Postgres is the tuned target; this one has to keep
// working.
type SQLiteStore struct {
	db *sql.DB
}

// Compile-time proof that the SQLite implementation satisfies the interface
// the scheduler consumes.
var _ Store = (*SQLiteStore)(nil)

// NewSQLite opens (creating if needed) the SQLite run store at path and
// applies the embedded migrations.
func NewSQLite(path string) (Store, error) {
	// WAL keeps a reader from blocking the writer; the busy timeout absorbs
	// the brief contention that remains instead of failing the append.
	//
	// _txlock=immediate takes the write lock when a transaction BEGINS rather
	// than at its first write. SQLite has no SELECT ... FOR UPDATE, so this
	// database-wide write lock IS the outbox drainer's row claim: a second
	// control plane cannot read the same unsent rows and publish them a
	// second time, because it cannot enter the transaction at all until the
	// first one commits. Deferred locking would instead let both read, both
	// publish, and one fail at commit — after the duplicate had already gone
	// out.
	dsn := "file:" + url.PathEscape(path) +
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)" +
		"&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite run store: %w", err)
	}
	// One writer at a time: SQLite serialises writes anyway, and a single
	// connection turns lock contention into ordinary queueing.
	db.SetMaxOpenConns(1)

	store := &SQLiteStore{db: db}
	if err := store.migrate(context.Background()); err != nil {
		return nil, fmt.Errorf("migrate sqlite run store: %w", err)
	}
	return store, nil
}

// migrate applies every non-Postgres embedded migration in filename order. Each statement
// is idempotent, so re-applying them on reopen is a no-op.
func (s *SQLiteStore) migrate(ctx context.Context) error {
	names, err := dialectMigrations(false)
	if err != nil {
		return err
	}
	for _, name := range names {
		stmts, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, string(stmts)); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

// sqliteAppend is the one INSERT both the store and its transactions use. A
// second copy of it inside WithTx would be free to drift from this one, and a
// transactional append behaving differently from a plain one is exactly the
// kind of divergence the outbox cannot survive.
const sqliteAppend = `INSERT INTO run_events
	(tenant_id, run_id, step_id, attempt, sequence, type, payload, at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT DO NOTHING`

// sqliteAppendArgs binds one event to sqliteAppend.
func sqliteAppendArgs(tenantID string, e Event) []any {
	return []any{
		tenantID, e.RunID, e.StepID, e.Attempt, e.Sequence,
		string(e.Type), e.Payload, e.At.UTC().Format(TimeFormat),
	}
}

// Append records an event, ignoring a duplicate of one already stored.
func (s *SQLiteStore) Append(ctx context.Context, tenantID string, e Event) error {
	if tenantID == "" {
		return ErrTenantRequired
	}
	if _, err := s.db.ExecContext(ctx, sqliteAppend, sqliteAppendArgs(tenantID, e)...); err != nil {
		return fmt.Errorf("append run event: %w", err)
	}
	return nil
}

// WithTx runs fn in one transaction, committing only if it returns nil.
//
// fn's error is returned unwrapped, so a caller that aborts with its own
// sentinel can still identify it with errors.Is. The deferred rollback is a
// no-op after a successful commit and is what covers a panic inside fn: a
// transaction left open holds SQLite's write lock and stalls every writer
// behind it.
func (s *SQLiteStore) WithTx(ctx context.Context, fn func(Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := fn(&sqliteTx{tx: tx}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// sqliteTx is one open SQLite transaction handed to a WithTx callback.
type sqliteTx struct {
	tx *sql.Tx
}

var _ Tx = (*sqliteTx)(nil)

func (t *sqliteTx) Dialect() Dialect { return DialectSQLite }

func (t *sqliteTx) Append(ctx context.Context, tenantID string, e Event) error {
	if tenantID == "" {
		return ErrTenantRequired
	}
	if _, err := t.tx.ExecContext(ctx, sqliteAppend, sqliteAppendArgs(tenantID, e)...); err != nil {
		return fmt.Errorf("append run event: %w", err)
	}
	return nil
}

func (t *sqliteTx) Exec(ctx context.Context, query string, args ...any) error {
	if _, err := t.tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("exec in transaction: %w", err)
	}
	return nil
}

func (t *sqliteTx) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query in transaction: %w", err)
	}
	// *sql.Rows already has exactly the Rows shape.
	return rows, nil
}

// Replay returns the run's events in the order they must be applied.
func (s *SQLiteStore) Replay(ctx context.Context, tenantID, runID string) ([]Event, error) {
	if tenantID == "" {
		return nil, ErrTenantRequired
	}
	const q = `SELECT run_id, step_id, attempt, sequence, type, payload, at
		FROM run_events
		WHERE tenant_id = ? AND run_id = ?
		ORDER BY sequence ASC`
	rows, err := s.db.QueryContext(ctx, q, tenantID, runID)
	if err != nil {
		return nil, fmt.Errorf("replay run: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var events []Event
	for rows.Next() {
		var (
			e     Event
			typ   string
			at    string
			given []byte
		)
		if err := rows.Scan(&e.RunID, &e.StepID, &e.Attempt, &e.Sequence, &typ, &given, &at); err != nil {
			return nil, fmt.Errorf("replay run: %w", err)
		}
		e.Type = EventType(typ)
		e.Payload = given
		if e.At, err = time.Parse(TimeFormat, at); err != nil {
			return nil, fmt.Errorf("replay run: parse event time: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("replay run: %w", err)
	}
	return events, nil
}

// LastSequence is where the scheduler resumes after a restart.
func (s *SQLiteStore) LastSequence(ctx context.Context, tenantID string) (uint64, error) {
	if tenantID == "" {
		return 0, ErrTenantRequired
	}
	const q = `SELECT COALESCE(MAX(sequence), 0) FROM run_events WHERE tenant_id = ?`
	var last uint64
	if err := s.db.QueryRowContext(ctx, q, tenantID).Scan(&last); err != nil {
		return 0, fmt.Errorf("last sequence: %w", err)
	}
	return last, nil
}

// Close releases the underlying database handle.
func (s *SQLiteStore) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close sqlite run store: %w", err)
	}
	return nil
}
