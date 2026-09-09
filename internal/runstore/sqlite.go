package runstore

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
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

// postgresSuffix marks a migration as the Postgres form of its number.
//
// Most DDL here is dialect-neutral, so a plain .sql file is applied by BOTH
// runners. Where a statement cannot be — SQLite's BLOB is Postgres's BYTEA —
// the number is written twice, plain and with this suffix, and the suffixed
// file REPLACES the plain one for Postgres.
//
// The earlier rule was "Postgres applies only suffixed files", which meant a
// migration written once reached SQLite and silently never reached Postgres.
// Four tables were missing there before anything noticed, because every SQLite
// test passed and the Postgres contract test only exercised run_events.
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

	// A number with a Postgres form is overridden there; SQLite never sees one.
	overridden := make(map[string]bool, len(names))
	for _, name := range names {
		if strings.HasSuffix(name, postgresSuffix) {
			overridden[migrationNumber(name)] = true
		}
	}

	out := make([]string, 0, len(names))
	for _, name := range names {
		suffixed := strings.HasSuffix(name, postgresSuffix)
		switch {
		case !postgres && suffixed:
			continue // Postgres DDL never reaches SQLite.
		case postgres && !suffixed && overridden[migrationNumber(name)]:
			continue // Superseded by this number's Postgres form.
		}
		out = append(out, name)
	}
	return out, nil
}

// migrationNumber is the leading number a migration is ordered and paired by:
// "migrations/0005_definitions.postgres.sql" yields "0005".
func migrationNumber(name string) string {
	base := strings.TrimPrefix(name, "migrations/")
	number, _, _ := strings.Cut(base, "_")
	return number
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
	db, err := OpenSQLite(path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite run store: %w", err)
	}
	return &SQLiteStore{db: db}, nil
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

// sqliteOpenRun and sqliteCloseRun maintain the open-run index. They are
// written in the SAME transaction as the event that justifies them, so a crash
// can never leave a finished run offered for advancement forever or an open
// one invisible to a plane that restarted.
// sqliteNextSequence hands out the tenant's next log position, atomically.
//
// It is the counter's own successor OR the log's high-water mark, whichever is
// higher. The high-water mark is not only a seed for the first allocation: a
// caller may still supply an explicit sequence — replaying a log whose order
// is already decided does exactly that — and the allocator has to stay ahead
// of anything written that way, or it would hand out a number the log already
// holds.
const sqliteNextSequence = `INSERT INTO run_sequences (tenant_id, next)
	VALUES (?, COALESCE((SELECT MAX(sequence) FROM run_events WHERE tenant_id = ?), 0) + 1)
	ON CONFLICT (tenant_id) DO UPDATE SET next = MAX(
		run_sequences.next + 1,
		COALESCE((SELECT MAX(sequence) FROM run_events WHERE tenant_id = ?), 0) + 1)
	RETURNING next`

const (
	sqliteOpenRun = `INSERT INTO open_runs (tenant_id, run_id, created_at)
		VALUES (?, ?, ?) ON CONFLICT DO NOTHING`
	sqliteCloseRun = `DELETE FROM open_runs WHERE tenant_id = ? AND run_id = ?`
)

// Append records an event, ignoring a duplicate of one already stored.
//
// It runs in a transaction because it writes two things: the event, and the
// open-run index entry the event implies. Committed separately they could
// disagree, which is the same two-act commit the outbox exists to prevent.
func (s *SQLiteStore) Append(ctx context.Context, tenantID string, e Event) error {
	if tenantID == "" {
		return ErrTenantRequired
	}
	return s.WithTx(ctx, func(tx Tx) error { return tx.Append(ctx, tenantID, e) })
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
	if e.Sequence == 0 {
		if err := t.tx.QueryRowContext(ctx, sqliteNextSequence, tenantID, tenantID, tenantID).
			Scan(&e.Sequence); err != nil {
			return fmt.Errorf("append run event: allocating a sequence: %w", err)
		}
	}
	result, err := t.tx.ExecContext(ctx, sqliteAppend, sqliteAppendArgs(tenantID, e)...)
	if err != nil {
		return fmt.Errorf("append run event: %w", err)
	}
	// The index moves only when the LOG moved. A redelivered event finds its
	// row already there and writes nothing, so a replayed RUN_CREATED cannot
	// resurrect a run that has since finished.
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("append run event: %w", err)
	}
	if affected == 0 {
		return nil
	}
	switch {
	case e.Type == RunCreated:
		_, err = t.tx.ExecContext(ctx, sqliteOpenRun,
			tenantID, e.RunID, e.At.UTC().Format(TimeFormat))
	case closesRun(e.Type):
		_, err = t.tx.ExecContext(ctx, sqliteCloseRun, tenantID, e.RunID)
	}
	if err != nil {
		return fmt.Errorf("append run event: open-run index: %w", err)
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

// OpenRuns lists the tenant's unfinished runs, oldest first.
func (s *SQLiteStore) OpenRuns(ctx context.Context, tenantID string) ([]string, error) {
	if tenantID == "" {
		return nil, ErrTenantRequired
	}
	const q = `SELECT run_id FROM open_runs WHERE tenant_id = ?
		ORDER BY created_at ASC, run_id ASC`
	rows, err := s.db.QueryContext(ctx, q, tenantID)
	if err != nil {
		return nil, fmt.Errorf("open runs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var runs []string
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return nil, fmt.Errorf("open runs: %w", err)
		}
		runs = append(runs, runID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("open runs: %w", err)
	}
	return runs, nil
}

// Close releases the underlying database handle.
func (s *SQLiteStore) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close sqlite run store: %w", err)
	}
	return nil
}
