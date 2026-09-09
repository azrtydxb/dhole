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

// timeFormat is how an event timestamp is stored. SQLite has no time type;
// RFC3339 with nanoseconds in UTC sorts lexicographically in the same order it
// sorts chronologically.
const timeFormat = time.RFC3339Nano

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
	dsn := "file:" + url.PathEscape(path) +
		"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
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

// Append records an event, ignoring a duplicate of one already stored.
func (s *SQLiteStore) Append(ctx context.Context, tenantID string, e Event) error {
	if tenantID == "" {
		return ErrTenantRequired
	}
	const q = `INSERT INTO run_events
		(tenant_id, run_id, step_id, attempt, sequence, type, payload, at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT DO NOTHING`
	_, err := s.db.ExecContext(ctx, q,
		tenantID, e.RunID, e.StepID, e.Attempt, e.Sequence,
		string(e.Type), e.Payload, e.At.UTC().Format(timeFormat))
	if err != nil {
		return fmt.Errorf("append run event: %w", err)
	}
	return nil
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
		if e.At, err = time.Parse(timeFormat, at); err != nil {
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
