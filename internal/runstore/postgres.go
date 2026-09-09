package runstore

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresStore is the tuned deployment target's implementation of Store.
// SQLite is what a developer and a homelab run; this is what a cluster runs.
// Both are held to one contract, because a behaviour that differs between them
// is a behaviour a user discovers in production.
type PostgresStore struct {
	pool *pgxpool.Pool
}

// Compile-time proof that the Postgres implementation satisfies the same
// interface the scheduler consumes.
var _ Store = (*PostgresStore)(nil)

// migrationLockKey is an arbitrary but fixed advisory-lock key. Several
// control-plane processes start at once and each applies the migrations on
// open; `CREATE TABLE IF NOT EXISTS` racing against itself in Postgres can
// still fail with a duplicate-object error, so the runner serialises on this
// lock instead of hoping.
const migrationLockKey int64 = 0x64686f6c65727573 // "dholerus"

// NewPostgres connects to the Postgres run store at dsn and applies the
// embedded Postgres migrations.
func NewPostgres(ctx context.Context, dsn string) (Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres run store: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("open postgres run store: %w", err)
	}

	store := &PostgresStore{pool: pool}
	if err := store.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate postgres run store: %w", err)
	}
	return store, nil
}

// migrate applies every embedded Postgres migration in filename order. Each
// statement is idempotent, so re-applying them on every open is a no-op.
func (s *PostgresStore) migrate(ctx context.Context) error {
	names, err := dialectMigrations(true)
	if err != nil {
		return err
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		// Best effort: the session ends with the connection either way.
		_, _ = conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", migrationLockKey)
	}()

	for _, name := range names {
		stmts, err := migrations.ReadFile(name)
		if err != nil {
			return err
		}
		// No bind parameters, so pgx sends this through the simple protocol
		// and a file may hold several statements.
		if _, err := conn.Exec(ctx, string(stmts)); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

// Append records an event, ignoring a duplicate of one already stored. The log
// is append-only: a redelivered command finds its row already there and
// changes nothing, rather than duplicating or rewriting history.
func (s *PostgresStore) Append(ctx context.Context, tenantID string, e Event) error {
	if tenantID == "" {
		return ErrTenantRequired
	}
	// Postgres has no unsigned integer type, so the log's uint64 sequence is
	// stored in a BIGINT. A sequence past the signed range would wrap into a
	// negative number and silently reorder the log, so it is refused instead.
	if e.Sequence > math.MaxInt64 {
		return fmt.Errorf("append run event: sequence %d exceeds what a bigint can hold", e.Sequence)
	}
	const q = `INSERT INTO run_events
		(tenant_id, run_id, step_id, attempt, sequence, type, payload, at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT DO NOTHING`
	_, err := s.pool.Exec(ctx, q,
		tenantID, e.RunID, e.StepID, int64(e.Attempt), int64(e.Sequence),
		string(e.Type), e.Payload, e.At.UTC())
	if err != nil {
		return fmt.Errorf("append run event: %w", err)
	}
	return nil
}

// Replay returns the run's events in the order they must be applied.
func (s *PostgresStore) Replay(ctx context.Context, tenantID, runID string) ([]Event, error) {
	if tenantID == "" {
		return nil, ErrTenantRequired
	}
	const q = `SELECT run_id, step_id, attempt, sequence, type, payload, at
		FROM run_events
		WHERE tenant_id = $1 AND run_id = $2
		ORDER BY sequence ASC`
	rows, err := s.pool.Query(ctx, q, tenantID, runID)
	if err != nil {
		return nil, fmt.Errorf("replay run: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var (
			e        Event
			typ      string
			attempt  int64
			sequence int64
			at       time.Time
			payload  []byte
		)
		if err := rows.Scan(&e.RunID, &e.StepID, &attempt, &sequence, &typ, &payload, &at); err != nil {
			return nil, fmt.Errorf("replay run: %w", err)
		}
		e.Attempt = uint32(attempt)   //nolint:gosec // written from a uint32
		e.Sequence = uint64(sequence) //nolint:gosec // written from a uint64
		e.Type = EventType(typ)
		e.Payload = payload
		e.At = at.UTC()
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("replay run: %w", err)
	}
	return events, nil
}

// LastSequence is where the scheduler resumes after a restart.
func (s *PostgresStore) LastSequence(ctx context.Context, tenantID string) (uint64, error) {
	if tenantID == "" {
		return 0, ErrTenantRequired
	}
	const q = `SELECT COALESCE(MAX(sequence), 0) FROM run_events WHERE tenant_id = $1`
	var last int64
	if err := s.pool.QueryRow(ctx, q, tenantID).Scan(&last); err != nil {
		return 0, fmt.Errorf("last sequence: %w", err)
	}
	return uint64(last), nil //nolint:gosec // sequences are written from uint64
}

// Close releases the connection pool.
func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}
