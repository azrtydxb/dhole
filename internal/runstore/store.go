// Package runstore is the durable source of truth for run state: an
// append-only, tenant-scoped event log. A run is a state machine driven by
// this log rather than a goroutine holding its position in a call stack, so a
// control-plane restart is a replay and not a loss (ADR 0003). The bus is only
// transport; nothing is considered to have happened until it is in this store
// (ADR 0005).
package runstore

import (
	"context"
	"errors"
	"time"
)

// ErrTenantRequired is returned by every method given an empty tenant. There
// is no unscoped query in this system, even while only one tenant exists: an
// empty tenant is a bug in the caller, never a wildcard.
var ErrTenantRequired = errors.New("tenant scope required")

// EventType names a transition in a run's life. The values are stored
// verbatim, so they are a persistence contract: add new ones, never rename an
// existing one.
type EventType string

// The transitions a run and its steps go through.
const (
	RunCreated     EventType = "RUN_CREATED"
	StepReady      EventType = "STEP_READY"
	StepDispatched EventType = "STEP_DISPATCHED"
	StepSucceeded  EventType = "STEP_SUCCEEDED"
	StepFailed     EventType = "STEP_FAILED"
	RunCompleted   EventType = "RUN_COMPLETED"
)

// Event is one immutable entry in the log. Sequence is per-tenant and
// monotonic; it is what the scheduler resumes from. Payload carries the
// type-specific detail, opaque to the store.
type Event struct {
	RunID    string
	StepID   string
	Attempt  uint32
	Sequence uint64
	Type     EventType
	Payload  []byte
	At       time.Time
}

// Store is the run event log. Implementations are safe for concurrent use.
//
// Every method takes an explicit tenant and rejects an empty one with
// ErrTenantRequired.
type Store interface {
	// Append records an event. It is idempotent on
	// (RunID, StepID, Attempt, Sequence): re-appending an event the store
	// already holds is a no-op returning nil, so a redelivered command
	// cannot duplicate history.
	Append(ctx context.Context, tenantID string, e Event) error

	// Replay returns every event of one run in ascending sequence order.
	Replay(ctx context.Context, tenantID, runID string) ([]Event, error)

	// LastSequence returns the highest sequence written for the tenant, or 0
	// if the tenant has no events yet.
	LastSequence(ctx context.Context, tenantID string) (uint64, error)

	// WithTx runs fn inside one database transaction and commits only if fn
	// returns nil, rolling back and returning fn's error otherwise.
	//
	// This is what the outbox is built on. A run event and the intent to
	// publish it have to commit as ONE act: committed separately, a crash
	// between them loses the step with no trace, and no retry recovers what
	// was never recorded (ADR 0005). The transaction is the caller's, so
	// anything else that must agree with the event — the outbox row above
	// all — is written through the same Tx.
	WithTx(ctx context.Context, fn func(Tx) error) error

	// Close releases the store's resources.
	Close() error
}

// Dialect names the SQL a Tx speaks. It exists because a caller writing its
// own statements through Tx.Exec has to choose a placeholder style: SQLite
// binds with `?` and Postgres with `$1`.
type Dialect string

// The dialects the two store implementations speak.
const (
	DialectSQLite   Dialect = "sqlite"
	DialectPostgres Dialect = "postgres"
)

// Tx is one open transaction. It is NOT safe for concurrent use: a
// transaction holds a single connection, and two goroutines using it at once
// interleave on the wire.
//
// Alongside Append it carries a deliberate escape hatch — Exec, Query and
// Dialect — for the tables that must commit atomically WITH a run event but
// are not the event log itself. The outbox is the reason it exists; without
// it the outbox would have to write on its own connection, which is precisely
// the two-act commit this design refuses.
type Tx interface {
	// Append records an event in this transaction, with the same
	// idempotence and tenant rules as Store.Append.
	Append(ctx context.Context, tenantID string, e Event) error

	// Exec runs a statement in this transaction.
	Exec(ctx context.Context, query string, args ...any) error

	// Query runs a query in this transaction. The caller must close the
	// returned Rows before issuing anything else on the same Tx.
	Query(ctx context.Context, query string, args ...any) (Rows, error)

	// Dialect says which placeholder style and types query must use.
	Dialect() Dialect
}

// Rows is a result set from Tx.Query, narrowed to what both drivers offer.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}
