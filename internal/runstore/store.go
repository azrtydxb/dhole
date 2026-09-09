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
	// RunFailed closes a run whose step used up its attempts. It lives here
	// beside RunCompleted because the store has to know both: they are the two
	// events that take a run out of the open-run index, and an index that knew
	// only one of them would offer every failed run for advancement forever.
	RunFailed EventType = "RUN_FAILED"
)

// closesRun reports whether an event ends a run's life. It is the store's own
// rule rather than the scheduler's, because the open-run index is maintained
// on the append path: a writer that forgot to tell the store would leave a
// finished run in the index or an open one out of it.
func closesRun(t EventType) bool {
	return t == RunCompleted || t == RunFailed
}

// Event is one immutable entry in the log.
//
// Sequence is the event's position in its TENANT's log, and it is a total
// order: the store hands out each number once, inside the transaction that
// writes the event, so two runs of one tenant can never share one. Leave it
// ZERO and the store allocates the next position — which is what every writer
// should do. Setting it explicitly is for replaying a log whose order is
// already decided, and a caller that computes its own next position
// reintroduces the collision this design removed.
//
// Payload carries the type-specific detail, opaque to the store.
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
	// Append records an event, allocating its Sequence when it is zero. It
	// is idempotent on (RunID, StepID, Attempt, Sequence): re-appending an
	// event the store already holds — sequence included — is a no-op
	// returning nil, so a replayed command cannot duplicate history.
	Append(ctx context.Context, tenantID string, e Event) error

	// Replay returns every event of one run in ascending sequence order.
	Replay(ctx context.Context, tenantID, runID string) ([]Event, error)

	// LastSequence returns the highest sequence written for the tenant, or 0
	// if the tenant has no events yet.
	LastSequence(ctx context.Context, tenantID string) (uint64, error)

	// OpenRuns lists the tenant's unfinished runs, oldest first: every run
	// that has been created and has neither completed nor failed.
	//
	// This is what makes ADR 0003's claim true. A restart is a replay, and a
	// replay needs a run id — so without this the set of runs still to advance
	// exists only in the memory of the process that submitted them, and a
	// plane that restarts silently abandons every run that was merely waiting
	// rather than in flight.
	OpenRuns(ctx context.Context, tenantID string) ([]string, error)

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
