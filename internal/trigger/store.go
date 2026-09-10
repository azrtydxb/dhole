package trigger

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// ErrExists is returned when a tenant already has a trigger with this id.
//
// The id is the identity: a schedule's id is the key of its row in
// trigger_schedules, so two triggers sharing one would be two pipelines driven
// by a single due time — and the second would silently never fire.
var ErrExists = errors.New("trigger: a trigger with this id already exists")

// ErrNoSuchTrigger is returned when nothing this tenant owns has that id. It
// is an error rather than a silent success because a delete that reported
// success for a trigger that was never there lets a typo read as a trigger
// removed, which is the one mistake an operator cannot see.
var ErrNoSuchTrigger = errors.New("trigger: no such trigger")

// timeFormat matches the run store's: SQLite has no time type, and RFC3339
// with nanoseconds in UTC sorts lexicographically in chronological order.
const timeFormat = time.RFC3339Nano

// Spec is one stored event source: what the operator said, and nothing about
// what it is currently doing.
//
// It is deliberately the same shape the `--triggers` file has always
// described. Two vocabularies for one concept — one for the file, one for the
// table — would drift, and the plane would have to decide which of two
// meanings of "untrusted" it was honouring.
type Spec struct {
	// ID is the trigger's identity within the tenant.
	ID string
	// Kind is "schedule", "http" or "git".
	Kind string
	// PipelineID is the pipeline it drives. Its ACTIVE revision is resolved
	// when the trigger fires, never pinned here.
	PipelineID string
	// InputMapping maps each of the pipeline's declared inputs to a field of
	// this trigger's own event. See Binding.
	InputMapping map[string]string
	// Expression is a five- or six-field cron expression. Schedules only.
	Expression string
	// Secret is the shared secret the forge signs with. Git triggers only,
	// and required for them.
	Secret string
	// Untrusted marks every value this trigger produces as tainted
	// (ADR 0015).
	Untrusted bool
	// CreatedBy is the principal that created it, kept because a trigger
	// starts runs and "who put this here" is the first question after one
	// fires unexpectedly.
	CreatedBy string
	// CreatedAt is when. Set by the store on Create.
	CreatedAt time.Time
}

// Store is the durable table of triggers an operator created through the
// contract.
//
// It exists because the only way to configure a trigger was the plane's own
// `--triggers` file, read once at start-up — so creating one meant a shell on
// the control plane's host and a restart, which is a capability the GUI and an
// agent can never have (ADR 0013).
//
// Every method takes an explicit tenant and refuses an empty one with
// runstore.ErrTenantRequired. A trigger starts runs, so an unscoped one would
// be one tenant firing another's pipeline.
type Store interface {
	// Create stores a trigger, and returns ErrExists if the id is taken.
	Create(ctx context.Context, tenantID string, s Spec) error
	// List returns the tenant's triggers in id order, secrets included: the
	// plane cannot verify a forge's signature without the secret it signed
	// with. What the CONTRACT returns is a narrower thing.
	List(ctx context.Context, tenantID string) ([]Spec, error)
	// Delete removes one, and returns ErrNoSuchTrigger if it was not there.
	Delete(ctx context.Context, tenantID, triggerID string) error
}

// sqlStore is the trigger table over the same database as the run event log,
// exactly as the catalog is: one database to back up, one to restore
// consistently, and one migration runner that owns the schema.
type sqlStore struct {
	db      *sql.DB
	dialect runstore.Dialect
	now     func() time.Time
}

var _ Store = (*sqlStore)(nil)

// NewStore returns the trigger store over an already-open handle speaking
// dialect. The handle is the caller's; its schema is expected to carry the
// migrations the run store applies.
func NewStore(db *sql.DB, dialect runstore.Dialect) Store {
	return &sqlStore{db: db, dialect: dialect, now: time.Now}
}

func (s *sqlStore) Create(ctx context.Context, tenantID string, spec Spec) error {
	if tenantID == "" {
		return runstore.ErrTenantRequired
	}
	if spec.ID == "" {
		return errors.New("trigger: a trigger needs an id")
	}
	mapping, err := json.Marshal(nonNilMapping(spec.InputMapping))
	if err != nil {
		return fmt.Errorf("trigger: encoding the input mapping: %w", err)
	}

	// The insert is the check. Reading first and inserting after is two
	// statements with a window between them, and two planes in that window
	// both find the id free — so the primary key is what actually refuses.
	const q = `INSERT INTO triggers
		(tenant_id, trigger_id, kind, pipeline_id, input_mapping, expression,
		 secret, untrusted, created_at, created_by)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err = s.db.ExecContext(ctx, s.dialect.Rebind(q),
		tenantID, spec.ID, spec.Kind, spec.PipelineID, string(mapping), spec.Expression,
		spec.Secret, boolText(spec.Untrusted), s.now().UTC().Format(timeFormat), spec.CreatedBy)
	if err != nil {
		if isDuplicate(err) {
			return fmt.Errorf("%w: %q", ErrExists, spec.ID)
		}
		return fmt.Errorf("trigger: creating %q: %w", spec.ID, err)
	}
	return nil
}

func (s *sqlStore) List(ctx context.Context, tenantID string) ([]Spec, error) {
	if tenantID == "" {
		return nil, runstore.ErrTenantRequired
	}
	const q = `SELECT trigger_id, kind, pipeline_id, input_mapping, expression,
		secret, untrusted, created_at, created_by
		FROM triggers WHERE tenant_id = ? ORDER BY trigger_id`
	rows, err := s.db.QueryContext(ctx, s.dialect.Rebind(q), tenantID)
	if err != nil {
		return nil, fmt.Errorf("trigger: list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var specs []Spec
	for rows.Next() {
		var (
			spec      Spec
			mapping   string
			untrusted string
			createdAt string
		)
		if err := rows.Scan(&spec.ID, &spec.Kind, &spec.PipelineID, &mapping,
			&spec.Expression, &spec.Secret, &untrusted, &createdAt, &spec.CreatedBy); err != nil {
			return nil, fmt.Errorf("trigger: list: %w", err)
		}
		if err := json.Unmarshal([]byte(mapping), &spec.InputMapping); err != nil {
			return nil, fmt.Errorf("trigger: trigger %q has an unreadable input mapping: %w", spec.ID, err)
		}
		spec.Untrusted = untrusted == "1"
		if at, err := time.Parse(timeFormat, createdAt); err == nil {
			spec.CreatedAt = at
		}
		specs = append(specs, spec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("trigger: list: %w", err)
	}
	return specs, nil
}

func (s *sqlStore) Delete(ctx context.Context, tenantID, triggerID string) error {
	if tenantID == "" {
		return runstore.ErrTenantRequired
	}
	const q = `DELETE FROM triggers WHERE tenant_id = ? AND trigger_id = ?`
	result, err := s.db.ExecContext(ctx, s.dialect.Rebind(q), tenantID, triggerID)
	if err != nil {
		return fmt.Errorf("trigger: deleting %q: %w", triggerID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("trigger: deleting %q: %w", triggerID, err)
	}
	if affected == 0 {
		return fmt.Errorf("%w: %q", ErrNoSuchTrigger, triggerID)
	}
	return nil
}

// nonNilMapping keeps the stored JSON an object rather than `null`, so a
// trigger created with no mapping reads back as one with an empty one.
func nonNilMapping(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// boolText is how a boolean is stored: every column is TEXT so that one DDL
// file serves SQLite and Postgres, which is the rule the catalog's table
// follows too.
func boolText(v bool) string {
	if v {
		return "1"
	}
	return "0"
}

// isDuplicate reports whether err is the primary key refusing a second row.
//
// It is matched on the message rather than on a driver error type because the
// two drivers report it differently — "UNIQUE constraint failed" from SQLite,
// "duplicate key value violates unique constraint" from Postgres — and
// importing either driver's error package here would tie this store to the
// dialect it is meant to be free of.
func isDuplicate(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "duplicate key value")
}
