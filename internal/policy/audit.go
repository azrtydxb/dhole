package policy

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// timeFormat matches the run store: SQLite has no time type, and RFC3339 with
// nanoseconds in UTC sorts lexicographically in chronological order.
const timeFormat = time.RFC3339Nano

// AuditRecord is one decision, kept so it can be answered for later. ADR 0012
// asks for a single trail behind a single evaluation point, which means an
// allow is recorded as faithfully as a denial: "why was this allowed" is the
// harder question of the two.
type AuditRecord struct {
	TenantID  string
	Tier      string
	Subject   string
	Rule      string
	Reason    string
	PluginRef string
	Allow     bool
	At        time.Time
}

// Auditor records decisions.
type Auditor interface {
	Record(ctx context.Context, r AuditRecord) error
}

// DiscardAudit throws decisions away. It exists for benchmarks and for the
// tests that measure evaluation on its own; a deployment uses SQLAudit.
type DiscardAudit struct{}

// Record discards r.
func (DiscardAudit) Record(context.Context, AuditRecord) error { return nil }

// SQLAudit writes the audit trail to the policy_audit table, in the same
// database as the run event log and the definitions — one decision, one row,
// one place to read the history back from.
type SQLAudit struct {
	db      *sql.DB
	dialect runstore.Dialect
}

// Compile-time proof the SQL auditor is the Auditor the engine consumes.
var _ Auditor = (*SQLAudit)(nil)

// NewSQLAudit returns an auditor over a SQLite handle. It is the convenience
// form of NewSQLAuditWithDialect and nothing more: a Postgres deployment calls
// NewSQLAuditWithDialect, because pgx rejects the `?` placeholders these
// statements are written with.
func NewSQLAudit(db *sql.DB) *SQLAudit {
	return NewSQLAuditWithDialect(db, runstore.DialectSQLite)
}

// NewSQLAuditWithDialect returns an auditor over db, which speaks dialect and
// whose schema is expected to carry the migrations the run store applies.
func NewSQLAuditWithDialect(db *sql.DB, dialect runstore.Dialect) *SQLAudit {
	return &SQLAudit{db: db, dialect: dialect}
}

// Record appends one decision. An empty tenant is refused: there is no
// unscoped record, even while only one tenant exists.
func (a *SQLAudit) Record(ctx context.Context, r AuditRecord) error {
	if r.TenantID == "" {
		return ErrTenantRequired
	}
	if r.At.IsZero() {
		r.At = time.Now().UTC()
	}
	id, err := recordID()
	if err != nil {
		return err
	}

	// `allow` is an INTEGER rather than a boolean so one dialect-neutral
	// migration serves both SQLite and Postgres.
	const q = `INSERT INTO policy_audit
		(tenant_id, id, at, tier, subject, rule, reason, plugin_ref, allow)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	allow := 0
	if r.Allow {
		allow = 1
	}
	if _, err := a.db.ExecContext(ctx, a.dialect.Rebind(q),
		r.TenantID, id, r.At.UTC().Format(timeFormat),
		r.Tier, r.Subject, r.Rule, r.Reason, r.PluginRef, allow,
	); err != nil {
		return fmt.Errorf("policy: append audit row: %w", err)
	}
	return nil
}

// Records returns the tenant's most recent decisions, oldest first, capped at
// limit. There is no unscoped query: an empty tenant is refused rather than
// read as a wildcard.
func (a *SQLAudit) Records(ctx context.Context, tenantID string, limit int) ([]AuditRecord, error) {
	if tenantID == "" {
		return nil, ErrTenantRequired
	}
	if limit <= 0 {
		limit = 100
	}
	const q = `SELECT tenant_id, at, tier, subject, rule, reason, plugin_ref, allow
		FROM policy_audit WHERE tenant_id = ? ORDER BY at ASC, id ASC LIMIT ?`
	rows, err := a.db.QueryContext(ctx, a.dialect.Rebind(q), tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("policy: read audit rows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []AuditRecord
	for rows.Next() {
		var (
			r     AuditRecord
			at    string
			allow int
		)
		if err := rows.Scan(&r.TenantID, &at, &r.Tier, &r.Subject,
			&r.Rule, &r.Reason, &r.PluginRef, &allow); err != nil {
			return nil, fmt.Errorf("policy: read audit rows: %w", err)
		}
		r.Allow = allow != 0
		if r.At, err = time.Parse(timeFormat, at); err != nil {
			return nil, fmt.Errorf("policy: parse audit time: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("policy: read audit rows: %w", err)
	}
	return out, nil
}

// recordID is the row's identity. Rows are ordered by time and then by id, so
// two decisions recorded in the same nanosecond still read back in a stable
// order.
func recordID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("policy: audit row id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
