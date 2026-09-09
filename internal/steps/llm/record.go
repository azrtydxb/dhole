package llm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// Call is one recorded model call: what was asked, what came back, which model
// answered, what it cost, and how long it took.
//
// Every attempt is one Call, including the ones that failed. A step that
// retried a malformed answer three times spent three calls' worth of money and
// three calls' worth of somebody's data, and a record that keeps only the
// successful one describes neither the bill nor the exposure.
type Call struct {
	RunID            string
	StepID           string
	Attempt          uint32
	ModelFingerprint string
	Prompt           string
	Response         string
	PromptTokens     int
	CompletionTokens int
	Latency          time.Duration
	At               time.Time
}

// Recorder writes and reads llm_calls, and enforces the two rules that make
// that table safe to have: it is tenant-scoped on every path, and it has a
// retention window that actually deletes.
//
// It takes (*sql.DB, runstore.Dialect) like every other store here, and writes
// its statements once with `?` placeholders rebound per dialect — a
// hand-copied Postgres variant of the same statement is two things that drift.
type Recorder struct {
	db        *sql.DB
	dialect   runstore.Dialect
	retention time.Duration
}

// ErrRetentionRequired refuses a recorder with no retention window.
//
// This table holds prompt content — whatever the pipeline fed the model. "Kept
// for ever because nobody configured a window" is the default that turns an
// operational record into a breach, so it is not available by omission. A
// deployment that genuinely wants a decade says a decade.
var ErrRetentionRequired = errors.New("llm: a retention window is required for recorded prompts")

// NewRecorder builds a Recorder over an already-migrated handle. Open it with
// runstore.OpenSQLite or runstore.OpenPostgres and pass the matching dialect:
// pgx rejects the `?` placeholders below outright, so a Postgres handle left
// on the zero value records nothing at all.
func NewRecorder(db *sql.DB, dialect runstore.Dialect, retention time.Duration) (*Recorder, error) {
	if db == nil {
		return nil, errors.New("llm: a database handle is required")
	}
	if retention <= 0 {
		return nil, ErrRetentionRequired
	}
	return &Recorder{db: db, dialect: dialect, retention: retention}, nil
}

// Retention is the window this recorder stamps onto the rows it writes.
func (r *Recorder) Retention() time.Duration { return r.retention }

// insertCall is written once and shared by the direct and transactional write
// paths, so the two cannot disagree about the row's shape.
const insertCall = `INSERT INTO llm_calls (
	tenant_id, run_id, step_id, attempt, model_fingerprint,
	prompt, response, prompt_tokens, completion_tokens, latency_ms,
	recorded_at, retain_until
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

// Record writes one call.
//
// Prompt and response are redacted first. A credential that reached this far
// — echoed by a provider into an error body, or interpolated into a prompt
// template by a pipeline that should not have — must not be made durable by
// the act of recording it.
func (r *Recorder) Record(ctx context.Context, tenantID string, c Call) error {
	if tenantID == "" {
		return fmt.Errorf("llm: recording a call: %w", runstore.ErrTenantRequired)
	}
	args := r.rowArgs(tenantID, c)
	if _, err := r.db.ExecContext(ctx, r.dialect.Rebind(insertCall), args...); err != nil {
		return fmt.Errorf("llm: recording call %s/%s attempt %d: %w",
			c.RunID, c.StepID, c.Attempt, err)
	}
	return nil
}

// rowArgs is the row itself, in the order insertCall names its columns.
func (r *Recorder) rowArgs(tenantID string, c Call) []any {
	at := c.At.UTC()
	return []any{
		tenantID, c.RunID, c.StepID, c.Attempt, c.ModelFingerprint,
		redact(c.Prompt), redact(c.Response),
		c.PromptTokens, c.CompletionTokens, c.Latency.Milliseconds(),
		at.Format(runstore.TimeFormat), at.Add(r.retention).Unix(),
	}
}

const selectCalls = `SELECT run_id, step_id, attempt, model_fingerprint,
	prompt, response, prompt_tokens, completion_tokens, latency_ms, recorded_at
	FROM llm_calls WHERE tenant_id = ? AND run_id = ?
	ORDER BY step_id, attempt`

// Calls returns one run's recorded calls, in attempt order.
//
// The tenant is a parameter and not a filter applied afterwards: run ids are
// chosen per tenant, so two tenants running the same pipeline hold the same
// run id, and a read that forgot the scope would return both.
func (r *Recorder) Calls(ctx context.Context, tenantID, runID string) ([]Call, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("llm: reading calls: %w", runstore.ErrTenantRequired)
	}
	rows, err := r.db.QueryContext(ctx, r.dialect.Rebind(selectCalls), tenantID, runID)
	if err != nil {
		return nil, fmt.Errorf("llm: reading calls for %s: %w", runID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []Call
	for rows.Next() {
		var (
			c         Call
			latencyMS int64
			at        string
		)
		if err := rows.Scan(&c.RunID, &c.StepID, &c.Attempt, &c.ModelFingerprint,
			&c.Prompt, &c.Response, &c.PromptTokens, &c.CompletionTokens,
			&latencyMS, &at); err != nil {
			return nil, fmt.Errorf("llm: reading calls for %s: %w", runID, err)
		}
		c.Latency = time.Duration(latencyMS) * time.Millisecond
		if c.At, err = time.Parse(runstore.TimeFormat, at); err != nil {
			return nil, fmt.Errorf("llm: reading calls for %s: %w", runID, err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("llm: reading calls for %s: %w", runID, err)
	}
	return out, nil
}

// Purge deletes every recorded call whose retention window has closed, across
// every tenant, and reports how many rows went.
//
// This is what makes retain_until a control rather than a comment. A retention
// column nothing ever deletes from is worse than no column: it reads, to
// anyone auditing the schema, as a promise that is being kept.
//
// It sweeps across tenants deliberately — it is the operator's housekeeping,
// not a tenant-scoped query — and it is the one method here that takes no
// tenant. The window it applies was fixed when the row was written, so
// shortening a deployment's configured window does not retroactively expire
// rows already stamped with the longer one; that is a migration, not a sweep.
func (r *Recorder) Purge(ctx context.Context, now time.Time) (int64, error) {
	const q = `DELETE FROM llm_calls WHERE retain_until <= ?`
	res, err := r.db.ExecContext(ctx, r.dialect.Rebind(q), now.UTC().Unix())
	if err != nil {
		return 0, fmt.Errorf("llm: purging expired call records: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("llm: purging expired call records: %w", err)
	}
	return n, nil
}

// --- redaction ----------------------------------------------------------

// redacted is what a matched credential is replaced with. It is deliberately
// visible: a record that silently dropped the text would leave a reader
// wondering what the prompt actually said.
const redacted = "[redacted]"

// credentialPatterns are the shapes a credential takes when it turns up
// somewhere it should not: in a prompt built from a template that interpolated
// too much, or echoed back by a provider that quotes the request in its error
// body.
//
// This is a net, not a proof. The real defence is that this package is handed
// a constructed model and never sees a key at all (ADR: engines never receive
// secret values). But everything here is written down — the call record
// outlives the process, and a log line leaves the tenant boundary entirely —
// so a second, cheap net sits in front of both.
var credentialPatterns = []*regexp.Regexp{
	// Provider API keys: OpenAI/Anthropic `sk-`, Slack `xox*-`, GitHub `gh?_`.
	regexp.MustCompile(`(?i)\bsk-[A-Za-z0-9_\-]{8,}`),
	regexp.MustCompile(`(?i)\bxox[abposr]-[A-Za-z0-9_\-]{8,}`),
	regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{8,}`),
	regexp.MustCompile(`(?i)\bAKIA[0-9A-Z]{12,}`),
	// Bearer tokens and basic auth, as they appear in an echoed header.
	regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9_\-.=+/]{8,}`),
	// Anything a caller was decent enough to label.
	regexp.MustCompile(`(?i)\b(api[_\-]?key|secret|token|password|passwd)` +
		`\s*[:=]\s*"?[A-Za-z0-9_\-.=+/]{8,}"?`),
}

// redact replaces every credential-shaped run in s. It is applied to
// everything this package persists, logs, or returns in an error.
func redact(s string) string {
	for _, re := range credentialPatterns {
		s = re.ReplaceAllString(s, redacted)
	}
	return s
}
