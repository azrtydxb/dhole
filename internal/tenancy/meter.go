package tenancy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/azrtydxb/dhole/internal/runstore"
)

// StepAttemptLost and the dispatch payload below MIRROR what the scheduler
// persists, rather than being imported from it, and the direction of the
// dependency is the reason. Quotas are enforced IN the scheduler, so the
// scheduler imports this package; importing it back would be a cycle.
//
// Mirroring is safe here in a way copying code would not be, because both are
// already persistence contracts rather than internal details: the event type
// is stored verbatim in run_events and the payload is JSON that older rows
// still hold, so both are additive-only by rule. The guard against drift is in
// the tests, which build these payloads with the scheduler's own marshaller
// and would fail the moment the two disagreed.
const StepAttemptLost runstore.EventType = "STEP_ATTEMPT_LOST"

// dispatched is the part of a STEP_DISPATCHED payload the meter needs: whether
// the step was SERVED from the cache rather than sent to an engine. Everything
// else in that payload is somebody else's business.
type dispatched struct {
	CacheHit bool `json:"cache_hit"`
}

// decodeDispatched reads the cache verdict off a STEP_DISPATCHED payload. An
// empty payload is not an error — older events carry none — but malformed JSON
// is: guessing that a step was not a cache hit would bill for it.
func decodeDispatched(payload []byte) (dispatched, error) {
	var d dispatched
	if len(payload) == 0 {
		return d, nil
	}
	if err := json.Unmarshal(payload, &d); err != nil {
		return d, fmt.Errorf("tenancy: decoding a %s payload: %w", runstore.StepDispatched, err)
	}
	return d, nil
}

// UsageKind names a class of metered work. The values are stored verbatim and
// are part of the primary key, so they are a persistence contract: add new
// ones, never rename an existing one, or every historical row silently leaves
// the total it belonged to.
type UsageKind string

// The kinds this system meters. Each has ONE unit, given by Unit, because a
// column that mixes seconds and bytes cannot be summed.
const (
	// KindRunStarted is one admitted run. Quantity is always 1; it is what the
	// daily run quota counts.
	KindRunStarted UsageKind = "run_started"
	// KindStepSeconds is compute the customer is charged for. Quantity is
	// milliseconds.
	KindStepSeconds UsageKind = "step_seconds"
	// KindStepSecondsUnbilled is compute the customer is NOT charged for —
	// an attempt the platform lost. Quantity is milliseconds. It is recorded
	// rather than dropped so that "the run took nine seconds and I was charged
	// for two" has an answer in the ledger.
	KindStepSecondsUnbilled UsageKind = "step_seconds_unbilled"
	// KindCacheHit is a step served from the cache. Quantity is always 0: no
	// compute happened. It is recorded so an invoice can show what the cache
	// saved.
	KindCacheHit UsageKind = "cache_hit"
	// KindCASBytes is stored artifact bytes, keyed by digest so identical
	// bytes are charged once.
	KindCASBytes UsageKind = "cas_bytes"
	// KindCASBytesReleased is stored artifact bytes the collector reclaimed,
	// keyed by the same digest that was charged. Quantity is bytes, positive:
	// it is subtracted by CASBytes rather than stored negative, because a
	// ledger of signed quantities is one where a sign error is invisible and
	// RecordUsage would have to stop refusing negatives.
	//
	// It exists because CAS usage only ever grew. The collector deleted blobs
	// and wrote nothing that offset them, so a tenant who reclaimed a terabyte
	// was still charged for it and was eventually refused every write against
	// a store that was nearly empty.
	KindCASBytesReleased UsageKind = "cas_bytes_released"
)

// Unit is the base unit of a kind's Quantity, for whoever formats an invoice.
func (k UsageKind) Unit() string {
	switch k {
	case KindStepSeconds, KindStepSecondsUnbilled:
		return "ms"
	case KindCASBytes, KindCASBytesReleased:
		return "bytes"
	case KindRunStarted, KindCacheHit:
		return "count"
	default:
		return ""
	}
}

// Usage is one row of the ledger.
//
// (Kind, RunID, StepID, Attempt) is its IDENTITY, and that is the whole reason
// metering can be correct in a system that delivers at least once. Recording
// the same usage twice — a redelivered status, a second control plane
// advancing the same run, a re-run of the meter over a log it has already seen
// — writes nothing the second time.
//
// Quantity is an integer in the kind's own base unit. Never a float: a bill
// that rounds differently on two machines is not defensible to the person
// reading it.
type Usage struct {
	Kind     UsageKind
	RunID    string
	StepID   string
	Attempt  uint32
	Quantity int64
	// Billable says whether this row is charged for. A false row is history,
	// not revenue.
	Billable bool
	At       time.Time
}

// Summary is what one metering pass added.
//
// The counts are of what this pass WROTE, not of what the log contains: a
// second pass over the same log summarises zero billed attempts, which is
// exactly the signal that the meter did not double-charge.
type Summary struct {
	// StepSeconds is the billable compute this pass recorded, in seconds.
	StepSeconds float64
	// BilledAttempts is how many attempts this pass charged for.
	BilledAttempts int
	// UnbilledAttempts is how many it recorded and did not charge for.
	UnbilledAttempts int
	// CacheHits is how many steps were served from cache and therefore not
	// charged as if they had run.
	CacheHits int
}

// RunLog is the part of the run event log the meter needs. It is an interface
// so the meter depends on a replay and nothing else; runstore.Store satisfies
// it.
type RunLog interface {
	Replay(ctx context.Context, tenantID, runID string) ([]runstore.Event, error)
}

// MeterConfig configures a Meter.
type MeterConfig struct {
	// Store is the usage ledger. Required.
	Store *Store
	// Log is the run event log usage is derived from. Required for MeterRun.
	Log RunLog
}

// Meter turns work into billable usage.
//
// It derives usage from the run event log rather than from the messages that
// carried the work, and that choice is what makes a bill defensible. The log
// is the source of truth for a run (ADR 0003); anything derived from it can be
// recomputed from it, so a disputed invoice line can be answered by replaying
// the run rather than by trusting a counter nobody can inspect.
type Meter struct {
	store *Store
	log   RunLog
}

// NewMeter builds a Meter.
func NewMeter(cfg MeterConfig) (*Meter, error) {
	if cfg.Store == nil {
		return nil, errors.New("tenancy: a meter needs a store")
	}
	return &Meter{store: cfg.Store, log: cfg.Log}, nil
}

// Record writes one usage row, idempotently.
func (m *Meter) Record(ctx context.Context, tenantID string, u Usage) error {
	_, err := m.store.RecordUsage(ctx, tenantID, u)
	return err
}

// MeterRun derives one run's usage from its event log and records what is not
// there yet.
//
// The rules, which are the billing policy of this system written where it can
// be tested:
//
//   - An attempt is billed for the wall-clock time between its STEP_DISPATCHED
//     and the event that ended it.
//
//   - An attempt that SUCCEEDED or FAILED is billable. A step that exits
//     non-zero consumed exactly the compute a successful one would have, and
//     the customer's own code decided to exit that way; not billing it would
//     let a pipeline run a fleet for free by failing on purpose. So a retry's
//     second attempt is charged, because it is a second real execution the
//     customer asked for.
//
//   - An attempt the PLATFORM lost — STEP_ATTEMPT_LOST, written when a lease
//     expired and the orphan sweep re-dispatched the step under a new fence —
//     is NOT billable. Nobody asked for that work to be repeated; we lost it.
//     Charging for it would bill a customer for our own incident, which is the
//     one error in a meter that is worse than undercounting. The redispatch is
//     a fresh attempt and is judged on its own terms.
//
//   - A CACHE HIT is not billed. A hit writes the same STEP_DISPATCHED and
//     STEP_SUCCEEDED pair a real dispatch writes — deliberately, so nothing
//     replaying the log needs to know about the cache — and the only thing
//     separating them is the CacheHit flag on the dispatch payload. Reading
//     that flag is the difference between charging for two seconds of compute
//     and charging for the microseconds it took to not run a step.
//
//   - An attempt still IN FLIGHT is not recorded at all. It has no duration
//     yet, and a later pass will meter it once it ends.
//
// It is safe to call repeatedly on the same run, from several control planes:
// every row it writes is keyed by the work, so a second pass adds nothing.
func (m *Meter) MeterRun(ctx context.Context, tenantID, runID string) (Summary, error) {
	if tenantID == "" {
		return Summary{}, fmt.Errorf("tenancy: metering a run: %w", runstore.ErrTenantRequired)
	}
	if runID == "" {
		return Summary{}, errors.New("tenancy: metering a run: a run id is required")
	}
	if m.log == nil {
		return Summary{}, errors.New("tenancy: metering a run needs a run log")
	}
	events, err := m.log.Replay(ctx, tenantID, runID)
	if err != nil {
		return Summary{}, fmt.Errorf("tenancy: metering run %s: %w", runID, err)
	}

	type attemptKey struct {
		stepID  string
		attempt uint32
	}
	type attempt struct {
		dispatchedAt time.Time
		endedAt      time.Time
		cacheHit     bool
		terminal     runstore.EventType
	}
	attempts := make(map[attemptKey]*attempt)
	// Order matters: two attempts of one step must be recorded in the order
	// they ran, so an invoice reads the way the run did.
	var order []attemptKey

	for _, e := range events {
		key := attemptKey{stepID: e.StepID, attempt: e.Attempt}
		switch e.Type {
		case runstore.StepDispatched:
			d, err := decodeDispatched(e.Payload)
			if err != nil {
				return Summary{}, fmt.Errorf("tenancy: metering run %s: %w", runID, err)
			}
			if _, seen := attempts[key]; !seen {
				order = append(order, key)
			}
			attempts[key] = &attempt{dispatchedAt: e.At, cacheHit: d.CacheHit}
		case runstore.StepSucceeded, runstore.StepFailed, StepAttemptLost:
			a, ok := attempts[key]
			if !ok {
				// A terminal event with no dispatch in the log is not
				// something to guess a duration for. Charging a made-up
				// number is worse than charging nothing.
				continue
			}
			a.endedAt = e.At
			a.terminal = e.Type
		}
	}

	var sum Summary
	for _, key := range order {
		a := attempts[key]
		if a.terminal == "" {
			continue // Still in flight; a later pass will meter it.
		}
		u := Usage{
			RunID: runID, StepID: key.stepID, Attempt: key.attempt,
			At: a.endedAt.UTC(),
		}
		switch {
		case a.cacheHit:
			u.Kind, u.Quantity, u.Billable = KindCacheHit, 0, false
		case a.terminal == StepAttemptLost:
			u.Kind, u.Billable = KindStepSecondsUnbilled, false
			u.Quantity = milliseconds(a.dispatchedAt, a.endedAt)
		default:
			u.Kind, u.Billable = KindStepSeconds, true
			u.Quantity = milliseconds(a.dispatchedAt, a.endedAt)
		}

		written, err := m.store.RecordUsage(ctx, tenantID, u)
		if err != nil {
			return Summary{}, err
		}
		if !written {
			continue // Already metered: a redelivery, or a second plane.
		}
		switch u.Kind {
		case KindCacheHit:
			sum.CacheHits++
		case KindStepSecondsUnbilled:
			sum.UnbilledAttempts++
		case KindStepSeconds:
			sum.BilledAttempts++
			sum.StepSeconds += float64(u.Quantity) / 1000
		case KindRunStarted, KindCASBytes, KindCASBytesReleased:
			// Not produced by this pass.
		}
	}
	return sum, nil
}

// milliseconds is an attempt's duration, floored at zero. A negative duration
// means two clocks disagreed, and the honest answer to that is to bill nothing
// rather than to bill a wrapped-around number.
func milliseconds(from, to time.Time) int64 {
	d := to.Sub(from)
	if d < 0 {
		return 0
	}
	return d.Milliseconds()
}

const insertUsage = `INSERT INTO usage_records
	(tenant_id, kind, run_id, step_id, attempt, quantity, billable,
	 occurred_at, occurred_at_unix_nano)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`

// RecordUsage writes one usage row and reports whether it was new.
//
// The insert is ON CONFLICT DO NOTHING over the ledger's primary key, so
// recording the same work twice is a no-op returning false. That is the single
// mechanism this package's idempotence rests on: it holds against a redelivered
// status, a re-run of the meter, and two control planes metering one run at the
// same time, because all three present the same key.
func (s *Store) RecordUsage(ctx context.Context, tenantID string, u Usage) (bool, error) {
	if tenantID == "" {
		return false, fmt.Errorf("tenancy: recording usage: %w", runstore.ErrTenantRequired)
	}
	if u.Kind == "" {
		return false, errors.New("tenancy: recording usage: a kind is required")
	}
	if u.Quantity < 0 {
		return false, fmt.Errorf("tenancy: recording usage: quantity cannot be negative, got %d", u.Quantity)
	}
	at := u.At.UTC()
	if at.IsZero() {
		return false, errors.New("tenancy: recording usage: a timestamp is required")
	}
	billable := 0
	if u.Billable {
		billable = 1
	}
	res, err := s.db.ExecContext(ctx, s.dialect.Rebind(insertUsage),
		tenantID, string(u.Kind), u.RunID, u.StepID, u.Attempt, u.Quantity, billable,
		at.Format(runstore.TimeFormat), at.UnixNano())
	if err != nil {
		return false, fmt.Errorf("tenancy: recording %s usage for %s/%s: %w",
			u.Kind, u.RunID, u.StepID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("tenancy: recording %s usage for %s/%s: %w",
			u.Kind, u.RunID, u.StepID, err)
	}
	return affected > 0, nil
}

const selectRunStarted = `SELECT COUNT(*) FROM usage_records
	WHERE tenant_id = ? AND kind = ? AND run_id = ?`

const countRunsToday = `SELECT COUNT(*) FROM usage_records
	WHERE tenant_id = ? AND kind = ? AND occurred_at_unix_nano >= ?`

const sumCASBytes = `SELECT COALESCE(SUM(quantity), 0) FROM usage_records
	WHERE tenant_id = ? AND kind = ?`

// chargedCASBytes is what a single digest was billed, which is the amount to
// credit back when it is collected. Summed rather than read as one row: the
// charge is keyed by digest, but reading a sum means a schema that ever allows
// two rows for one digest still credits the right total.
const chargedCASBytes = `SELECT COALESCE(SUM(quantity), 0) FROM usage_records
	WHERE tenant_id = ? AND kind = ? AND step_id = ?`

const sumBilledStepMillis = `SELECT COALESCE(SUM(quantity), 0) FROM usage_records
	WHERE tenant_id = ? AND kind = ? AND billable = 1`

const selectUsageForRun = `SELECT kind, run_id, step_id, attempt, quantity, billable, occurred_at
	FROM usage_records WHERE tenant_id = ? AND run_id = ?
	ORDER BY occurred_at_unix_nano, step_id, attempt`

// runStartedToday reports whether this run has already been counted against
// the daily quota. The window is the run id itself rather than the day,
// because a run that started before midnight and is re-admitted after it is
// still the same run and must not be charged a second entry.
func (s *Store) runStartedToday(ctx context.Context, tenantID, runID string, _ time.Time) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, s.dialect.Rebind(selectRunStarted),
		tenantID, string(KindRunStarted), runID).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("tenancy: checking whether run %s was admitted: %w", runID, err)
	}
	return n > 0, nil
}

// RunsStartedToday counts the runs the tenant has been admitted in the UTC day
// containing now. It is the number the daily quota is applied to and the
// number that appears on the invoice, deliberately the same number.
func (s *Store) RunsStartedToday(ctx context.Context, tenantID string, now time.Time) (int, error) {
	if tenantID == "" {
		return 0, fmt.Errorf("tenancy: counting runs: %w", runstore.ErrTenantRequired)
	}
	day := now.UTC().Truncate(24 * time.Hour)
	var n int
	err := s.db.QueryRowContext(ctx, s.dialect.Rebind(countRunsToday),
		tenantID, string(KindRunStarted), day.UnixNano()).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("tenancy: counting runs for %q: %w", tenantID, err)
	}
	return n, nil
}

// CASBytes is how many bytes of content-addressed storage the tenant holds, as
// the ledger sees it.
func (s *Store) CASBytes(ctx context.Context, tenantID string) (int64, error) {
	if tenantID == "" {
		return 0, fmt.Errorf("tenancy: totalling stored bytes: %w", runstore.ErrTenantRequired)
	}
	var stored, released int64
	if err := s.db.QueryRowContext(ctx, s.dialect.Rebind(sumCASBytes),
		tenantID, string(KindCASBytes)).Scan(&stored); err != nil {
		return 0, fmt.Errorf("tenancy: totalling stored bytes for %q: %w", tenantID, err)
	}
	if err := s.db.QueryRowContext(ctx, s.dialect.Rebind(sumCASBytes),
		tenantID, string(KindCASBytesReleased)).Scan(&released); err != nil {
		return 0, fmt.Errorf("tenancy: totalling reclaimed bytes for %q: %w", tenantID, err)
	}
	// Floored at zero. The two sums are written by different components at
	// different times, and a credit that outran its charge would otherwise
	// report a tenant as holding a negative number of bytes — which is not a
	// quantity anyone can act on, and would read as unlimited headroom.
	if total := stored - released; total > 0 {
		return total, nil
	}
	return 0, nil
}

// ChargedCASBytes is what this tenant was billed for one digest, or zero if it
// was never charged for. Zero is not an error: blobs predate the metering.
func (s *Store) ChargedCASBytes(ctx context.Context, tenantID, hex string) (int64, error) {
	if tenantID == "" {
		return 0, fmt.Errorf("tenancy: reading a charge: %w", runstore.ErrTenantRequired)
	}
	var charged int64
	err := s.db.QueryRowContext(ctx, s.dialect.Rebind(chargedCASBytes),
		tenantID, string(KindCASBytes), hex).Scan(&charged)
	if err != nil {
		return 0, fmt.Errorf("tenancy: reading the charge for %q: %w", hex, err)
	}
	return charged, nil
}

// BilledStepSeconds is the compute the tenant has been charged for, in
// seconds. Rows marked unbillable are excluded — they are history, not
// revenue.
func (s *Store) BilledStepSeconds(ctx context.Context, tenantID string) (float64, error) {
	if tenantID == "" {
		return 0, fmt.Errorf("tenancy: totalling step seconds: %w", runstore.ErrTenantRequired)
	}
	var millis int64
	err := s.db.QueryRowContext(ctx, s.dialect.Rebind(sumBilledStepMillis),
		tenantID, string(KindStepSeconds)).Scan(&millis)
	if err != nil {
		return 0, fmt.Errorf("tenancy: totalling step seconds for %q: %w", tenantID, err)
	}
	return float64(millis) / 1000, nil
}

// UsageForRun returns one run's ledger rows, oldest first. It is what an
// invoice line is built from, and what answers "why was I charged this".
func (s *Store) UsageForRun(ctx context.Context, tenantID, runID string) ([]Usage, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenancy: reading usage: %w", runstore.ErrTenantRequired)
	}
	rows, err := s.db.QueryContext(ctx, s.dialect.Rebind(selectUsageForRun), tenantID, runID)
	if err != nil {
		return nil, fmt.Errorf("tenancy: reading usage for run %s: %w", runID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []Usage
	for rows.Next() {
		var u Usage
		var kind, at string
		var billable int
		if err := rows.Scan(&kind, &u.RunID, &u.StepID, &u.Attempt, &u.Quantity, &billable, &at); err != nil {
			return nil, fmt.Errorf("tenancy: reading usage for run %s: %w", runID, err)
		}
		u.Kind = UsageKind(kind)
		u.Billable = billable != 0
		if u.At, err = time.Parse(runstore.TimeFormat, at); err != nil {
			return nil, fmt.Errorf("tenancy: reading usage for run %s: %w", runID, err)
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("tenancy: reading usage for run %s: %w", runID, err)
	}
	return out, nil
}
