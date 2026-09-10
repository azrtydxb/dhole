// Package wait makes a run wait without anything staying awake.
//
// A wait is a ROW. Nothing sleeps, nothing holds the run in memory, and the
// process that scheduled a wait is routinely not the process that fires it —
// which is the whole payoff of ADR 0003: a run is a state machine over a
// persisted event log, so a three-day wait, or a wait for a person who is
// asleep, costs a row rather than a goroutine. A timer implemented as
// time.AfterFunc would pass every test that never restarts anything and lose
// every outstanding wait on the first deploy.
//
// Two rules follow from the table, and both are load-bearing:
//
// A timer is one row with one due time, so coming back three days late fires
// it ONCE. The bug this refuses is the catch-up loop that walks the missed
// intervals and fires a nightly job forty times after a weekend outage.
//
// A due timer is claimed before it is acted on, the way the outbox claims a
// row it is about to publish: FOR UPDATE SKIP LOCKED on Postgres, the
// immediate write lock on SQLite. Two control planes poll the same table every
// second, and a timer handed to both resumes its run twice.
package wait

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// StepTimerFired records that a wait ended because its time came. It is the
// audit half of firing: the step's own STEP_SUCCEEDED says the gate is open,
// and this says why and how late.
const StepTimerFired runstore.EventType = "STEP_TIMER_FIRED"

// claimBatch bounds one poll. It matters most on SQLite, where the claim is a
// database-wide write lock held for the whole batch: an unbounded sweep after
// a long outage would stall every other writer until it finished.
const claimBatch = 100

// ErrRunRequired is returned for a timer that names no run or no step. A
// timer that resumes nothing is a row nobody will ever read.
var ErrRunRequired = errors.New("wait: a run and a step are required")

// Due is one timer that has come due and been claimed by this caller. The
// tenant travels on it because Due is the only cross-tenant read in this
// package: a resume under the wrong scope is exactly the bug the tenant rule
// exists to prevent.
type Due struct {
	TenantID string
	RunID    string
	StepID   string
	// At is when the timer was DUE, not when it fired. After an outage those
	// are days apart, and the difference is the only record of how late the
	// wait ended.
	At time.Time
}

// Scheduled is the payload of a STEP_AWAITING_TIMER event.
type Scheduled struct {
	DueAt time.Time `json:"due_at"`
}

// Fired is the payload of a STEP_TIMER_FIRED event. It carries both times so
// that a run resumed long after its due time says so in its own log.
type Fired struct {
	DueAt   time.Time `json:"due_at"`
	FiredAt time.Time `json:"fired_at"`
}

// Timers is the durable timer table. It is safe for concurrent use and safe to
// run in several control planes at once.
type Timers struct {
	store runstore.Store
	batch int
}

// TimersOption tunes a timer table.
type TimersOption func(*Timers)

// WithClaimBatch bounds how many timers one poll claims. Smaller is not
// slower in any way that matters — the poll runs every second — and it is the
// knob that keeps a long backlog from holding SQLite's database-wide write
// lock for the whole sweep.
func WithClaimBatch(n int) TimersOption {
	return func(t *Timers) {
		if n > 0 {
			t.batch = n
		}
	}
}

// NewTimers builds the timer table over the run store, whose database it
// shares. Sharing is not incidental: firing a timer appends the events that
// resume the run, and those two acts have to commit together or a claimed
// timer can vanish leaving the run waiting forever.
func NewTimers(store runstore.Store, opts ...TimersOption) *Timers {
	t := &Timers{store: store, batch: claimBatch}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Schedule records that a step waits until at. It writes the timer row and the
// STEP_AWAITING_TIMER event that gates the step in ONE transaction: a gate
// without a timer waits forever, and a timer without a gate fires at a step
// that has already been dispatched.
//
// It is idempotent per (tenant, run, step): a redelivered command schedules the
// same wait, not a second one.
func (t *Timers) Schedule(ctx context.Context, tenantID, runID, stepID string, at time.Time) error {
	if tenantID == "" {
		return fmt.Errorf("wait: schedule: %w", runstore.ErrTenantRequired)
	}
	if runID == "" || stepID == "" {
		return ErrRunRequired
	}
	if at.IsZero() {
		return errors.New("wait: schedule: a due time is required")
	}
	due := at.UTC()
	payload, err := json.Marshal(Scheduled{DueAt: due})
	if err != nil {
		return fmt.Errorf("wait: schedule: %w", err)
	}

	return t.store.WithTx(ctx, func(tx runstore.Tx) error {
		return arm(ctx, tx, tenantID, runID, stepID, due, payload)
	})
}

// ArmInTx is Schedule inside a transaction the CALLER owns, and it is the
// whole of the atomicity fix.
//
// A gate used to be armed in a transaction of its own while the scheduler
// decided readiness in another. Sequences are allocated inside a transaction
// and visibility is not, so the gate could hold a lower sequence than the
// STEP_DISPATCHED of the step it gated: the log read "gated, then dispatched",
// and the wait had been skipped entirely. Handing the transaction in lets the
// decision that a step is ready and the arming of its gate be one act, with no
// interval for the other to be missed in.
//
// It is idempotent per (tenant, run, step) for the same reason Schedule is,
// and migration 0023 is what makes that true across transactions the caller's
// database cannot serialise.
func (t *Timers) ArmInTx(
	ctx context.Context, tx runstore.Tx, tenantID, runID, stepID string, at time.Time,
) error {
	if tenantID == "" {
		return fmt.Errorf("wait: arm: %w", runstore.ErrTenantRequired)
	}
	if runID == "" || stepID == "" {
		return ErrRunRequired
	}
	if at.IsZero() {
		return errors.New("wait: arm: a due time is required")
	}
	due := at.UTC()
	payload, err := json.Marshal(Scheduled{DueAt: due})
	if err != nil {
		return fmt.Errorf("wait: arm: %w", err)
	}
	return arm(ctx, tx, tenantID, runID, stepID, due, payload)
}

// arm writes the gate event and the timer row that must exist together: a gate
// without a timer waits for ever, and a timer without a gate fires at a step
// that has already been dispatched.
func arm(
	ctx context.Context, tx runstore.Tx,
	tenantID, runID, stepID string, due time.Time, payload []byte,
) error {
	outstanding, err := exists(ctx, tx,
		`SELECT COUNT(*) FROM run_timers
		 WHERE tenant_id = ? AND run_id = ? AND step_id = ?`,
		tenantID, runID, stepID)
	if err != nil {
		return err
	}
	if outstanding {
		return nil
	}

	if err := tx.Append(ctx, tenantID, runstore.Event{
		RunID:  runID,
		StepID: stepID,
		// Sequence 0: the store allocates it inside this transaction.
		// Computing it here would read a high-water mark another
		// caller is about to write, and the loser of that race is
		// discarded silently by the idempotent insert.
		Type:    scheduler.StepAwaitingTimer,
		Payload: payload,
		At:      time.Now().UTC(),
	}); err != nil {
		return fmt.Errorf("wait: schedule %s/%s: %w", runID, stepID, err)
	}

	const insert = `INSERT INTO run_timers (tenant_id, run_id, step_id, due_at, fired_at)
		VALUES (?, ?, ?, ?, NULL) ON CONFLICT DO NOTHING`
	if err := tx.Exec(ctx, tx.Dialect().Rebind(insert),
		tenantID, runID, stepID, due.Format(runstore.TimeFormat)); err != nil {
		return fmt.Errorf("wait: schedule %s/%s: %w", runID, stepID, err)
	}
	return nil
}

// Pending says whether a step still has an outstanding wait, and when it is
// due. It is how an interface answers "waiting until when", and it is also the
// only way to see that a fired timer has left the poll: a timer that fires but
// is never marked is re-scanned every second forever, and every one of those
// scans is a chance to fire it again.
func (t *Timers) Pending(
	ctx context.Context, tenantID, runID, stepID string,
) (time.Time, bool, error) {
	if tenantID == "" {
		return time.Time{}, false, fmt.Errorf("wait: pending: %w", runstore.ErrTenantRequired)
	}
	if runID == "" || stepID == "" {
		return time.Time{}, false, ErrRunRequired
	}

	var (
		due       time.Time
		streaming bool
	)
	err := t.store.WithTx(ctx, func(tx runstore.Tx) error {
		const q = `SELECT due_at FROM run_timers
			WHERE tenant_id = ? AND run_id = ? AND step_id = ? AND fired_at IS NULL`
		rows, err := tx.Query(ctx, tx.Dialect().Rebind(q), tenantID, runID, stepID)
		if err != nil {
			return fmt.Errorf("wait: pending %s/%s: %w", runID, stepID, err)
		}
		defer func() { _ = rows.Close() }()

		if rows.Next() {
			var dueAt string
			if err := rows.Scan(&dueAt); err != nil {
				return fmt.Errorf("wait: pending %s/%s: %w", runID, stepID, err)
			}
			due, err = time.Parse(runstore.TimeFormat, dueAt)
			if err != nil {
				return fmt.Errorf("wait: pending %s/%s: unreadable due time %q: %w",
					runID, stepID, dueAt, err)
			}
			streaming = true
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("wait: pending %s/%s: %w", runID, stepID, err)
		}
		return rows.Close()
	})
	if err != nil {
		return time.Time{}, false, err
	}
	return due, streaming, nil
}

// Due claims every timer that has come due by now, fires it, and returns what
// it claimed. Firing is: mark the row, append STEP_TIMER_FIRED and the step's
// STEP_SUCCEEDED — all inside the claim's transaction, so a timer is handed
// out exactly once and the run it belongs to is durably past its wait before
// anyone is told about it.
//
// A timer whose run has already finished, or whose step already has a verdict,
// is RETIRED instead: the row is marked fired, nothing is appended, and it is
// not returned. A wait cannot resurrect a run that ended without it.
//
// The caller is expected to advance each returned run; see Runner.
func (t *Timers) Due(ctx context.Context, now time.Time) ([]Due, error) {
	at := now.UTC()
	var fired []Due
	err := t.store.WithTx(ctx, func(tx runstore.Tx) error {
		fired = nil
		claimed, err := claim(ctx, tx, at, t.batch)
		if err != nil {
			return err
		}
		// Sequences are per tenant and read once per tenant per batch: the
		// log's own position, taken on this transaction's connection rather
		// than through the store, which on SQLite would deadlock against the
		// transaction already holding it.
		for _, d := range claimed {
			if err := markFired(ctx, tx, d, at); err != nil {
				return err
			}
			orphaned, err := retired(ctx, tx, d)
			if err != nil {
				return err
			}
			if orphaned {
				continue
			}
			if err := resume(ctx, tx, d, at); err != nil {
				return err
			}
			fired = append(fired, d)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return fired, nil
}

// claim takes ownership of a batch of due timers for the life of tx.
//
// This is the outbox's claim, deliberately: Postgres walks past rows another
// plane holds with FOR UPDATE SKIP LOCKED, and SQLite has no such clause, so
// the claim is the database-wide write lock its store takes when a transaction
// begins (`_txlock=immediate`). Both are claims; only the granularity differs,
// and SQLite is the single-node store where it does not matter. A third
// mechanism invented here would be a third thing to get wrong.
func claim(ctx context.Context, tx runstore.Tx, now time.Time, batch int) ([]Due, error) {
	q := `SELECT tenant_id, run_id, step_id, due_at FROM run_timers
		WHERE fired_at IS NULL AND due_at <= ?
		ORDER BY due_at LIMIT ?`
	if tx.Dialect() == runstore.DialectPostgres {
		q += ` FOR UPDATE SKIP LOCKED`
	}

	rows, err := tx.Query(ctx, tx.Dialect().Rebind(q),
		now.Format(runstore.TimeFormat), batch)
	if err != nil {
		return nil, fmt.Errorf("wait: claim: %w", err)
	}
	// The whole batch is read and the rows closed before anything else runs
	// on this transaction: both drivers hold one connection per transaction
	// and cannot interleave a second statement with an open result set.
	defer func() { _ = rows.Close() }()

	var claimed []Due
	for rows.Next() {
		var (
			d     Due
			dueAt string
		)
		if err := rows.Scan(&d.TenantID, &d.RunID, &d.StepID, &dueAt); err != nil {
			return nil, fmt.Errorf("wait: claim: %w", err)
		}
		d.At, err = time.Parse(runstore.TimeFormat, dueAt)
		if err != nil {
			return nil, fmt.Errorf("wait: claim %s/%s: unreadable due time %q: %w",
				d.RunID, d.StepID, dueAt, err)
		}
		claimed = append(claimed, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("wait: claim: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("wait: claim: %w", err)
	}
	return claimed, nil
}

// markFired stamps a claimed row. It is what makes a timer fire once however
// long the outage was: the row is gone from the poll, not merely handled.
func markFired(ctx context.Context, tx runstore.Tx, d Due, at time.Time) error {
	const q = `UPDATE run_timers SET fired_at = ?
		WHERE tenant_id = ? AND run_id = ? AND step_id = ? AND fired_at IS NULL`
	if err := tx.Exec(ctx, tx.Dialect().Rebind(q),
		at.Format(runstore.TimeFormat), d.TenantID, d.RunID, d.StepID); err != nil {
		return fmt.Errorf("wait: firing %s/%s: %w", d.RunID, d.StepID, err)
	}
	return nil
}

// retired says whether this timer's run has already ended, or its step already
// has a verdict — the orphan case. It is asked on the poll rather than on the
// path that ends a run, because every one of those paths would have to
// remember to cancel outstanding timers and the one that forgets resurrects a
// finished run days later.
func retired(ctx context.Context, tx runstore.Tx, d Due) (bool, error) {
	const q = `SELECT COUNT(*) FROM run_events
		WHERE tenant_id = ? AND run_id = ?
		AND (type IN (?, ?) OR (step_id = ? AND type IN (?, ?)))`
	return exists(ctx, tx, q,
		d.TenantID, d.RunID,
		string(runstore.RunCompleted), string(scheduler.RunFailed),
		d.StepID,
		string(runstore.StepSucceeded), string(runstore.StepFailed))
}

// resume appends the two events that end the wait: the audit record, and the
// step's own success, which is what lifts the gate the scheduler honours.
func resume(ctx context.Context, tx runstore.Tx, d Due, at time.Time) error {
	payload, err := json.Marshal(Fired{DueAt: d.At, FiredAt: at})
	if err != nil {
		return fmt.Errorf("wait: firing %s/%s: %w", d.RunID, d.StepID, err)
	}
	// A wait step produces no artifacts; the status exists because the
	// scheduler reads a STEP_SUCCEEDED payload as one, and a step that
	// finished with no outputs is exactly what a wait is.
	status, err := proto.Marshal(&dholev1.JobStatus{
		RunId:  d.RunID,
		StepId: d.StepID,
		Phase:  dholev1.Phase_PHASE_SUCCEEDED,
	})
	if err != nil {
		return fmt.Errorf("wait: firing %s/%s: %w", d.RunID, d.StepID, err)
	}

	for _, e := range []runstore.Event{
		{RunID: d.RunID, StepID: d.StepID, Type: StepTimerFired, Payload: payload, At: at},
		{RunID: d.RunID, StepID: d.StepID, Type: runstore.StepSucceeded, Payload: status, At: at},
	} {
		// Sequence stays 0 so the store allocates inside this transaction.
		if err := tx.Append(ctx, d.TenantID, e); err != nil {
			return fmt.Errorf("wait: firing %s/%s: %w", d.RunID, d.StepID, err)
		}
	}
	return nil
}

// exists runs a COUNT(*) query written with `?` placeholders and says whether
// it found anything.
func exists(ctx context.Context, tx runstore.Tx, query string, args ...any) (bool, error) {
	rows, err := tx.Query(ctx, tx.Dialect().Rebind(query), args...)
	if err != nil {
		return false, fmt.Errorf("wait: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var count int64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return false, fmt.Errorf("wait: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("wait: %w", err)
	}
	if err := rows.Close(); err != nil {
		return false, fmt.Errorf("wait: %w", err)
	}
	return count > 0, nil
}
