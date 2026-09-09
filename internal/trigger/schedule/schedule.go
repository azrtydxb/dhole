// Package schedule is the trigger that fires on a cron expression.
//
// It is the simplest event source there is, and the one that exposes every
// hard problem in the subsystem, so the shape here is the shape the others
// follow (ADR 0007).
//
// Three rules are load-bearing, and each of them is a production incident
// somewhere else:
//
// A schedule is a ROW holding the NEXT due time, not a queue of intervals.
// Coming back after three hours of downtime fires the missed window once,
// because firing advances the due time past everything already elapsed. The
// catch-up loop this refuses is how a minutely job runs 180 times at once at
// the exact moment a system is least able to take it.
//
// An occurrence is CLAIMED before it is fired, under the same discipline the
// wait timers and the outbox use: FOR UPDATE SKIP LOCKED on Postgres, the
// immediate write lock on SQLite, with the due time advanced inside the claim.
// Two control planes poll the same row every second, and an occurrence handed
// to both starts the pipeline twice.
//
// An occurrence that arrives while the last one is still in flight is SKIPPED,
// with a reason, and the schedule moves on. A queue that grows without bound
// behind a slow run is the other way a minutely schedule takes a system down.
//
// # Why not the run timers
//
// The durable timer store in internal/wait solves the missed-window and
// two-plane problems for a run that is already waiting, and this package
// reuses its claim discipline deliberately. It cannot reuse its API. A wait is
// keyed by (tenant, run, step) and fires by appending that step's
// STEP_TIMER_FIRED and STEP_SUCCEEDED — but a schedule has no run and no step
// until it fires, its key must be re-armable (a wait's, once fired, never is,
// which is exactly right for a wait and fatal for a recurrence), and its poll
// must not claim rows belonging to runs: wait.Timers.Due claims and resumes
// EVERY due row, so a second consumer of it would swallow other runs' waits.
package schedule

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/trigger"
)

// Kind is what this trigger reports itself as.
const Kind = "schedule"

// DefaultPollInterval is how often a schedule looks at its row. A cron
// boundary is therefore served to about a second, which is the trade the table
// buys: a schedule that survives a restart is worth more than one that fires
// to the millisecond and is lost on the next deploy.
const DefaultPollInterval = time.Second

// DefaultActiveLease is how long an in-flight fire is believed. Past it the
// mark is treated as stale, because the plane that set it may have died mid
// fire and a schedule that never fires again is worse than one that overlaps.
const DefaultActiveLease = time.Hour

// The fields of a schedule's event, which is all a binding may draw on. They
// are checked at configuration time: a binding that reads `commit_sha` from a
// cron trigger is a mistake to catch while someone is wiring it up.
const (
	FieldScheduledFor = "scheduled_for"
	FieldFiredAt      = "fired_at"
	FieldExpression   = "expression"
	FieldTriggerID    = "trigger_id"
	FieldKind         = "kind"
)

// parser accepts both the five-field cron expression everyone knows and the
// six-field form with seconds, plus the @-descriptors.
var parser = cron.NewParser(
	cron.SecondOptional | cron.Minute | cron.Hour |
		cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// Skip is one occurrence that was passed over rather than fired.
type Skip struct {
	TenantID     string
	TriggerID    string
	ScheduledFor time.Time
	Reason       string
}

// Outcome is what one poll did. Exactly one of Fired and Skipped is true, or
// neither when nothing was due.
type Outcome struct {
	Fired        bool
	Skipped      bool
	ScheduledFor time.Time
	Reason       string
}

// Config configures a schedule trigger. Everything it can refuse, it refuses
// in New: an unparseable expression, an unscoped trigger, and a binding that
// does not match the pipeline's declared inputs.
type Config struct {
	// ID is the schedule's identity within the tenant, and the key of its
	// row. Two processes running the same ID are two control planes running
	// one schedule, which is the point.
	ID       string
	TenantID string
	// Expression is a cron expression, five or six fields.
	Expression string
	Binding    trigger.Binding
	// Pipeline is the definition the binding is checked against. It is
	// required: a binding nobody checked is an untyped bag that fails deep
	// inside a step (ADR 0007).
	Pipeline *dholev1.Pipeline
	Store    runstore.Store

	// PollInterval defaults to DefaultPollInterval.
	PollInterval time.Duration
	// ActiveLease defaults to DefaultActiveLease.
	ActiveLease time.Duration
	// OnSkip is called with every occurrence passed over. The skip is
	// recorded on the row either way; this is how it reaches a log.
	OnSkip func(Skip)
	// OnError is called with every error a poll swallows. Without it a
	// database that has been unreachable for an hour is invisible.
	OnError func(error)
}

// Schedule is one configured cron trigger. It is safe for concurrent use and
// safe to run in several control planes at once.
type Schedule struct {
	id         string
	tenantID   string
	expression string
	binding    trigger.Binding
	spec       cron.Schedule
	store      runstore.Store
	poll       time.Duration
	lease      time.Duration
	onSkip     func(Skip)
	onError    func(error)
}

// Compile-time proof that a schedule is a trigger.
var _ trigger.Trigger = (*Schedule)(nil)

// New validates cfg and returns the trigger it describes.
func New(cfg Config) (*Schedule, error) {
	if cfg.TenantID == "" {
		return nil, fmt.Errorf("schedule %q: %w", cfg.ID, trigger.ErrTenantRequired)
	}
	if cfg.ID == "" {
		return nil, errors.New("schedule: an id is required; it is the key of the row two planes share")
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("schedule %q: a store is required", cfg.ID)
	}
	if err := trigger.ValidateBinding(cfg.Pipeline, cfg.Binding); err != nil {
		return nil, fmt.Errorf("schedule %q: %w", cfg.ID, err)
	}
	if err := validateSources(cfg.Binding); err != nil {
		return nil, fmt.Errorf("schedule %q: %w", cfg.ID, err)
	}
	spec, err := parser.Parse(cfg.Expression)
	if err != nil {
		return nil, fmt.Errorf("schedule %q: cron expression %q is not valid: %w",
			cfg.ID, cfg.Expression, err)
	}

	s := &Schedule{
		id:         cfg.ID,
		tenantID:   cfg.TenantID,
		expression: cfg.Expression,
		binding:    cfg.Binding,
		spec:       spec,
		store:      cfg.Store,
		poll:       cfg.PollInterval,
		lease:      cfg.ActiveLease,
		onSkip:     cfg.OnSkip,
		onError:    cfg.OnError,
	}
	if s.poll <= 0 {
		s.poll = DefaultPollInterval
	}
	if s.lease <= 0 {
		s.lease = DefaultActiveLease
	}
	return s, nil
}

// validateSources rejects a binding that draws on an event field a schedule
// has no way to fill.
func validateSources(b trigger.Binding) error {
	for input, field := range b.InputMapping {
		if _, ok := eventFields("", "", time.Time{}, time.Time{})[field]; !ok {
			return fmt.Errorf(
				"binding fills input %q from %q, which a schedule event does not carry (it carries: %s)",
				input, field, strings.Join(fieldNames(), ", "))
		}
	}
	return nil
}

// Kind implements trigger.Trigger.
func (s *Schedule) Kind() string { return Kind }

// Start polls until ctx is done and returns ctx's error. It owns nothing but
// its ticker, and stops that on the way out: everything else it needs is in
// the row.
func (s *Schedule) Start(ctx context.Context, sink trigger.Sink) error {
	if sink == nil {
		return fmt.Errorf("schedule %q: a sink is required", s.id)
	}
	ticker := time.NewTicker(s.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := s.Tick(ctx, sink, time.Now().UTC()); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if s.onError != nil {
					s.onError(err)
				}
			}
		}
	}
}

// Tick serves whatever is due at now, and is the whole trigger: Start is a
// ticker around it.
//
// Time is a parameter rather than a reading of the clock so that a test can be
// three hours into an outage exactly, instead of sleeping past a boundary
// approximately. A generous sleep hides a schedule that never fires.
func (s *Schedule) Tick(ctx context.Context, sink trigger.Sink, now time.Time) (Outcome, error) {
	if sink == nil {
		return Outcome{}, fmt.Errorf("schedule %q: a sink is required", s.id)
	}
	now = now.UTC()

	c, err := s.claim(ctx, now)
	if err != nil {
		return Outcome{}, err
	}
	switch {
	case c.skipped:
		if s.onSkip != nil {
			s.onSkip(Skip{
				TenantID:     s.tenantID,
				TriggerID:    s.id,
				ScheduledFor: c.occurrence,
				Reason:       c.reason,
			})
		}
		return Outcome{Skipped: true, ScheduledFor: c.occurrence, Reason: c.reason}, nil
	case !c.claimed:
		return Outcome{}, nil
	}

	// The release is not the caller's context's to cancel: a shutdown between
	// the fire and the release would leave the schedule marked in-flight
	// until its lease ran out.
	defer func() { _ = s.release(context.WithoutCancel(ctx)) }()

	inputs, err := s.inputs(c.occurrence, now)
	if err != nil {
		return Outcome{ScheduledFor: c.occurrence}, err
	}
	if err := sink.Fire(ctx, s.tenantID, s.binding.PipelineID, inputs); err != nil {
		return Outcome{ScheduledFor: c.occurrence}, fmt.Errorf(
			"schedule %q: firing %s: %w", s.id, c.occurrence.Format(time.RFC3339), err)
	}
	return Outcome{Fired: true, ScheduledFor: c.occurrence}, nil
}

// inputs turns one occurrence into the pipeline's declared inputs. This is the
// whole of ADR 0007 in one function: the pipeline receives values on its own
// ports and never learns that a clock produced them.
func (s *Schedule) inputs(occurrence, now time.Time) (map[string]*structpb.Value, error) {
	fields := eventFields(s.id, s.expression, occurrence, now)
	out := make(map[string]*structpb.Value, len(s.binding.InputMapping))
	for input, field := range s.binding.InputMapping {
		v, ok := fields[field]
		if !ok {
			return nil, fmt.Errorf("schedule %q: input %q reads unknown event field %q",
				s.id, input, field)
		}
		out[input] = structpb.NewStringValue(v)
	}
	return out, nil
}

func eventFields(id, expression string, occurrence, now time.Time) map[string]string {
	return map[string]string{
		FieldScheduledFor: occurrence.UTC().Format(time.RFC3339Nano),
		FieldFiredAt:      now.UTC().Format(time.RFC3339Nano),
		FieldExpression:   expression,
		FieldTriggerID:    id,
		FieldKind:         Kind,
	}
}

func fieldNames() []string {
	out := make([]string, 0, 5)
	for name := range eventFields("", "", time.Time{}, time.Time{}) {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// claimed is the result of one look at the row.
type claimed struct {
	claimed    bool
	skipped    bool
	occurrence time.Time
	reason     string
}

// claim takes the due occurrence, if there is one, and advances the row past
// it in the SAME transaction. Advancing inside the claim is what makes the
// loser of a race see a schedule that is not due rather than a second copy of
// the occurrence — and what makes a missed window fire once, since the next
// due time is computed from NOW and not from the boundary that was missed.
func (s *Schedule) claim(ctx context.Context, now time.Time) (claimed, error) {
	var res claimed
	err := s.store.WithTx(ctx, func(tx runstore.Tx) error {
		res = claimed{}
		due, active, found, err := s.row(ctx, tx)
		if err != nil {
			return err
		}
		if !found {
			// Either the schedule is new, or another plane holds its row and
			// Postgres skipped it. Both are "nothing to do here"; the insert
			// is a no-op in the second case.
			return s.seed(ctx, tx, s.spec.Next(now))
		}
		if due.After(now) {
			return nil
		}

		// Everything already elapsed is one occurrence, not many.
		next := s.spec.Next(now)
		if !active.IsZero() && now.Sub(active) < s.lease {
			res.skipped = true
			res.occurrence = due
			res.reason = fmt.Sprintf(
				"the run started at %s is still in flight and the concurrency budget is 1",
				active.Format(time.RFC3339))
			return s.skip(ctx, tx, next, res.reason, now)
		}
		res.claimed = true
		res.occurrence = due
		return s.take(ctx, tx, next, now)
	})
	if err != nil {
		return claimed{}, err
	}
	return res, nil
}

// row reads this schedule's row, holding it for the life of tx.
func (s *Schedule) row(ctx context.Context, tx runstore.Tx) (due, active time.Time, found bool, err error) {
	q := `SELECT due_at, active_at FROM trigger_schedules
		WHERE tenant_id = ? AND trigger_id = ?`
	if tx.Dialect() == runstore.DialectPostgres {
		q += ` FOR UPDATE SKIP LOCKED`
	}
	rows, err := tx.Query(ctx, tx.Dialect().Rebind(q), s.tenantID, s.id)
	if err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("schedule %q: claim: %w", s.id, err)
	}
	// Both drivers hold one connection per transaction and cannot interleave
	// a second statement with an open result set, so the row is read and the
	// result closed before anything else runs on tx.
	defer func() { _ = rows.Close() }()

	var (
		dueAt    string
		activeAt sql.NullString
	)
	if rows.Next() {
		if err := rows.Scan(&dueAt, &activeAt); err != nil {
			return time.Time{}, time.Time{}, false, fmt.Errorf("schedule %q: claim: %w", s.id, err)
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("schedule %q: claim: %w", s.id, err)
	}
	if err := rows.Close(); err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("schedule %q: claim: %w", s.id, err)
	}
	if !found {
		return time.Time{}, time.Time{}, false, nil
	}
	if due, err = parseTime(dueAt); err != nil {
		return time.Time{}, time.Time{}, false, fmt.Errorf("schedule %q: unreadable due time: %w", s.id, err)
	}
	if activeAt.Valid && activeAt.String != "" {
		if active, err = parseTime(activeAt.String); err != nil {
			return time.Time{}, time.Time{}, false,
				fmt.Errorf("schedule %q: unreadable in-flight mark: %w", s.id, err)
		}
	}
	return due, active, true, nil
}

// seed records a schedule's first due time. ON CONFLICT DO NOTHING because
// several planes start at once and the first one to arrive decides.
func (s *Schedule) seed(ctx context.Context, tx runstore.Tx, due time.Time) error {
	const q = `INSERT INTO trigger_schedules (tenant_id, trigger_id, due_at)
		VALUES (?, ?, ?) ON CONFLICT DO NOTHING`
	if err := tx.Exec(ctx, tx.Dialect().Rebind(q),
		s.tenantID, s.id, due.UTC().Format(runstore.TimeFormat)); err != nil {
		return fmt.Errorf("schedule %q: arming: %w", s.id, err)
	}
	return nil
}

// take moves the row to its next due time and marks the fire in flight, in the
// same statement and the same transaction as the claim.
func (s *Schedule) take(ctx context.Context, tx runstore.Tx, next, now time.Time) error {
	const q = `UPDATE trigger_schedules SET due_at = ?, active_at = ?
		WHERE tenant_id = ? AND trigger_id = ?`
	if err := tx.Exec(ctx, tx.Dialect().Rebind(q),
		next.UTC().Format(runstore.TimeFormat), now.UTC().Format(runstore.TimeFormat),
		s.tenantID, s.id); err != nil {
		return fmt.Errorf("schedule %q: claiming: %w", s.id, err)
	}
	return nil
}

// skip passes over an occurrence and records why. It moves the due time on —
// that is what keeps a slow run from accumulating a queue behind it — and
// deliberately does NOT touch active_at: the run that caused the skip is still
// in flight, and clearing its mark here would let the NEXT occurrence through,
// which is the concurrency budget leaking one occurrence at a time.
func (s *Schedule) skip(
	ctx context.Context, tx runstore.Tx, next time.Time, reason string, now time.Time,
) error {
	const q = `UPDATE trigger_schedules SET due_at = ?, skipped_at = ?, skipped_reason = ?
		WHERE tenant_id = ? AND trigger_id = ?`
	if err := tx.Exec(ctx, tx.Dialect().Rebind(q),
		next.UTC().Format(runstore.TimeFormat), now.UTC().Format(runstore.TimeFormat), reason,
		s.tenantID, s.id); err != nil {
		return fmt.Errorf("schedule %q: recording a skip: %w", s.id, err)
	}
	return nil
}

// release clears the in-flight mark once the fire has returned.
func (s *Schedule) release(ctx context.Context) error {
	return s.store.WithTx(ctx, func(tx runstore.Tx) error {
		const q = `UPDATE trigger_schedules SET active_at = NULL
			WHERE tenant_id = ? AND trigger_id = ?`
		if err := tx.Exec(ctx, tx.Dialect().Rebind(q), s.tenantID, s.id); err != nil {
			return fmt.Errorf("schedule %q: releasing: %w", s.id, err)
		}
		return nil
	})
}

func parseTime(v string) (time.Time, error) {
	t, err := time.Parse(runstore.TimeFormat, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("unreadable time %q: %w", v, err)
	}
	return t.UTC(), nil
}
