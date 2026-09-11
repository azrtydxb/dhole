// Package approval is the human gate in a run.
//
// A run waiting for a person is the case ADR 0003 was written for: the person
// is asleep, the deploy that was going to ask them has been restarted twice,
// and the run has to be exactly where they left it when they wake up. So the
// gate is an EVENT, not a blocked goroutine — STEP_AWAITING_APPROVAL sits in
// the log, the scheduler refuses to move past it, and a decision appends the
// events that lift it.
//
// Two rules here are about people rather than about durability:
//
// The approver is recorded, always. An approval whose approver is not in the
// log is not an approval — it is a run that advanced itself — and "who
// approved this" is the only question ever asked of this trail.
//
// A second decision is REFUSED, not swallowed. Approvals get double-clicked,
// and an idempotent no-op would answer "done" to a person clicking DENY on a
// gate someone else had already approved: the two decisions disagree, and the
// second decider would be told theirs took effect. A refusal naming the
// standing decision is the only answer that stays true when the second click
// is not the same as the first, and it gives the interface something to show.
package approval

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// StepApprovalDecided records who decided, and what they decided. It is
// separate from the step's own verdict because the verdict says the run may
// move on and this says a named person said so.
const StepApprovalDecided runstore.EventType = "STEP_APPROVAL_DECIDED"

// The refusals. Each one is a distinct thing a caller can fix, so none of them
// collapses into a generic error.
var (
	// ErrApproverRequired: a decision by nobody.
	ErrApproverRequired = errors.New("approval: an approver is required")

	// ErrUnknownApprover: a subject that is not a principal of this tenant.
	// However plausible the string, it is not a person this system knows.
	ErrUnknownApprover = errors.New("approval: unknown approver")

	// ErrAlreadyDecided: the gate has a standing decision. The error names it.
	ErrAlreadyDecided = errors.New("approval: already decided")

	// ErrNotAwaiting: nobody asked for this approval. Recording it would let
	// a caller mark any step of any run succeeded.
	ErrNotAwaiting = errors.New("approval: no decision was requested")
)

// Request is the payload of a STEP_AWAITING_APPROVAL event: what the person
// is being asked.
type Request struct {
	Prompt string `json:"prompt"`
	// Parks says the gate is one a step is PARKED at rather than one that IS
	// a step — an agent that stopped mid-loop to ask (ADR 0025). It changes
	// what an approval means: an approval step that is approved has
	// succeeded, and a parked step that is approved has been RELEASED and
	// must run again to finish what it was doing. Without this distinction a
	// parked agent was marked succeeded on an answer it never produced, and
	// the run completed carrying the output of a step that had failed.
	Parks bool `json:"parks_step,omitempty"`
}

// Decision is the payload of a STEP_APPROVAL_DECIDED event.
type Decision struct {
	Approver string    `json:"approver"`
	Approved bool      `json:"approved"`
	At       time.Time `json:"at"`
}

// Denial is the payload of the RUN_FAILED event a refusal writes.
//
// Steps is first and named exactly as scheduler.RunFailure names it, so this
// payload reads as an ordinary run failure to the scheduler's own reader as
// well as naming the approver. One payload with two readers beats two formats
// that drift.
type Denial struct {
	Steps    []string `json:"steps"`
	Approver string   `json:"approver"`
	StepID   string   `json:"step_id"`
	Reason   string   `json:"reason"`
}

// Approvers answers "is this subject a principal of this tenant". It is
// identity.Store narrowed to the one question this package asks; *identity.
// SQLStore satisfies it directly.
type Approvers interface {
	PrincipalCredential(ctx context.Context, tenantID, subject string) (identity.StoredPrincipal, error)
}

// Resumer advances a run once its gate is open. *scheduler.Scheduler is the
// implementation.
type Resumer interface {
	Advance(ctx context.Context, tenantID, runID string) error
}

// Config is everything the gate needs. The tenant is bound here rather than
// passed per call because an approval is an act by a principal OF a tenant:
// the same subject in another tenant is a different person.
type Config struct {
	Store     runstore.Store
	TenantID  string
	Approvers Approvers
	Resume    Resumer
	// Now is the clock, injectable for tests.
	Now func() time.Time
}

// Step is the approval step type. It is safe for concurrent use.
type Step struct {
	store     runstore.Store
	tenantID  string
	approvers Approvers
	resume    Resumer
	now       func() time.Time
}

// New validates the configuration and builds a Step.
func New(cfg Config) (*Step, error) {
	switch {
	case cfg.Store == nil:
		return nil, errors.New("approval: a run store is required")
	case cfg.TenantID == "":
		return nil, fmt.Errorf("approval: %w", runstore.ErrTenantRequired)
	case cfg.Approvers == nil:
		return nil, errors.New("approval: a principal store is required; an unverified approver is not an approver")
	case cfg.Resume == nil:
		return nil, errors.New("approval: something has to advance the run once the gate opens")
	}
	s := &Step{
		store:     cfg.Store,
		tenantID:  cfg.TenantID,
		approvers: cfg.Approvers,
		resume:    cfg.Resume,
		now:       cfg.Now,
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// Request opens the gate's question: it appends STEP_AWAITING_APPROVAL, which
// is what stops the scheduler dispatching this step or anything behind it.
//
// Asking twice is the same question, not two: a redelivered command leaves one
// outstanding request. Asking after a decision is refused — the gate is over.
func (s *Step) Request(ctx context.Context, runID, stepID, prompt string) error {
	return s.request(ctx, runID, stepID, prompt, false)
}

// RequestPark opens a gate a step is PARKED at: the step asked mid-flight and
// is waiting to carry on rather than waiting to be finished. See Request.Parks.
//
// There is exactly ONE gate per (run, step), so a step can park once per run.
// A second park in the same run is refused by the standing decision with
// ErrAlreadyDecided, and the step fails saying so — which is a readable limit
// rather than a silent one. Widening it means giving a gate an identity of its
// own, which is a bigger change than this.
func (s *Step) RequestPark(ctx context.Context, runID, stepID, prompt string) error {
	return s.request(ctx, runID, stepID, prompt, true)
}

func (s *Step) request(ctx context.Context, runID, stepID, prompt string, parks bool) error {
	if runID == "" || stepID == "" {
		return errors.New("approval: a run and a step are required")
	}
	payload, err := json.Marshal(Request{Prompt: prompt, Parks: parks})
	if err != nil {
		return fmt.Errorf("approval: request: %w", err)
	}

	return s.store.WithTx(ctx, func(tx runstore.Tx) error {
		gate, err := s.read(ctx, tx, runID, stepID)
		if err != nil {
			return err
		}
		if gate.decided {
			return s.refuseDecided(runID, stepID, gate)
		}
		if gate.awaiting {
			return nil
		}
		if err := tx.Append(ctx, s.tenantID, runstore.Event{
			RunID:  runID,
			StepID: stepID,
			// Sequence 0: the store allocates it inside this transaction.
			// Computing it here would read a high-water mark another
			// caller is about to write, and the loser of that race is
			// discarded silently by the idempotent insert.
			Type:    scheduler.StepAwaitingApproval,
			Payload: payload,
			At:      s.now().UTC(),
		}); err != nil {
			return fmt.Errorf("approval: request %s/%s: %w", runID, stepID, err)
		}
		return nil
	})
}

// Decide records a named person's answer and lifts the gate.
//
// An approval appends the decision and the step's own STEP_SUCCEEDED, and the
// run advances. A denial appends the decision, the step's STEP_FAILED and a
// RUN_FAILED naming the approver: a refusal is a decision, not a pause, and a
// run that stopped for no stated reason is the worst possible record of one.
//
// Both write every event in ONE transaction. A decision recorded without the
// verdict it implies would leave a gate that is decided and still shut.
func (s *Step) Decide(ctx context.Context, runID, stepID, approver string, approved bool) error {
	if runID == "" || stepID == "" {
		return errors.New("approval: a run and a step are required")
	}
	approver = strings.TrimSpace(approver)
	if approver == "" {
		return ErrApproverRequired
	}
	// Verified BEFORE anything is written: an approval whose approver is not
	// a principal of this tenant is not an approval, and a log that records
	// it says something untrue about a person.
	if _, err := s.approvers.PrincipalCredential(ctx, s.tenantID, approver); err != nil {
		if errors.Is(err, identity.ErrNotFound) {
			return fmt.Errorf("%w: %q is not a principal of tenant %q",
				ErrUnknownApprover, approver, s.tenantID)
		}
		return fmt.Errorf("approval: verifying approver %q: %w", approver, err)
	}

	err := s.store.WithTx(ctx, func(tx runstore.Tx) error {
		gate, err := s.read(ctx, tx, runID, stepID)
		if err != nil {
			return err
		}
		if gate.decided {
			return s.refuseDecided(runID, stepID, gate)
		}
		if !gate.awaiting {
			return fmt.Errorf("%w for %s/%s", ErrNotAwaiting, runID, stepID)
		}
		events, err := s.decision(runID, stepID, approver, approved, gate.parks)
		if err != nil {
			return err
		}
		for _, e := range events {
			// Sequence stays 0: the store allocates inside this transaction.
			if err := tx.Append(ctx, s.tenantID, e); err != nil {
				return fmt.Errorf("approval: deciding %s/%s: %w", runID, stepID, err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !approved {
		// The run is over; there is nothing left to advance.
		return nil
	}
	if err := s.resume.Advance(ctx, s.tenantID, runID); err != nil {
		return fmt.Errorf("approval: resuming %s after %s approved %s: %w",
			runID, approver, stepID, err)
	}
	return nil
}

// decision is the event sequence a verdict writes.
//
// An approved PARKED gate writes no verdict at all. The step is not finished —
// it is released, and it has to run again to finish the work it stopped in the
// middle of. STEP_RESUMED is what says so: the scheduler folds it as "this
// attempt is out of flight and the step may be dispatched again", and the
// step's own verdict comes from that next attempt. Writing STEP_SUCCEEDED here
// instead reported an agent's answer that no model had given.
func (s *Step) decision(
	runID, stepID, approver string, approved, parks bool,
) ([]runstore.Event, error) {
	at := s.now().UTC()
	decision, err := json.Marshal(Decision{Approver: approver, Approved: approved, At: at})
	if err != nil {
		return nil, fmt.Errorf("approval: deciding %s/%s: %w", runID, stepID, err)
	}
	events := []runstore.Event{{
		RunID: runID, StepID: stepID, Type: StepApprovalDecided, Payload: decision, At: at,
	}}

	if approved && parks {
		return append(events, runstore.Event{
			RunID: runID, StepID: stepID, Type: scheduler.StepResumed,
			Payload: decision, At: at,
		}), nil
	}

	if approved {
		// An approval gate produces no artifacts; the status exists because
		// the scheduler reads a STEP_SUCCEEDED payload as one.
		status, err := proto.Marshal(&dholev1.JobStatus{
			RunId:  runID,
			StepId: stepID,
			Phase:  dholev1.Phase_PHASE_SUCCEEDED,
		})
		if err != nil {
			return nil, fmt.Errorf("approval: deciding %s/%s: %w", runID, stepID, err)
		}
		return append(events, runstore.Event{
			RunID: runID, StepID: stepID, Type: runstore.StepSucceeded, Payload: status, At: at,
		}), nil
	}

	denial, err := json.Marshal(Denial{
		Steps:    []string{stepID},
		Approver: approver,
		StepID:   stepID,
		Reason:   fmt.Sprintf("approval of %q denied by %s", stepID, approver),
	})
	if err != nil {
		return nil, fmt.Errorf("approval: deciding %s/%s: %w", runID, stepID, err)
	}
	return append(events,
		runstore.Event{
			RunID: runID, StepID: stepID, Type: runstore.StepFailed, Payload: decision, At: at,
		},
		runstore.Event{RunID: runID, Type: scheduler.RunFailed, Payload: denial, At: at},
	), nil
}

// refuseDecided is the double-click answer: what was decided, and by whom.
func (s *Step) refuseDecided(runID, stepID string, gate gateState) error {
	verdict := "denied"
	if gate.approved {
		verdict = "approved"
	}
	return fmt.Errorf("%w: %s/%s was %s by %s", ErrAlreadyDecided, runID, stepID, verdict, gate.approver)
}

// gateState is what the log says about one gate.
type gateState struct {
	awaiting bool
	decided  bool
	approved bool
	approver string
	// parks is Request.Parks, read back off the standing request: what an
	// approval MEANS depends on it, and the request is the only place it was
	// ever written down.
	parks bool
}

// read folds this gate's events. It reads through the transaction rather than
// through the store: on SQLite the store holds a single connection, and
// replaying through it while this transaction holds it would deadlock against
// itself.
func (s *Step) read(ctx context.Context, tx runstore.Tx, runID, stepID string) (gateState, error) {
	const q = `SELECT type, payload FROM run_events
		WHERE tenant_id = ? AND run_id = ? AND step_id = ? AND type IN (?, ?)
		ORDER BY sequence`
	rows, err := tx.Query(ctx, tx.Dialect().Rebind(q),
		s.tenantID, runID, stepID,
		string(scheduler.StepAwaitingApproval), string(StepApprovalDecided))
	if err != nil {
		return gateState{}, fmt.Errorf("approval: reading %s/%s: %w", runID, stepID, err)
	}
	defer func() { _ = rows.Close() }()

	var state gateState
	for rows.Next() {
		var (
			kind    string
			payload []byte
		)
		if err := rows.Scan(&kind, &payload); err != nil {
			return gateState{}, fmt.Errorf("approval: reading %s/%s: %w", runID, stepID, err)
		}
		switch runstore.EventType(kind) {
		case scheduler.StepAwaitingApproval:
			state.awaiting = true
			request, err := UnmarshalRequest(payload)
			if err != nil {
				return gateState{}, err
			}
			state.parks = request.Parks
		case StepApprovalDecided:
			decision, err := UnmarshalDecision(payload)
			if err != nil {
				return gateState{}, err
			}
			state.decided = true
			state.approved = decision.Approved
			state.approver = decision.Approver
		}
	}
	if err := rows.Err(); err != nil {
		return gateState{}, fmt.Errorf("approval: reading %s/%s: %w", runID, stepID, err)
	}
	if err := rows.Close(); err != nil {
		return gateState{}, fmt.Errorf("approval: reading %s/%s: %w", runID, stepID, err)
	}
	return state, nil
}

// UnmarshalRequest decodes a STEP_AWAITING_APPROVAL payload.
func UnmarshalRequest(b []byte) (Request, error) {
	return decode[Request](b, string(scheduler.StepAwaitingApproval))
}

// UnmarshalDecision decodes a STEP_APPROVAL_DECIDED payload.
func UnmarshalDecision(b []byte) (Decision, error) {
	return decode[Decision](b, string(StepApprovalDecided))
}

// UnmarshalDenial decodes the RUN_FAILED payload a refusal writes.
func UnmarshalDenial(b []byte) (Denial, error) {
	return decode[Denial](b, string(scheduler.RunFailed))
}

func decode[T any](b []byte, kind string) (T, error) {
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return v, fmt.Errorf("approval: decoding %s payload: %w", kind, err)
	}
	return v, nil
}
