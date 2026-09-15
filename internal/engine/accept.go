package engine

import (
	"context"
	"log/slog"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
)

// acceptTimeout bounds one acceptance request. The plane answers from one
// lease read and one write, so a healthy plane is milliseconds; the bound is
// there for a plane that is up but cannot reach its leases.
const acceptTimeout = 5 * time.Second

// acceptRetry is how long an at-most-once dispatch nobody confirmed stays out
// of the queue before it is redelivered and asked about again.
const acceptRetry = time.Second

// verdict is what confirm decided about one dispatch.
type verdict int

const (
	// start: run the step.
	start verdict = iota
	// superseded: the plane says a newer attempt holds the step. Never start
	// it; acknowledge it off the queue.
	superseded
	// stopping: the engine is shutting down before an at-most-once step was
	// confirmed. It never started; give the delivery back.
	stopping
	// unconfirmed: an at-most-once step nobody answered for. It never started;
	// give it back to the queue, after a delay, with its slot (ADR 0031).
	unconfirmed
)

// confirm asks the control plane whether this dispatch may still start
// (docs/wire-contract.md, "Fence tokens"; ADR 0029).
//
// It exists because the fence used to be enforced only on the way back. A
// dispatch that waited in the queue while its attempt was declared lost and
// re-dispatched — or that was redelivered after the engine that accepted it
// died — was started anyway, and only its result was discarded. On kw that was
// a sandbox running `build` for nobody for five minutes; for an at-most-once
// step it is the side effect happening twice.
//
// Only a plane that set confirm_acceptance is asked. An older plane serves
// nothing on job.accept.*, and its engine credentials may not permit
// publishing there at all, in which case every request would cost the whole
// timeout.
//
// No answer — no plane serving, a timeout, a plane that could not decide —
// starts a pure or idempotent step, whose stale result the fence still
// discards, and does NOT start an at-most-once step: its effect cannot be
// discarded afterwards, and it is the class required to hold its lease before
// it executes (ADR 0002). That dispatch goes back to the queue with its slot
// (unconfirmed, ADR 0031). It used to keep asking here, once a second, holding
// the slot and the engine's room to fetch for as long as nobody answered — a
// plane restarting, partitioned, or replaced in an upgrade — and a one-slot
// engine ran nothing at all meanwhile, pure work included.
//
// It is asked about only while it holds a slot, here and on every redelivery.
// A CURRENT answer accepts the lease, and a lease accepted by a dispatch that
// then waits for a slot, listed in no heartbeat, is declared lost one TTL later.
func (a *Agent) confirm(ctx context.Context, d *dholev1.JobDispatch) verdict {
	if !d.GetConfirmAcceptance() {
		return start
	}
	answer, err := a.ask(ctx, d)
	switch {
	case err == nil && answer == dholev1.Acceptance_ACCEPTANCE_FENCED:
		return superseded
	case err == nil && answer == dholev1.Acceptance_ACCEPTANCE_CURRENT:
		return start
	case ctx.Err() != nil:
		return stopping
	case d.GetStep().GetEffectClass() != dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE:
		slog.Warn("starting a step the control plane did not confirm",
			"engine", a.cfg.EngineID, "run", d.GetRunId(), "step", d.GetStepId(),
			"attempt", d.GetAttempt(), "acceptance", answer, "error", err)
		return start
	}
	slog.Warn("an at-most-once step nobody confirmed goes back to the queue",
		"engine", a.cfg.EngineID, "run", d.GetRunId(), "step", d.GetStepId(),
		"attempt", d.GetAttempt(), "acceptance", answer, "error", err, "retry_in", acceptRetry)
	return unconfirmed
}

// ask sends one acceptance request: the ACCEPTED status this engine is about to
// publish, fence echoed unchanged.
func (a *Agent) ask(ctx context.Context, d *dholev1.JobDispatch) (dholev1.Acceptance, error) {
	ctx, cancel := context.WithTimeout(ctx, acceptTimeout)
	defer cancel()
	reply := &dholev1.AcceptReply{}
	if err := a.cfg.Bus.Request(ctx, bus.SubjectAccept(d.GetRunId(), d.GetStepId()),
		a.phase(d, dholev1.Phase_PHASE_ACCEPTED), reply); err != nil {
		return dholev1.Acceptance_ACCEPTANCE_UNSPECIFIED, err
	}
	return reply.GetAcceptance(), nil
}
