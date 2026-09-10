package scheduler

import (
	"context"
	"fmt"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// Gate arms a step that WAITS instead of running, in a transaction this
// package hands it.
//
// It is the seam that made arming atomic with the readiness decision. Arming
// used to be somebody else's transaction — whoever started the run scheduled
// the wait, the scheduler decided readiness — and a sequence is allocated
// inside a transaction while its VISIBILITY is not, so a gate could hold a
// lower sequence than the STEP_DISPATCHED of the step it gated. The log read
// "gated, then dispatched anyway" and the wait had been skipped entirely; both
// acceptance pipelines armed their gate behind a five-second predecessor to
// make the window improbable rather than closed.
//
// Two things about the shape are the fix, not the plumbing.
//
// Arm is asked about the STEP, from the pinned definition, and not about the
// run's log. Whether a step waits was decided when the revision was authored,
// so there is no commit that can arrive too late to be seen.
//
// It is given the transaction rather than the store. The gate event, its timer
// row, and the decision that the step was ready are then one act: a plane that
// dies between any two of them has done none of them.
//
// *gate.Gate satisfies it. The interface lives here so that internal/steps/gate
// can depend on this package's event vocabulary — as internal/wait already
// does — without this package depending back on it.
type Gate interface {
	// Arm records that the step waits and reports whether it is a gate at
	// all. A step that is not a gate must be answered false with nothing
	// written, because the caller then dispatches it exactly as before.
	Arm(ctx context.Context, tx runstore.Tx, tenantID, runID string, step *dholev1.Step) (bool, error)
}

// armGate diverts a ready step that waits, and reports whether it did.
//
// The transaction is opened HERE rather than joined to the dispatch's own,
// because a gate is the negation of a dispatch: there is no lease to claim, no
// engine to match, no outbox row to enqueue, and nothing about the step goes
// anywhere near the bus. Sharing the dispatch's transaction would mean
// building all of that first in order to throw it away.
//
// Two advances that both find one gate ready both arm it, and that is
// deliberate: migration 0023 makes a step's STEP_AWAITING_TIMER unique, so the
// second append is the same no-op a redelivered event already is, and the
// loser needs to learn nothing — the wait is armed either way. That is the
// shape migrations 0020 and 0021 settled on for the run's terminal event and
// the two step verdicts.
func (s *Scheduler) armGate(
	ctx context.Context, tenantID, runID string, step *dholev1.Step,
) (bool, error) {
	if s.gate == nil {
		return false, nil
	}
	var armed bool
	err := s.store.WithTx(ctx, func(tx runstore.Tx) error {
		var err error
		armed, err = s.gate.Arm(ctx, tx, tenantID, runID, step)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("scheduler: arming the gate on %s/%s: %w",
			runID, step.GetId(), err)
	}
	return armed, nil
}
