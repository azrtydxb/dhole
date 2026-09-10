package scheduler

import (
	"context"
	"fmt"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/tenancy"
)

// This file is the join between plan() and dispatch(): what may go out now,
// how much of it, and whose.
//
// Every piece of it existed and none of it was connected. Queue and Budgets
// were built and unit-tested with no caller, and so was tenancy's Enforcer, so
// a fleet ran with no fair share between tenants, no per-pipeline cap and no
// tenant limit — three mechanisms that pass their own tests and govern
// nothing. The tests for this file therefore all run through Advance: a
// component's own unit test cannot fail when nothing calls it.
//
// One rule runs through all three checks. A step held back has to leave a
// STEP_UNSCHEDULABLE behind saying why. A step that is never dispatched, never
// fails and never explains itself is the worst failure a scheduler has: the
// run looks slow instead of stuck, and there is nothing to look at.

// Budget is the per-pipeline concurrency cap, narrowed to what the dispatch
// path asks of it. *Budgets satisfies it; so does a fake in a test, which is
// the point — an admission decision must be testable without a bus.
type Budget interface {
	// Acquire takes one in-flight slot, or reports that the pipeline is full.
	// The returned release is always non-nil and always safe to call.
	Acquire(ctx context.Context, tenantID, pipelineID string) (func(), bool)
	// InFlight is how many of this tenant's steps hold a slot, across the
	// whole fleet rather than in this plane.
	InFlight(ctx context.Context, tenantID string) (int, error)
}

var _ Budget = (*Budgets)(nil)

// Quotas is the tenant admission check, narrowed to the one question the
// dispatch path asks. *tenancy.Enforcer satisfies it.
//
// The dependency runs scheduler -> tenancy and never back: quotas are enforced
// where work is dispatched, so tenancy cannot import this package and mirrors
// the two persistence contracts it needs instead (see
// TestMirroredSchedulerContractsHaveNotDrifted).
type Quotas interface {
	AdmitStep(ctx context.Context, tenantID string, inFlight int) (tenancy.Decision, error)
}

// enqueueReady puts a ready step in its tenant's queue, once.
//
// The dedupe is not decoration. A step that is ready and not drained this pass
// — the fleet had no room, or its pipeline was at its budget — is still ready
// on the next pass, and an Advance that enqueued it again would put the same
// step in the rotation twice, take two slots for it and charge its budget
// twice. The set is cleared when the item is drained, so a step that comes
// back round after a failure is enqueued again as the new attempt it is.
func (s *Scheduler) enqueueReady(ctx context.Context, item QueueItem) error {
	key := stepKey(item.TenantID, item.RunID, item.StepID)

	s.pendingMu.Lock()
	if s.pending[key] {
		s.pendingMu.Unlock()
		return nil
	}
	s.pending[key] = true
	s.pendingMu.Unlock()

	if err := s.queue.Enqueue(ctx, item); err != nil {
		s.pendingMu.Lock()
		delete(s.pending, key)
		s.pendingMu.Unlock()
		return fmt.Errorf("scheduler: queueing %s/%s: %w", item.RunID, item.StepID, err)
	}
	return nil
}

// drain takes what the fleet has room for and dispatches it.
//
// The slots asked for are the CAPACITY the tenant's ready engines advertise,
// not a live free count: nothing in the registry reports how many of an
// engine's slots are occupied right now, and the budget plus the engine's own
// refusal are what stop an over-subscription becoming a lost step. It is a
// rate, in other words, and not a reservation.
//
// A fleet advertising nothing is still drained, for one slot. The step will
// not be placed — Match refuses every instance — but it reaches dispatch,
// which records the reason Explain gives for it. Returning early instead would
// leave the queue holding a step nobody had said anything about, which is the
// silence this whole file exists to break.
func (s *Scheduler) drain(ctx context.Context, tenantID string) error {
	slots, err := s.capacity(ctx, tenantID)
	if err != nil {
		return err
	}
	if slots <= 0 {
		slots = 1
	}
	items, err := s.queue.Next(ctx, slots)
	if err != nil {
		return fmt.Errorf("scheduler: draining the queue: %w", err)
	}
	for _, item := range items {
		s.pendingMu.Lock()
		delete(s.pending, stepKey(item.TenantID, item.RunID, item.StepID))
		s.pendingMu.Unlock()

		if err := s.dispatchQueued(ctx, item); err != nil {
			return err
		}
	}
	return nil
}

// capacity is how many jobs the tenant's ready engines say they will run at
// once.
func (s *Scheduler) capacity(ctx context.Context, tenantID string) (int, error) {
	instances, err := s.fleet.Instances(ctx, tenantID)
	if err != nil {
		return 0, fmt.Errorf("scheduler: listing engines for %s: %w", tenantID, err)
	}
	total := 0
	for _, e := range instances {
		if e.State != registry.StateReady || e.Slots <= 0 {
			continue
		}
		total += e.Slots
	}
	return total, nil
}

// dispatchQueued places one item the queue handed back.
//
// The run is re-read rather than carried on the item, because the queue is
// ordered by tenant SHARE: an item drained during one run's Advance routinely
// belongs to another run, and often to another tenant. Nothing about the
// dispatch is taken on trust from the item — the log is still the authority on
// whether the step is still ready at all.
func (s *Scheduler) dispatchQueued(ctx context.Context, item QueueItem) error {
	state, err := s.load(ctx, item.TenantID, item.RunID)
	if err != nil {
		return err
	}
	switch {
	case state.completed:
		return nil
	case state.attempts[item.StepID] >= item.Attempt:
		// Somebody dispatched this attempt between the enqueue and the drain.
		return nil
	case state.terminal[item.StepID] == "" && state.gated[item.StepID]:
		return nil
	}
	pipeline, err := s.defs.Get(ctx, item.TenantID, state.pipelineID, state.revisionID)
	if err != nil {
		return fmt.Errorf("scheduler: run %q pins revision %q: %w", item.RunID, state.revisionID, err)
	}
	step := stepByID(pipeline, item.StepID)
	if step == nil {
		// The revision a run pinned cannot lose a step, so this is a queue
		// item for a run whose definition moved underneath it. Dropping it is
		// right: there is nothing to dispatch.
		return nil
	}
	return s.dispatch(ctx, item.TenantID, item.RunID, pipeline, step, state)
}

// admit is the two limits that stand between a ready step and its lease: the
// tenant's fleet-wide quota, and its pipeline's concurrency budget.
//
// It returns the release for the slot it took. On a refusal it returns
// (nil, false) having already recorded WHY on the run's log.
//
// Both checks happen before the lease is claimed. A claim supersedes the
// previous holder's fence, so claiming for a step that is then refused would
// fence out an attempt that is still running, to dispatch nothing.
func (s *Scheduler) admit(
	ctx context.Context, tenantID, runID, pipelineID string, step *dholev1.Step, state *runState,
) (func(), bool, error) {
	if s.quotas != nil {
		inFlight, err := s.budgets.InFlight(ctx, tenantID)
		if err != nil {
			return nil, false, fmt.Errorf("scheduler: counting in-flight steps for %s: %w", tenantID, err)
		}
		decision, err := s.quotas.AdmitStep(ctx, tenantID, inFlight)
		if err != nil {
			return nil, false, fmt.Errorf("scheduler: admitting %s/%s: %w", runID, step.GetId(), err)
		}
		if !decision.Allowed {
			return nil, false, s.recordUnschedulable(
				ctx, tenantID, runID, step.GetId(), decision.Reason, state)
		}
	}
	if s.budgets == nil {
		return func() {}, true, nil
	}
	release, ok := s.budgets.Acquire(ctx, tenantID, pipelineID)
	if !ok {
		return nil, false, s.recordUnschedulable(ctx, tenantID, runID, step.GetId(),
			fmt.Sprintf("pipeline %q is at its concurrency budget; "+
				"the step runs when one of its siblings finishes", pipelineID), state)
	}
	return release, true, nil
}

// hold remembers the release for a step that is now in flight, so a terminal
// status can give the slot back.
//
// It is in memory and it is not run position: what is held is a lease on a
// fleet resource, exactly as the lease manager holds one, and it is the
// BUCKET that decides how long a slot survives a plane that dies (see
// budget.go). A plane that restarts forgets these and the slots age out; it
// does not forget where any run had got to, because that is in the log.
func (s *Scheduler) hold(tenantID, runID, stepID string, release func()) {
	key := stepKey(tenantID, runID, stepID)
	s.heldMu.Lock()
	previous, existed := s.held[key]
	s.held[key] = release
	s.heldMu.Unlock()
	if existed {
		// A re-dispatch of a step whose previous slot was never released:
		// give the old one back rather than losing the reference to it.
		previous()
	}
}

// releaseHold gives back the budget slot a step was holding, if this plane is
// the one holding it.
//
// It is called on EVERY way a step can leave flight — succeeded, failed,
// cancelled, and an attempt lost with its engine — and not only on success. A
// slot released only on the happy path leaks on every failure, and a leaked
// slot wedges its pipeline permanently at the moment something else has
// already gone wrong.
func (s *Scheduler) releaseHold(tenantID, runID, stepID string) {
	key := stepKey(tenantID, runID, stepID)
	s.heldMu.Lock()
	release, ok := s.held[key]
	delete(s.held, key)
	s.heldMu.Unlock()
	if ok {
		release()
	}
}

// stepKey identifies one step of one run, tenant first. Every map in this file
// is keyed by it, so a step of two tenants' identically named runs is two
// entries and never one.
func stepKey(tenantID, runID, stepID string) string {
	return tenantID + "\x00" + runID + "\x00" + stepID
}
