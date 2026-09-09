// Package scheduler decides what a run does next.
//
// A run is NOT a goroutine. There is no object here holding a run's position,
// no channel carrying its state and no call stack it lives in. Advance reads
// the run's persisted event log, computes what has become ready, and writes
// events; everything it knew is on disk when it returns. That is what makes a
// control-plane restart a replay rather than a loss, and what makes a step
// that waits three days cost a row rather than a goroutine (ADR 0003).
//
// Two consequences follow and both are load-bearing:
//
// Advance is idempotent. It is called after every status, after every restart,
// and by several control planes at once, so calling it against unchanged state
// must dispatch nothing. Readiness is derived from the log every time rather
// than remembered between calls.
//
// A dispatch and the event that justifies it commit together, through the
// outbox, or neither does (ADR 0005). Publishing directly would let a crash
// between the two leave a step that ran with nothing in the log saying so, or
// an event with no work ever sent — and nothing could tell which had happened.
package scheduler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/dag"
	"github.com/azrtydxb/dhole/internal/effects"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/wire"
)

// StepUnschedulable records that a ready step matched no engine, and why.
//
// It is an event rather than a log line because the run's log is the only
// place a person looks when a run is not progressing: a step that is ready,
// placeable nowhere, and silent about it is indistinguishable from a slow
// system. Stored values are a persistence contract — add types, never rename
// one — and this one belongs to the same log as runstore's own.
const StepUnschedulable runstore.EventType = "STEP_UNSCHEDULABLE"

// StepAwaitingReplay records that a step failed and will NOT be retried
// automatically, because its effect class forbids it (ADR 0002). The run is
// not finished and not failed: it is waiting for a person to decide whether
// the step ran, and whether to replay it.
//
// It is an event rather than silence for the same reason StepUnschedulable is.
// A step that stops without a word is indistinguishable from a slow one, and
// this is the case where the operator most needs to be told, because nothing
// will move until they act.
const StepAwaitingReplay runstore.EventType = "STEP_AWAITING_REPLAY"

// StepAwaitingTimer and StepAwaitingApproval are the two gates a step can sit
// behind: a durable wait until a time, and a wait for a person. They are what
// makes a three-day wait cost a row rather than a goroutine (ADR 0003), and
// they are declared here because this is where they are HONOURED — a gated
// step is neither dispatched nor counted as finished, so a run holding one
// neither runs ahead of its wait nor completes without it.
//
// The packages that emit them (internal/wait, internal/steps/approval) read
// these constants rather than repeating the strings, because a gate written
// one way and read another is a run that waits forever.
const (
	StepAwaitingTimer    runstore.EventType = "STEP_AWAITING_TIMER"
	StepAwaitingApproval runstore.EventType = "STEP_AWAITING_APPROVAL"
)

// RunFailed closes a run whose step used up its attempts. A run that failed is
// not a run that completed: reporting one as the other is how a broken
// pipeline looks green.
const RunFailed runstore.EventType = "RUN_FAILED"

// IdempotencyKeyEnv is the environment variable a step's idempotency key
// arrives in. A key computed in the control plane and never handed to the
// process protects nothing: the far end is the only place a duplicate can be
// recognised, and the step is what talks to it.
const IdempotencyKeyEnv = "DHOLE_IDEMPOTENCY_KEY"

// DefaultLeaseTTL is how long a step's lease lives before the control plane
// treats its holder as dead. An engine heartbeats every five seconds
// (docs/wire-contract.md), so this allows several missed beats before a step
// is taken away from an engine that is merely slow.
const DefaultLeaseTTL = 30 * time.Second

// Fleet is the live engine registry, narrowed to what scheduling reads. The
// KV-backed registry.Registry satisfies it; so does a list in a test, which is
// the point — the matching decision must be testable without a bus.
type Fleet interface {
	Instances(ctx context.Context, tenantID string) ([]registry.Instance, error)
}

// Definitions resolves the exact pipeline revision a run pinned. A run reads
// back what it started with however long it lasts, so this is never asked for
// "the current" definition of anything.
type Definitions interface {
	Get(ctx context.Context, tenantID, pipelineID, revisionID string) (*dholev1.Pipeline, error)
}

// Config is everything the scheduler needs. All of it is durable
// infrastructure or a pure lookup: there is deliberately no place to keep
// per-run state, because a field holding one would be the run position that
// ADR 0003 exists to keep out of memory.
type Config struct {
	// Store is the run event log: the run's only position.
	Store runstore.Store
	// Outbox couples a dispatch to the event that justifies it.
	Outbox *outbox.Outbox
	// Leases hands out the fence every dispatch carries.
	Leases lease.Manager
	// Fleet is the live engine registry.
	Fleet Fleet
	// Definitions resolves the pinned pipeline revision.
	Definitions Definitions
	// Tier is the trust tier work is dispatched to.
	Tier string
	// OS and Arch are the platform steps are scheduled for, in Go's
	// GOOS/GOARCH vocabulary. Empty means the deployment does not care.
	OS   string
	Arch string
	// EnvIdentity is the digest of the environment steps run in, as the
	// executor reports it. Empty means the backend has none — a host process
	// backend, say — and every step is then cached against nothing, which is
	// to say not cached.
	EnvIdentity string
	// LeaseTTL overrides DefaultLeaseTTL.
	LeaseTTL time.Duration
	// Now is the clock, injectable for tests.
	Now func() time.Time
}

// Scheduler advances runs. It is safe for concurrent use and safe to run in
// several control planes at once.
type Scheduler struct {
	store runstore.Store
	out   *outbox.Outbox
	leas  lease.Manager
	fleet Fleet
	defs  Definitions

	tier        string
	os          string
	arch        string
	envIdentity string
	ttl         time.Duration
	now         func() time.Time
}

// New validates the configuration and builds a Scheduler.
func New(cfg Config) (*Scheduler, error) {
	switch {
	case cfg.Store == nil:
		return nil, errors.New("scheduler: a run store is required")
	case cfg.Outbox == nil:
		return nil, errors.New("scheduler: an outbox is required; a dispatch may not be published directly")
	case cfg.Leases == nil:
		return nil, errors.New("scheduler: a lease manager is required")
	case cfg.Fleet == nil:
		return nil, errors.New("scheduler: an engine registry is required")
	case cfg.Definitions == nil:
		return nil, errors.New("scheduler: a definition store is required")
	case cfg.Tier == "":
		return nil, errors.New("scheduler: a dispatch tier is required")
	}

	s := &Scheduler{
		store:       cfg.Store,
		out:         cfg.Outbox,
		leas:        cfg.Leases,
		fleet:       cfg.Fleet,
		defs:        cfg.Definitions,
		tier:        cfg.Tier,
		os:          cfg.OS,
		arch:        cfg.Arch,
		envIdentity: cfg.EnvIdentity,
		ttl:         cfg.LeaseTTL,
		now:         cfg.Now,
	}
	if s.ttl <= 0 {
		s.ttl = DefaultLeaseTTL
	}
	if s.now == nil {
		s.now = time.Now
	}
	return s, nil
}

// Advance moves a run as far as its log allows: every step whose predecessors
// have all succeeded is claimed and dispatched, and a run with nothing left
// ready and nothing in flight is completed.
//
// It is idempotent. Calling it twice against the same state dispatches
// nothing the second time, because readiness is computed from the log and a
// dispatched step is no longer ready.
func (s *Scheduler) Advance(ctx context.Context, tenantID, runID string) error {
	if tenantID == "" {
		return fmt.Errorf("scheduler: advance: %w", runstore.ErrTenantRequired)
	}
	if runID == "" {
		return errors.New("scheduler: advance: run id is required")
	}

	state, err := s.load(ctx, tenantID, runID)
	if err != nil {
		return err
	}
	if state.completed {
		return nil
	}

	pipeline, err := s.defs.Get(ctx, tenantID, state.pipelineID, state.revisionID)
	if err != nil {
		return fmt.Errorf("scheduler: run %q pins revision %q: %w", runID, state.revisionID, err)
	}
	graph, err := dag.Build(pipeline)
	if err != nil {
		return fmt.Errorf("scheduler: run %q: %w", runID, err)
	}

	pl := plan(pipeline, graph, state, s.now())

	// Before anything is dispatched: a step whose effect class forbids an
	// automatic retry has to be visible as such, whatever else the run is
	// still doing.
	for _, step := range pl.blocked {
		if err := s.recordAwaitingReplay(ctx, tenantID, runID, step, state); err != nil {
			return err
		}
	}

	if len(pl.ready) == 0 && pl.inFlight == 0 && pl.waiting == 0 {
		switch {
		case len(pl.exhausted) > 0:
			return s.fail(ctx, tenantID, runID, pl.exhausted)
		case len(pl.blocked) > 0:
			// Nothing is running and nothing is going to until a person
			// decides. The run stays open on purpose: it has neither
			// succeeded nor been given up on.
			return nil
		default:
			return s.complete(ctx, tenantID, runID)
		}
	}

	for _, step := range pl.ready {
		if err := s.dispatch(ctx, tenantID, runID, pipeline, step, state); err != nil {
			return err
		}
	}
	return nil
}

// OnStatus applies an engine's report and advances the run it belongs to.
//
// A status whose fence is not the current lease is IGNORED, not applied and
// not an error. An engine presumed dead can come back holding a step that has
// since been re-dispatched; its report is honest and simply belongs to an
// attempt nobody is waiting for any more, and applying it would overwrite the
// newer attempt's result (docs/wire-contract.md, "Fence tokens").
func (s *Scheduler) OnStatus(ctx context.Context, st *dholev1.JobStatus) error {
	if st == nil {
		return errors.New("scheduler: status is nil")
	}
	tenantID, token, err := DecodeFence(st.GetFenceToken())
	if err != nil {
		return fmt.Errorf("scheduler: status for %s/%s: %w", st.GetRunId(), st.GetStepId(), err)
	}
	if tenantID == "" {
		return fmt.Errorf("scheduler: status for %s/%s: %w",
			st.GetRunId(), st.GetStepId(), runstore.ErrTenantRequired)
	}

	if err := s.leas.Validate(ctx, token); err != nil {
		if errors.Is(err, lease.ErrFenced) {
			// Superseded. The result is discarded, deliberately and silently:
			// the engine did nothing wrong, it was simply overtaken.
			return nil
		}
		return fmt.Errorf("scheduler: validating fence for %s/%s: %w",
			st.GetRunId(), st.GetStepId(), err)
	}

	eventType, terminal := terminalEvent(st.GetPhase())
	if !terminal {
		// ACCEPTED and RUNNING carry no durable transition. Recording them
		// would grow the log without changing what any replay decides.
		return nil
	}

	state, err := s.load(ctx, tenantID, st.GetRunId())
	if err != nil {
		return err
	}
	if _, done := state.terminal[st.GetStepId()]; done {
		// At-least-once delivery means the same terminal status arrives
		// twice. The second one changes nothing.
		return s.Advance(ctx, tenantID, st.GetRunId())
	}

	payload, err := proto.Marshal(st)
	if err != nil {
		return fmt.Errorf("scheduler: encoding status for %s/%s: %w",
			st.GetRunId(), st.GetStepId(), err)
	}
	if err := s.append(ctx, tenantID, runstore.Event{
		RunID:   st.GetRunId(),
		StepID:  st.GetStepId(),
		Attempt: st.GetAttempt(),
		Type:    eventType,
		Payload: payload,
	}); err != nil {
		return err
	}
	return s.Advance(ctx, tenantID, st.GetRunId())
}

// terminalEvent maps a reported phase to the event it records, if any.
func terminalEvent(p dholev1.Phase) (runstore.EventType, bool) {
	switch p {
	case dholev1.Phase_PHASE_SUCCEEDED:
		return runstore.StepSucceeded, true
	case dholev1.Phase_PHASE_FAILED, dholev1.Phase_PHASE_CANCELLED:
		// A cancelled attempt is a failed one as far as the graph is
		// concerned: it produced no outputs, so nothing downstream of it can
		// run. Which of the two it was is in the recorded status.
		return runstore.StepFailed, true
	case dholev1.Phase_PHASE_UNSPECIFIED, dholev1.Phase_PHASE_ACCEPTED, dholev1.Phase_PHASE_RUNNING:
		return "", false
	default:
		return "", false
	}
}

// runState is the run reduced from its log. It is built fresh on every call
// and thrown away at the end of it: holding one between calls would be the
// in-memory run position this design refuses.
type runState struct {
	created       bool
	pipelineID    string
	revisionID    string
	completed     bool
	attempts      map[string]uint32
	terminal      map[string]runstore.EventType
	outputs       map[string][]*dholev1.OutputRef
	unschedulable map[string]string
	// failedAt is when a step's latest attempt failed, which is what a
	// backoff is measured from. It comes off the event, not the clock,
	// because a control plane that restarts mid-backoff must resume the wait
	// rather than start it again.
	failedAt map[string]time.Time
	// awaiting is the set of steps already recorded as waiting for a human.
	awaiting map[string]bool
	// gated is the set of steps stopped at a durable gate — a timer that has
	// not come due, an approval nobody has decided. The gate is lifted by the
	// step's own terminal event, written by whoever owns the gate.
	gated map[string]bool
}

// load replays the run and folds its events into the state a decision needs.
func (s *Scheduler) load(ctx context.Context, tenantID, runID string) (*runState, error) {
	events, err := s.store.Replay(ctx, tenantID, runID)
	if err != nil {
		return nil, fmt.Errorf("scheduler: replaying run %q: %w", runID, err)
	}

	state := &runState{
		attempts:      map[string]uint32{},
		terminal:      map[string]runstore.EventType{},
		outputs:       map[string][]*dholev1.OutputRef{},
		unschedulable: map[string]string{},
		failedAt:      map[string]time.Time{},
		awaiting:      map[string]bool{},
		gated:         map[string]bool{},
	}
	for _, e := range events {
		switch e.Type {
		case runstore.RunCreated:
			created, err := UnmarshalRunCreated(e.Payload)
			if err != nil {
				return nil, fmt.Errorf("scheduler: run %q: %w", runID, err)
			}
			state.created = true
			state.pipelineID = created.PipelineID
			state.revisionID = created.RevisionID
		case runstore.StepDispatched:
			if e.Attempt > state.attempts[e.StepID] {
				state.attempts[e.StepID] = e.Attempt
			}
			// A retry supersedes the failure that justified it: the step is
			// in flight again, and its earlier verdict no longer describes
			// where it stands.
			delete(state.terminal, e.StepID)
		case runstore.StepSucceeded:
			state.terminal[e.StepID] = e.Type
			status := &dholev1.JobStatus{}
			if err := proto.Unmarshal(e.Payload, status); err != nil {
				return nil, fmt.Errorf("scheduler: run %q step %q: decoding status: %w",
					runID, e.StepID, err)
			}
			state.outputs[e.StepID] = status.GetOutputs()
		case runstore.StepFailed:
			state.terminal[e.StepID] = e.Type
			state.failedAt[e.StepID] = e.At
		case StepAwaitingReplay:
			state.awaiting[e.StepID] = true
		case StepAwaitingTimer, StepAwaitingApproval:
			state.gated[e.StepID] = true
		case RunFailed:
			state.completed = true
		case runstore.RunCompleted:
			state.completed = true
		case StepUnschedulable:
			reason, err := UnmarshalUnschedulable(e.Payload)
			if err != nil {
				return nil, fmt.Errorf("scheduler: run %q step %q: %w", runID, e.StepID, err)
			}
			state.unschedulable[e.StepID] = reason.Reason
		case runstore.StepReady:
		}
	}
	if !state.created {
		return nil, fmt.Errorf("scheduler: run %q has no %s event", runID, runstore.RunCreated)
	}
	return state, nil
}

// planned is what one pass over the log concluded about a run: what may be
// dispatched now, what is already out, what is waiting, and what has stopped.
type planned struct {
	ready    []*dholev1.Step
	inFlight int
	// waiting counts steps that failed and will be retried, but not yet: a
	// run with one of these is neither finished nor idle, and completing it
	// would throw away a retry that was about to happen.
	waiting int
	// blocked are failed steps whose effect class forbids an automatic retry.
	blocked []*dholev1.Step
	// exhausted are steps that used up every attempt they were allowed.
	exhausted []string
}

// plan is the readiness rule, and it is the whole of it: a step may run when
// every step feeding it has SUCCEEDED and it has not been dispatched or
// finished itself. A step whose predecessor failed is therefore never ready
// and never in flight, which is how a failed run reaches completion instead of
// waiting for work that can no longer happen.
//
// A step that failed is decided by its effect class and nothing else (ADR
// 0002). PURE and IDEMPOTENT are retried, after a backoff that grows.
// AT_MOST_ONCE — and anything undeclared — is not retried at all, because
// nothing here can tell whether the attempt that failed had already had its
// effect.
func plan(p *dholev1.Pipeline, g *dag.Graph, state *runState, now time.Time) planned {
	predecessors := predecessorsOf(p, g)

	var out planned
	for _, step := range p.GetSteps() {
		id := step.GetId()
		switch state.terminal[id] {
		case runstore.StepSucceeded:
			continue
		case runstore.StepFailed:
			out.classify(step, state, now)
			continue
		}
		// A gate is checked before anything else a step could be: it has not
		// been dispatched, it has not failed, and it is not finished. It is
		// waiting, which is neither idle nor in flight — completing the run
		// here would throw the wait away.
		if state.gated[id] {
			out.waiting++
			continue
		}
		if state.attempts[id] > 0 {
			out.inFlight++
			continue
		}
		satisfied := true
		for _, pred := range predecessors[id] {
			if state.terminal[pred] != runstore.StepSucceeded {
				satisfied = false
				break
			}
		}
		if satisfied {
			out.ready = append(out.ready, step)
		}
	}
	sort.Slice(out.ready, func(i, j int) bool { return out.ready[i].GetId() < out.ready[j].GetId() })
	return out
}

// classify decides what happens to a step that has failed. The whole decision
// is the policy its effect class implies: whether it may be repeated at all,
// how many times, and how long the wait is.
func (pl *planned) classify(step *dholev1.Step, state *runState, now time.Time) {
	id := step.GetId()
	policy := effects.RetryPolicy(step)
	attempts := state.attempts[id]

	if !policy.AllowsRetry(attempts) {
		if policy.MaxAttempts <= 1 {
			// Never automatically repeated: a person decides. This is the
			// case the effect class exists for.
			pl.blocked = append(pl.blocked, step)
			return
		}
		pl.exhausted = append(pl.exhausted, id)
		return
	}
	if now.Before(state.failedAt[id].Add(effects.Backoff(policy, attempts))) {
		pl.waiting++
		return
	}
	pl.ready = append(pl.ready, step)
}

// predecessorsOf inverts the graph's dependent edges. The dependencies come
// from dag.Graph rather than from the pipeline's edges directly so that the
// scheduler and the DAG can never disagree about what depends on what — and
// building the graph is also what refuses a cyclic pipeline.
func predecessorsOf(p *dholev1.Pipeline, g *dag.Graph) map[string][]string {
	preds := make(map[string][]string, len(p.GetSteps()))
	for _, step := range p.GetSteps() {
		for _, dependent := range g.Dependents(step.GetId()) {
			preds[dependent] = append(preds[dependent], step.GetId())
		}
	}
	return preds
}

// dispatch places one ready step: match it to the fleet, claim its lease, and
// write the event and the outbox row in one transaction.
func (s *Scheduler) dispatch(
	ctx context.Context,
	tenantID, runID string,
	pipeline *dholev1.Pipeline,
	step *dholev1.Step,
	state *runState,
) error {
	req := s.requirements(step)
	instances, err := s.fleet.Instances(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("scheduler: listing engines for %s: %w", tenantID, err)
	}
	if len(Match(req, instances)) == 0 {
		return s.recordUnschedulable(ctx, tenantID, runID, step.GetId(), Explain(req, instances), state)
	}

	attempt := state.attempts[step.GetId()] + 1
	token, err := s.leas.Claim(ctx, tenantID, runID, step.GetId(), attempt, s.ttl)
	if err != nil {
		return fmt.Errorf("scheduler: claiming %s/%s: %w", runID, step.GetId(), err)
	}

	// Claiming took a round trip, and another control plane may have finished
	// dispatching this very attempt while it did. Re-reading the log is what
	// keeps that from becoming a second dispatch; the fence check inside the
	// transaction below covers the reverse order.
	fresh, err := s.load(ctx, tenantID, runID)
	if err != nil {
		return err
	}
	if fresh.attempts[step.GetId()] >= attempt {
		return nil
	}

	policy := effects.RetryPolicy(step)
	if policy.RequiresExclusiveLease {
		// A step that must run at most once may only be sent by the plane
		// that demonstrably holds its lease, and it is checked HERE rather
		// than being left to the transaction below to undo: a fence that has
		// already moved means another plane owns this step, and a dispatch
		// built on it must never exist, not even briefly and rolled back
		// (ADR 0002).
		if err := s.leas.Validate(ctx, token); err != nil {
			if errors.Is(err, lease.ErrFenced) {
				return nil
			}
			return fmt.Errorf("scheduler: proving the exclusive lease on %s/%s: %w",
				runID, step.GetId(), err)
		}
	}

	dispatchMsg, err := s.buildDispatch(tenantID, runID, pipeline, step, attempt, token, fresh)
	if err != nil {
		return err
	}

	// The key is the same on every attempt of a step, which is exactly what
	// makes a retry safe: the far end sees the repeat and declines to act
	// twice (ADR 0002).
	var idempotencyKey string
	if policy.RequiresIdempotencyKey {
		idempotencyKey = effects.IdempotencyKey(runID, step.GetId(), attempt)
		if dispatchMsg.GetEnv() == nil {
			dispatchMsg.Env = map[string]string{}
		}
		dispatchMsg.Env[IdempotencyKeyEnv] = idempotencyKey
	}
	cacheable, reason := cache.Eligible(step, executorLeaseScope(step.GetLeaseScope()), s.envIdentity)
	payload, err := MarshalDispatched(Dispatched{
		Attempt:               attempt,
		Fence:                 token.Fence,
		Cacheable:             cacheable,
		CacheIneligibleReason: reason,
		IdempotencyKey:        idempotencyKey,
	})
	if err != nil {
		return err
	}

	sequence, err := s.nextSequence(ctx, tenantID)
	if err != nil {
		return err
	}
	subject := bus.SubjectDispatch(s.tier, engine.CapsHash(step.GetCapabilities()))

	err = s.store.WithTx(ctx, func(tx runstore.Tx) error {
		if err := tx.Append(ctx, tenantID, runstore.Event{
			RunID:    runID,
			StepID:   step.GetId(),
			Attempt:  attempt,
			Sequence: sequence,
			Type:     runstore.StepDispatched,
			Payload:  payload,
			At:       s.now().UTC(),
		}); err != nil {
			return err
		}
		// Through the outbox, never straight to the bus: the event and the
		// intent to publish it are one act or the step can be lost with no
		// trace of it anywhere (ADR 0005).
		if err := s.out.Enqueue(ctx, tx, tenantID, subject, dispatchMsg); err != nil {
			return err
		}
		// Last, so the window between proving the lease is ours and committing
		// on the strength of it is as small as it can be made. A plane that has
		// been superseded since it claimed rolls the whole thing back here and
		// the step is dispatched exactly once for this attempt.
		return s.leas.Validate(ctx, token)
	})
	switch {
	case errors.Is(err, lease.ErrFenced):
		// Another control plane owns this attempt. Nothing was written and
		// nothing was published; that is a normal outcome, not a failure.
		return nil
	case err != nil:
		return fmt.Errorf("scheduler: dispatching %s/%s: %w", runID, step.GetId(), err)
	}
	return nil
}

// requirements are what the step needs of the place it runs. The platform is
// the deployment's, since a step declares capabilities but not a machine.
func (s *Scheduler) requirements(step *dholev1.Step) executor.Requirements {
	return executor.Requirements{
		OS:           s.os,
		Arch:         s.arch,
		Capabilities: step.GetCapabilities(),
	}
}

// buildDispatch assembles the self-contained message an engine receives.
// Everything needed to run the step is in it: an engine never calls back into
// the control plane to find out what to do (docs/wire-contract.md).
func (s *Scheduler) buildDispatch(
	tenantID, runID string,
	pipeline *dholev1.Pipeline,
	step *dholev1.Step,
	attempt uint32,
	token lease.Token,
	state *runState,
) (*dholev1.JobDispatch, error) {
	return &dholev1.JobDispatch{
		RunId:           runID,
		StepId:          step.GetId(),
		Attempt:         attempt,
		FenceToken:      EncodeFence(tenantID, token),
		Step:            step,
		Inputs:          inputsFor(pipeline, step.GetId(), state),
		OutputPrefix:    fmt.Sprintf("runs/%s/%s/%s/%d", tenantID, runID, step.GetId(), attempt),
		ProtocolVersion: wire.ProtocolVersion,
		Tenant:          &dholev1.Tenant{Id: tenantID},
	}, nil
}

// inputsFor resolves what a step consumes from what its predecessors produced.
// The edges are the only source of this: a step reaches its predecessor's data
// by connecting a port to it and by no other means, so nothing here can drift
// from what the DAG says (ADR 0001).
func inputsFor(p *dholev1.Pipeline, stepID string, state *runState) []*dholev1.InputRef {
	var inputs []*dholev1.InputRef
	for _, e := range p.GetEdges() {
		if e.GetToStep() != stepID {
			continue
		}
		for _, out := range state.outputs[e.GetFromStep()] {
			if out.GetPort() != e.GetFromPort() {
				continue
			}
			inputs = append(inputs, &dholev1.InputRef{
				Port:   e.GetToPort(),
				Digest: out.GetDigest(),
				Key:    out.GetKey(),
			})
		}
	}
	return inputs
}

// recordUnschedulable writes why a ready step could not be placed, once per
// distinct reason. Repeating an unchanged reason on every Advance would bury
// the log; changing it — an engine drained, a capability disappeared — is news
// and is recorded.
func (s *Scheduler) recordUnschedulable(
	ctx context.Context, tenantID, runID, stepID, reason string, state *runState,
) error {
	if state.unschedulable[stepID] == reason {
		return nil
	}
	payload, err := MarshalUnschedulable(Unschedulable{Reason: reason})
	if err != nil {
		return err
	}
	return s.append(ctx, tenantID, runstore.Event{
		RunID:   runID,
		StepID:  stepID,
		Attempt: state.attempts[stepID],
		Type:    StepUnschedulable,
		Payload: payload,
	})
}

// recordAwaitingReplay writes, once, that a failed step will not be retried
// automatically. Once is enough: Advance runs after every status and every
// restart, and repeating the same standstill on each pass would bury the
// event that explains it.
func (s *Scheduler) recordAwaitingReplay(
	ctx context.Context, tenantID, runID string, step *dholev1.Step, state *runState,
) error {
	if state.awaiting[step.GetId()] {
		return nil
	}
	payload, err := MarshalAwaitingReplay(AwaitingReplay{
		EffectClass: step.GetEffectClass().String(),
		Reason: fmt.Sprintf(
			"effect class %s may not be retried automatically; a replay has to be authorised",
			strings.TrimPrefix(step.GetEffectClass().String(), "EFFECT_CLASS_")),
	})
	if err != nil {
		return err
	}
	if err := s.append(ctx, tenantID, runstore.Event{
		RunID:   runID,
		StepID:  step.GetId(),
		Attempt: state.attempts[step.GetId()],
		Type:    StepAwaitingReplay,
		Payload: payload,
	}); err != nil {
		return err
	}
	state.awaiting[step.GetId()] = true
	return nil
}

// fail closes a run whose steps ran out of attempts. It is deliberately not
// complete(): a run that failed and a run that finished are different answers
// to the only question anybody asks about a run.
func (s *Scheduler) fail(ctx context.Context, tenantID, runID string, steps []string) error {
	payload, err := MarshalRunFailure(RunFailure{Steps: steps})
	if err != nil {
		return err
	}
	return s.append(ctx, tenantID, runstore.Event{
		RunID:   runID,
		Type:    RunFailed,
		Payload: payload,
	})
}

// complete closes a run that has nothing ready and nothing in flight. Advance
// returns before reaching here once the run is completed, so this is written
// exactly once per run.
func (s *Scheduler) complete(ctx context.Context, tenantID, runID string) error {
	return s.append(ctx, tenantID, runstore.Event{
		RunID: runID,
		Type:  runstore.RunCompleted,
	})
}

// append stamps an event with the next sequence and the current time, then
// writes it. Events written here are the ones with nothing to publish
// alongside them; anything that owes a message goes through WithTx instead.
func (s *Scheduler) append(ctx context.Context, tenantID string, e runstore.Event) error {
	sequence, err := s.nextSequence(ctx, tenantID)
	if err != nil {
		return err
	}
	e.Sequence = sequence
	e.At = s.now().UTC()
	if err := s.store.Append(ctx, tenantID, e); err != nil {
		return fmt.Errorf("scheduler: appending %s for %s/%s: %w", e.Type, e.RunID, e.StepID, err)
	}
	return nil
}

// nextSequence is the tenant's next log position. It is read outside any
// transaction on purpose: the SQLite store serialises on a single connection,
// and reading through it while holding a transaction would deadlock against
// itself.
func (s *Scheduler) nextSequence(ctx context.Context, tenantID string) (uint64, error) {
	last, err := s.store.LastSequence(ctx, tenantID)
	if err != nil {
		return 0, fmt.Errorf("scheduler: reading sequence for %s: %w", tenantID, err)
	}
	return last + 1, nil
}

// executorLeaseScope maps the scope a step declares on the wire to the one the
// executor and the cache reason about.
//
// An unspecified scope is the step scope, exactly as the engine reads it: it
// is the only default that is safe in both directions, because every other
// scope reuses a previous occupant's state. The engine holds the same mapping
// for its own side of the wire; neither package can reach into the other's
// unexported half, and a wrong copy here shows up immediately as a step cached
// under a scope that carries state.
func executorLeaseScope(s dholev1.LeaseScope) executor.LeaseScope {
	switch s {
	case dholev1.LeaseScope_LEASE_SCOPE_JOB:
		return executor.LeaseJob
	case dholev1.LeaseScope_LEASE_SCOPE_PIPELINE:
		return executor.LeasePipeline
	case dholev1.LeaseScope_LEASE_SCOPE_POOL:
		return executor.LeasePool
	case dholev1.LeaseScope_LEASE_SCOPE_SERVICE:
		return executor.LeaseService
	case dholev1.LeaseScope_LEASE_SCOPE_UNSPECIFIED, dholev1.LeaseScope_LEASE_SCOPE_STEP:
		return executor.LeaseStep
	default:
		return executor.LeaseStep
	}
}

// RunCreated is the payload of the event a run starts from. A run pins a
// pipeline REVISION, not a pipeline: whatever is edited or approved while the
// run is in flight, it reads back the definition it began with.
type RunCreated struct {
	PipelineID string `json:"pipeline_id"`
	RevisionID string `json:"revision_id"`
}

// Dispatched is the payload of a STEP_DISPATCHED event.
//
// It carries the cache verdict because that is the only place anyone will see
// it. Cacheability turns on things a pipeline author changes without meaning
// to — a pool lease for speed, an effect class left undeclared — and a step
// that quietly stopped being cached looks like a permanent, unexplained
// slowdown. The reason travels with the dispatch that applied it.
type Dispatched struct {
	Attempt               uint32 `json:"attempt"`
	Fence                 uint64 `json:"fence"`
	Cacheable             bool   `json:"cacheable"`
	CacheIneligibleReason string `json:"cache_ineligible_reason,omitempty"`
	// IdempotencyKey is the key this attempt presented to the far end, when
	// the step's effect class required one. It is recorded because the
	// question after an incident is "did we send that twice", and the answer
	// is only in the log if the key that went out is in the log.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// Unschedulable is the payload of a STEP_UNSCHEDULABLE event: why a step that
// was ready could not be placed on any engine.
type Unschedulable struct {
	Reason string `json:"reason"`
}

// AwaitingReplay is the payload of a STEP_AWAITING_REPLAY event: why a failed
// step stopped instead of being retried.
type AwaitingReplay struct {
	EffectClass string `json:"effect_class"`
	Reason      string `json:"reason"`
}

// RunFailure is the payload of a RUN_FAILED event: which steps ran out of
// attempts.
type RunFailure struct {
	Steps []string `json:"steps"`
}

// MarshalAwaitingReplay encodes the STEP_AWAITING_REPLAY payload.
func MarshalAwaitingReplay(a AwaitingReplay) ([]byte, error) { return marshalPayload(a) }

// UnmarshalAwaitingReplay decodes the STEP_AWAITING_REPLAY payload.
func UnmarshalAwaitingReplay(b []byte) (AwaitingReplay, error) {
	return unmarshalPayload[AwaitingReplay](b, string(StepAwaitingReplay))
}

// MarshalRunFailure encodes the RUN_FAILED payload.
func MarshalRunFailure(r RunFailure) ([]byte, error) { return marshalPayload(r) }

// UnmarshalRunFailure decodes the RUN_FAILED payload.
func UnmarshalRunFailure(b []byte) (RunFailure, error) {
	return unmarshalPayload[RunFailure](b, string(RunFailed))
}

// MarshalRunCreated encodes the RUN_CREATED payload.
func MarshalRunCreated(r RunCreated) ([]byte, error) { return marshalPayload(r) }

// UnmarshalRunCreated decodes the RUN_CREATED payload.
func UnmarshalRunCreated(b []byte) (RunCreated, error) {
	return unmarshalPayload[RunCreated](b, string(runstore.RunCreated))
}

// MarshalDispatched encodes the STEP_DISPATCHED payload.
func MarshalDispatched(d Dispatched) ([]byte, error) { return marshalPayload(d) }

// UnmarshalDispatched decodes the STEP_DISPATCHED payload.
func UnmarshalDispatched(b []byte) (Dispatched, error) {
	return unmarshalPayload[Dispatched](b, string(runstore.StepDispatched))
}

// MarshalUnschedulable encodes the STEP_UNSCHEDULABLE payload.
func MarshalUnschedulable(u Unschedulable) ([]byte, error) { return marshalPayload(u) }

// UnmarshalUnschedulable decodes the STEP_UNSCHEDULABLE payload.
func UnmarshalUnschedulable(b []byte) (Unschedulable, error) {
	return unmarshalPayload[Unschedulable](b, string(StepUnschedulable))
}

// marshalPayload encodes an event payload as JSON. These payloads are read by
// people looking at a stuck run as often as by code, and a reason nobody can
// read without a decoder is a reason nobody reads.
func marshalPayload(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("scheduler: encoding event payload: %w", err)
	}
	return b, nil
}

func unmarshalPayload[T any](b []byte, kind string) (T, error) {
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		return v, fmt.Errorf("scheduler: decoding %s payload: %w", kind, err)
	}
	return v, nil
}

// fencePrefix versions the wire form of a fence token. An engine treats the
// token as opaque and echoes it unchanged, so the only reader is this package
// — but a stored dispatch outlives the code that wrote it, and a token with no
// version is one that cannot be changed.
const fencePrefix = "dhole1"

// EncodeFence renders a lease token as the opaque string a dispatch carries.
//
// The tenant is inside it because a JobStatus has no tenant field and every
// read in this system is tenant-scoped: without it, applying a status would
// mean searching every tenant's runs for one with a matching id, which is
// precisely the unscoped query that is not allowed to exist.
func EncodeFence(tenantID string, t lease.Token) string {
	return strings.Join([]string{
		fencePrefix,
		base64.RawURLEncoding.EncodeToString([]byte(tenantID)),
		base64.RawURLEncoding.EncodeToString([]byte(t.Value)),
		strconv.FormatUint(t.Fence, 10),
	}, ".")
}

// DecodeFence recovers the tenant and the lease token from a fence string. A
// token that does not parse is a protocol error rather than a stale fence: an
// engine must never invent or omit one.
func DecodeFence(s string) (string, lease.Token, error) {
	parts := strings.Split(s, ".")
	if len(parts) != 4 || parts[0] != fencePrefix {
		return "", lease.Token{}, fmt.Errorf("malformed fence token %q", s)
	}
	tenant, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", lease.Token{}, fmt.Errorf("malformed fence token %q: tenant: %w", s, err)
	}
	value, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", lease.Token{}, fmt.Errorf("malformed fence token %q: lease: %w", s, err)
	}
	fence, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return "", lease.Token{}, fmt.Errorf("malformed fence token %q: fence: %w", s, err)
	}
	return string(tenant), lease.Token{Value: string(value), Fence: fence}, nil
}
