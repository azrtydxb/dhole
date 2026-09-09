package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// Drainer is the registry, narrowed to the one write EngineService makes.
// Fleet (plan.go) is the read half; keeping them apart means a server given
// only a listing cannot drain anything by accident.
type Drainer interface {
	Drain(ctx context.Context, engineID string) error
}

// EngineControl delivers one control message to one engine.
//
// It is an interface because the API must not know how an engine is reached:
// engines are outbound-only — they dial the bus and nothing dials them — so
// "reaching" an engine means publishing on `engine.control.<engine-id>`, which
// is the bus's business (docs/wire-contract.md). BusControl is the one
// implementation, and a test can watch the subject instead.
type EngineControl interface {
	Send(ctx context.Context, engineID string, control *dholev1.EngineControl) error
}

// Compile-time proof that the server serves the whole engine contract too.
var _ dholev1connect.EngineServiceHandler = (*Server)(nil)

// ListEngines is the live fleet for the caller's tenant.
func (s *Server) ListEngines(
	ctx context.Context, req *connect.Request[dholev1.ListEnginesRequest],
) (*connect.Response[dholev1.ListEnginesResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	if s.fleet == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without an engine registry"))
	}
	instances, err := s.fleet.Instances(ctx, p.TenantID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("api: list engines: %w", err))
	}
	out := make([]*dholev1.Engine, 0, len(instances))
	for _, instance := range instances {
		out = append(out, wireEngine(instance))
	}
	return connect.NewResponse(&dholev1.ListEnginesResponse{Engines: out}), nil
}

// DrainEngine stops new work reaching an engine while it finishes what it
// holds. It kills nothing: a drain that interrupted running work would make
// every rolling upgrade an outage.
//
// An engine this tenant's fleet does not hold is refused rather than drained
// silently — a drain that reported success for an engine belonging to somebody
// else would be a cross-tenant write with a reassuring answer.
func (s *Server) DrainEngine(
	ctx context.Context, req *connect.Request[dholev1.DrainEngineRequest],
) (*connect.Response[dholev1.DrainEngineResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	engineID := req.Msg.GetEngineId()
	if engineID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: an engine_id is required"))
	}
	if s.fleet == nil || s.drain == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without an engine registry"))
	}

	// The tenant check is a listing rather than a flag on Drain, because the
	// registry keys instances by tenant and Drain does not take one: reading
	// this tenant's fleet is how the caller is held to it.
	instances, err := s.fleet.Instances(ctx, p.TenantID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("api: list engines: %w", err))
	}
	if !holds(instances, engineID) {
		return nil, connect.NewError(connect.CodeNotFound,
			fmt.Errorf("api: no engine %q in this fleet", engineID))
	}
	if err := s.drain.Drain(ctx, engineID); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("api: drain engine: %w", err))
	}

	// Read back rather than describing what was asked for: the instance may
	// already have gone, and reporting the state this call intended would be
	// reporting something that is not true.
	after, err := s.fleet.Instances(ctx, p.TenantID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("api: list engines: %w", err))
	}
	res := &dholev1.DrainEngineResponse{}
	for _, instance := range after {
		if instance.ID == engineID {
			res.Engine = wireEngine(instance)
		}
	}
	return connect.NewResponse(res), nil
}

// CancelRun stops a run and tells every engine holding one of its steps.
//
// Both halves are necessary. Closing the log alone leaves the sandboxes
// running and the slots occupied, so the capacity the operator asked for never
// comes back and they were told the run had stopped; telling the engines alone
// leaves a run the scheduler will dispatch again the moment the attempt is
// reported lost.
//
// Which engine holds a step is a question only the engines can answer: a
// dispatch goes to a SUBJECT, and which member of the fleet picked it up is
// reported by that engine's own heartbeat and nowhere else. So the cancel is
// addressed from the fleet's in-flight lists, with the fence of the attempt
// actually in flight — a control message carrying a stale fence is one the
// engine must ignore.
func (s *Server) CancelRun(
	ctx context.Context, req *connect.Request[dholev1.CancelRunRequest],
) (*connect.Response[dholev1.CancelRunResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	if s.runs == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a run store"))
	}
	runID := req.Msg.GetRunId()
	if runID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a run_id is required"))
	}

	events, err := s.runs.Replay(ctx, p.TenantID, runID)
	if err != nil {
		return nil, storeError("replay run", err)
	}
	if len(events) == 0 {
		// A run of another tenant is indistinguishable from one that does
		// not exist.
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("api: no run %q", runID))
	}

	held, err := s.heldBy(ctx, p.TenantID, runID)
	if err != nil {
		return nil, err
	}

	// The log first. A cancellation that reached the engines and was not
	// recorded is a run the scheduler starts again, and the second dispatch
	// would have no cancel behind it.
	payload, err := json.Marshal(cancellation{Reason: req.Msg.GetReason(), By: p.Subject})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if err := s.runs.Append(ctx, p.TenantID, runstore.Event{
		RunID: runID,
		// Sequence 0: the store allocates it in the write's own transaction.
		Type:    runstore.RunCancelled,
		Payload: payload,
		At:      s.now().UTC(),
	}); err != nil {
		return nil, storeError("record cancellation", err)
	}

	out := make([]*dholev1.CancelledStep, 0, len(held))
	for _, job := range held {
		if s.control == nil {
			return nil, connect.NewError(connect.CodeUnimplemented, errors.New(
				"api: this server has no way to reach an engine, so the run is closed in its log "+
					"and the step it holds is still running"))
		}
		if err := s.control.Send(ctx, job.engineID, &dholev1.EngineControl{
			Kind: &dholev1.EngineControl_Cancel{Cancel: &dholev1.Cancel{
				RunId:      runID,
				StepId:     job.job.StepID,
				Attempt:    job.job.Attempt,
				FenceToken: job.job.FenceToken,
			}},
		}); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf(
				"api: cancelling %s/%s on engine %q: %w", runID, job.job.StepID, job.engineID, err))
		}
		out = append(out, &dholev1.CancelledStep{
			StepId:   job.job.StepID,
			Attempt:  job.job.Attempt,
			EngineId: job.engineID,
		})
	}
	return connect.NewResponse(&dholev1.CancelRunResponse{RunId: runID, Steps: out}), nil
}

// cancellation is the payload of a RUN_CANCELLED event: why, and by whom. The
// subject is the authenticated caller and is never taken from the request.
type cancellation struct {
	Reason string `json:"reason,omitempty"`
	By     string `json:"by"`
}

// placed is one attempt and the engine that reported holding it.
type placed struct {
	engineID string
	job      registry.Job
}

// heldBy is every in-flight attempt of one run, with the engine holding it.
func (s *Server) heldBy(ctx context.Context, tenantID, runID string) ([]placed, error) {
	if s.fleet == nil {
		// Nothing to address a cancel to. That is a real deployment — a plane
		// with no registry — and it is reported rather than silently treated
		// as "the run held nothing".
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New(
			"api: this server was built without an engine registry, so it cannot find the "+
				"engine holding a step"))
	}
	instances, err := s.fleet.Instances(ctx, tenantID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("api: list engines: %w", err))
	}
	var out []placed
	for _, instance := range instances {
		for _, job := range instance.InFlight {
			if job.RunID == runID {
				out = append(out, placed{engineID: instance.ID, job: job})
			}
		}
	}
	return out, nil
}

// holds says whether the fleet contains that engine.
func holds(instances []registry.Instance, engineID string) bool {
	for _, instance := range instances {
		if instance.ID == engineID {
			return true
		}
	}
	return false
}

// wireEngine is one registry instance as the wire carries it.
func wireEngine(instance registry.Instance) *dholev1.Engine {
	slots := instance.Slots
	if slots < 0 {
		slots = 0
	}
	held := make([]*dholev1.InFlight, 0, len(instance.InFlight))
	for _, job := range instance.InFlight {
		held = append(held, &dholev1.InFlight{
			RunId:      job.RunID,
			StepId:     job.StepID,
			Attempt:    job.Attempt,
			FenceToken: job.FenceToken,
		})
	}
	return &dholev1.Engine{
		Id:               instance.ID,
		State:            string(instance.State),
		Capabilities:     instance.Capabilities,
		Os:               instance.OS,
		Arch:             instance.Arch,
		Slots:            uint32(slots), //nolint:gosec // negative slots are clamped above
		ProtocolVersions: instance.ProtocolVersions,
		InFlight:         held,
	}
}
