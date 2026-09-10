package api

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"connectrpc.com/connect"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/trigger"
	gittrigger "github.com/azrtydxb/dhole/internal/trigger/git"
	httptrigger "github.com/azrtydxb/dhole/internal/trigger/http"
	"github.com/azrtydxb/dhole/internal/trigger/schedule"
)

// Triggers is the durable trigger table, narrowed to what this service does
// with it. *trigger.Store's SQL implementation satisfies it.
//
// It is a seam here rather than an import of a concrete store for the reason
// every other collaborator on Config is: this package owns the contract, not
// the storage, and a test gets to substitute one that fails.
type Triggers interface {
	Create(ctx context.Context, tenantID string, s trigger.Spec) error
	List(ctx context.Context, tenantID string) ([]trigger.Spec, error)
	Delete(ctx context.Context, tenantID, triggerID string) error
}

// CreateTrigger stores an event source in the caller's tenant.
//
// It exists because a trigger could only be DECLARED: `--triggers` reads a
// YAML file into server.Config at start-up, so creating one meant a shell on
// the control plane's host and a restart. The GUI has neither and an agent has
// neither, which is precisely the capability ADR 0013 refuses to leave off the
// contract.
//
// The binding is checked HERE, against the pipeline's active revision, rather
// than when the trigger first fires. That is ADR 0007's rule: a binding naming
// an input the pipeline does not declare is a mistake somebody made while
// wiring a trigger up, and finding it now is worth far more than finding it at
// 3am inside the first step of a run that should never have started.
func (s *Server) CreateTrigger(
	ctx context.Context, req *connect.Request[dholev1.CreateTriggerRequest],
) (*connect.Response[dholev1.CreateTriggerResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	if s.triggers == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a trigger store"))
	}
	spec, err := specOf(req.Msg.GetTrigger())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	spec.CreatedBy = p.Subject

	// A declared trigger wins, and the refusal says so. The `--triggers` file
	// is what the plane reads at every start, so a stored row of the same id
	// would be shadowed on this plane and would come back the moment the file
	// stopped declaring it — a trigger that fires or does not depending on a
	// file the caller cannot see is worse than a refusal.
	if s.declared[spec.ID] {
		return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf(
			"api: trigger %q is declared in this plane's --triggers file, which wins over a stored one; "+
				"edit that file, or create this trigger under another id", spec.ID))
	}

	pipeline, err := s.activePipeline(ctx, p.TenantID, spec.PipelineID)
	if err != nil {
		return nil, err
	}
	if err := trigger.ValidateBinding(pipeline, trigger.Binding{
		PipelineID: spec.PipelineID, InputMapping: spec.InputMapping,
	}); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("api: %w", err))
	}

	switch err := s.triggers.Create(ctx, p.TenantID, spec); {
	case errors.Is(err, trigger.ErrExists):
		return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("api: %w", err))
	case err != nil:
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("api: create trigger: %w", err))
	}
	return connect.NewResponse(&dholev1.CreateTriggerResponse{Trigger: wireTrigger(spec, false)}), nil
}

// ListTriggers returns the tenant's triggers: the stored ones and the ones
// this plane declares, marked as such.
//
// Both, because an operator mid-migration has some of each and a listing that
// showed only half would make the other half invisible — and an invisible
// trigger that starts runs is the worst kind. No secret travels: a contract
// that read back the shared secret it was given would make every listing
// client a way to exfiltrate it.
func (s *Server) ListTriggers(
	ctx context.Context, req *connect.Request[dholev1.ListTriggersRequest],
) (*connect.Response[dholev1.ListTriggersResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	if s.triggers == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a trigger store"))
	}
	stored, err := s.triggers.List(ctx, p.TenantID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("api: list triggers: %w", err))
	}

	out := make([]*dholev1.Trigger, 0, len(stored)+len(s.declared))
	for _, spec := range stored {
		out = append(out, wireTrigger(spec, false))
	}
	for _, spec := range s.declaredSpecs {
		out = append(out, wireTrigger(spec, true))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetId() < out[j].GetId() })
	return connect.NewResponse(&dholev1.ListTriggersResponse{Triggers: out}), nil
}

// DeleteTrigger removes one stored trigger.
//
// A DECLARED one is refused rather than deleted: it is in the plane's
// configuration file, so a delete would appear to work and the trigger would
// be back at the next restart.
func (s *Server) DeleteTrigger(
	ctx context.Context, req *connect.Request[dholev1.DeleteTriggerRequest],
) (*connect.Response[dholev1.DeleteTriggerResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}
	if s.triggers == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			errors.New("api: this server was built without a trigger store"))
	}
	id := req.Msg.GetTriggerId()
	if id == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("api: a trigger_id is required"))
	}
	if s.declared[id] {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf(
			"api: trigger %q is declared in this plane's --triggers file and cannot be deleted through "+
				"the contract; the next restart would read it again", id))
	}

	switch err := s.triggers.Delete(ctx, p.TenantID, id); {
	case errors.Is(err, trigger.ErrNoSuchTrigger):
		// Another tenant's trigger is indistinguishable from one that was
		// never created, exactly as another tenant's revision is.
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("api: %w", err))
	case err != nil:
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("api: delete trigger: %w", err))
	}
	return connect.NewResponse(&dholev1.DeleteTriggerResponse{}), nil
}

// activePipeline is the definition a trigger would drive: the pipeline's
// active revision, read now so the binding can be checked against what the
// trigger will actually fire.
func (s *Server) activePipeline(
	ctx context.Context, tenantID, pipelineID string,
) (*dholev1.Pipeline, error) {
	revision, err := s.defs.Active(ctx, tenantID, pipelineID)
	if err != nil {
		return nil, storeError(fmt.Sprintf("resolving the active revision of pipeline %q", pipelineID), err)
	}
	pipeline, err := s.defs.Get(ctx, tenantID, pipelineID, revision.ID)
	if err != nil {
		return nil, storeError(fmt.Sprintf("reading pipeline %q", pipelineID), err)
	}
	return pipeline, nil
}

// specOf is the wire trigger as the store holds it, with everything a caller
// does not get to decide left out: the tenant is the credential's and
// `declared` describes the plane's own file.
func specOf(t *dholev1.Trigger) (trigger.Spec, error) {
	if t == nil {
		return trigger.Spec{}, errors.New("api: a trigger is required")
	}
	if t.GetId() == "" {
		return trigger.Spec{}, errors.New("api: a trigger needs an id")
	}
	if t.GetPipelineId() == "" {
		return trigger.Spec{}, errors.New("api: a trigger needs the pipeline it drives")
	}
	switch t.GetKind() {
	case schedule.Kind:
		if strings.TrimSpace(t.GetExpression()) == "" {
			return trigger.Spec{}, errors.New("api: a schedule trigger needs a cron expression")
		}
	case httptrigger.Kind:
	case gittrigger.Kind:
		if t.GetSecret() == "" {
			// The same rule the declared path enforces at start-up: an
			// endpoint that can verify nothing is an unauthenticated way to
			// start somebody's pipeline.
			return trigger.Spec{}, errors.New(
				"api: a git trigger needs the secret its forge signs with")
		}
	default:
		return trigger.Spec{}, fmt.Errorf(
			"api: no trigger kind is registered for %q; known kinds are %s, %s and %s",
			t.GetKind(), schedule.Kind, httptrigger.Kind, gittrigger.Kind)
	}
	return trigger.Spec{
		ID:           t.GetId(),
		Kind:         t.GetKind(),
		PipelineID:   t.GetPipelineId(),
		InputMapping: t.GetInputMapping(),
		Expression:   t.GetExpression(),
		Secret:       t.GetSecret(),
		Untrusted:    t.GetUntrusted(),
	}, nil
}

// wireTrigger is one trigger as the contract carries it. The secret is
// DROPPED and reported as a boolean: it travels inwards only.
func wireTrigger(spec trigger.Spec, declared bool) *dholev1.Trigger {
	return &dholev1.Trigger{
		Id:           spec.ID,
		Kind:         spec.Kind,
		PipelineId:   spec.PipelineID,
		InputMapping: spec.InputMapping,
		Expression:   spec.Expression,
		Untrusted:    spec.Untrusted,
		Declared:     declared,
		HasSecret:    spec.Secret != "",
	}
}
