package api

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/dag"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// The two severities a diagnostic carries. They are the wire's own strings
// rather than an enum, and they are written once here so a typo cannot invent
// a third one.
const (
	severityError   = "error"
	severityWarning = "warning"
)

// Validate returns every problem found in a definition, positioned at the step
// and port it is about.
//
// Everything about it is plural on purpose. A person fixing a pipeline wants
// the whole list: reporting the first problem and stopping turns a definition
// with four mistakes into four edit-save-validate round trips, and the editor
// that draws markers on ports can only draw the ones it was told about. So
// each check contributes what it found and none of them short-circuits the
// others.
//
// It answers about a saved revision or about a definition the caller has not
// saved — the editor validates on the way to a save, not after it.
func (s *Server) Validate(
	ctx context.Context, req *connect.Request[dholev1.ValidateRequest],
) (*connect.Response[dholev1.ValidateResponse], error) {
	p, err := s.principal(ctx, req.Header())
	if err != nil {
		return nil, err
	}

	pipeline := req.Msg.GetPipeline()
	if pipeline == nil {
		pipeline, _, err = s.pinned(ctx, p.TenantID, req.Msg.GetPipelineId(), req.Msg.GetRevisionId())
		if err != nil {
			return nil, err
		}
	}

	diags := make([]*dholev1.Diagnostic, 0)
	for _, d := range dag.TypeCheck(pipeline) {
		diags = append(diags, &dholev1.Diagnostic{
			Severity: severityError,
			Message:  d.Message,
			StepId:   d.StepID,
			Port:     d.PortName,
		})
	}
	// A cycle is not an edge that cannot carry data, so the type checker does
	// not see it; it is reported here rather than left for Plan to refuse,
	// because a definition that cannot be ordered is a problem of the
	// definition.
	if _, err := dag.Build(pipeline); err != nil {
		diags = append(diags, &dholev1.Diagnostic{Severity: severityError, Message: err.Error()})
	}

	pluginDiags, err := s.pluginDiagnostics(ctx, p.TenantID, pipeline)
	if err != nil {
		return nil, err
	}
	diags = append(diags, pluginDiags...)

	fleetDiags, err := s.schedulabilityDiagnostics(ctx, p.TenantID, pipeline)
	if err != nil {
		return nil, err
	}
	diags = append(diags, fleetDiags...)

	return connect.NewResponse(&dholev1.ValidateResponse{Diagnostics: diags}), nil
}

// pluginDiagnostics resolves each step against the plugin it names.
//
// The catalog is the one thing here that already validates plugin schemas: a
// manifest whose input or output document does not compile is refused at
// publish, so a step naming a published plugin names schemas that compiled.
// What is left to report is the step's own relationship to it — a reference
// that does not resolve, and an effect class the step widened over the one the
// plugin declared, which is what makes an unrepeatable action cacheable
// (ADR 0002).
//
// Without a catalog there is nothing to resolve against and the checks are
// skipped: reporting every step as unresolvable would be worse than silence.
func (s *Server) pluginDiagnostics(
	ctx context.Context, tenantID string, pipeline *dholev1.Pipeline,
) ([]*dholev1.Diagnostic, error) {
	if s.cat == nil {
		return nil, nil
	}
	var diags []*dholev1.Diagnostic
	for _, step := range pipeline.GetSteps() {
		if step.GetPluginRef() == "" {
			continue
		}
		entry, err := s.cat.ResolveStep(ctx, tenantID, step)
		if err != nil {
			diags = append(diags, &dholev1.Diagnostic{
				Severity: severityError,
				StepId:   step.GetId(),
				Message: fmt.Sprintf("step %q names plugin %q: %v",
					step.GetId(), step.GetPluginRef(), err),
			})
			continue
		}
		for _, warning := range entry.OverrideWarnings {
			diags = append(diags, &dholev1.Diagnostic{
				Severity: severityWarning,
				StepId:   step.GetId(),
				Message:  warning,
			})
		}
	}
	return diags, nil
}

// schedulabilityDiagnostics reports a step no engine in the fleet can take.
//
// This is the failure this system fears most: a step that is never dispatched
// never fails and never explains itself, so the run looks slow rather than
// stuck. Saying it before the run is the cheapest place to say it. The reason
// is the scheduler's own Explain, so the sentence a person reads here is the
// sentence the run's log would have carried — one wording, not two that drift.
//
// It is a warning rather than an error: the fleet is a fact about this
// moment, and an engine that is still starting makes the same definition
// schedulable a second later.
func (s *Server) schedulabilityDiagnostics(
	ctx context.Context, tenantID string, pipeline *dholev1.Pipeline,
) ([]*dholev1.Diagnostic, error) {
	if s.fleet == nil {
		return nil, nil
	}
	engines, err := s.fleet.Instances(ctx, tenantID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal,
			fmt.Errorf("api: reading the engine fleet: %w", err))
	}
	var diags []*dholev1.Diagnostic
	for _, step := range pipeline.GetSteps() {
		req := s.requirements(step)
		if len(scheduler.Match(req, engines)) > 0 {
			continue
		}
		diags = append(diags, &dholev1.Diagnostic{
			Severity: severityWarning,
			StepId:   step.GetId(),
			Message: fmt.Sprintf("no engine can take step %q: %s",
				step.GetId(), scheduler.Explain(req, engines)),
		})
	}
	return diags, nil
}

// pinned resolves the definition an RPC is about: the named revision, or the
// pipeline's head when none was named.
func (s *Server) pinned(
	ctx context.Context, tenantID, pipelineID, revisionID string,
) (*dholev1.Pipeline, string, error) {
	if pipelineID == "" {
		return nil, "", connect.NewError(connect.CodeInvalidArgument,
			errors.New("api: a pipeline_id is required"))
	}
	if revisionID == "" {
		var err error
		revisionID, err = s.headRevision(ctx, tenantID, pipelineID)
		if err != nil {
			return nil, "", err
		}
	}
	definition, err := s.defs.Get(ctx, tenantID, pipelineID, revisionID)
	if err != nil {
		return nil, "", storeError("get pipeline", err)
	}
	return definition, revisionID, nil
}
