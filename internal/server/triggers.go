package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/trigger"
	gittrigger "github.com/azrtydxb/dhole/internal/trigger/git"
	httptrigger "github.com/azrtydxb/dhole/internal/trigger/http"
	"github.com/azrtydxb/dhole/internal/trigger/schedule"
)

// TriggerPrefix is the path every webhook trigger is served under, on the
// plane's own listener.
//
// One listener, deliberately. A trigger on a port of its own is a second thing
// to expose, a second thing to put TLS on and a second thing an operator
// forgets, and the API already owns an ingress. The prefix is outside
// isAPIPath's two, so a webhook never shadows a contract call and a contract
// call never shadows a webhook.
const TriggerPrefix = "/triggers/"

// TriggerSpec is one configured event source, as a deployment declares it.
//
// It is configuration on Config rather than rows in a table because there is
// no trigger table: nothing in proto/, internal/defstore or internal/api can
// describe a trigger at all. That is a real gap — an operator cannot create
// one through the contract — and this is the smallest honest way to let a
// plane run the triggers internal/trigger already implements while it stays
// open.
type TriggerSpec struct {
	// ID is the trigger's identity within the tenant. A schedule's ID is the
	// key of its row, so two planes running the same ID run ONE schedule
	// between them — which is what stops a clustered deployment firing a
	// nightly job once per plane.
	ID string `json:"id"`
	// Kind is "schedule", "http" or "git".
	Kind string `json:"kind"`
	// TenantID scopes the runs it starts. Empty means DefaultTenant.
	TenantID string `json:"tenant_id,omitempty"`
	// PipelineID is the pipeline it drives. Its ACTIVE revision is resolved
	// when the trigger fires, not when it is configured: a schedule that
	// pinned whatever was current at start-up would keep running last week's
	// definition until somebody restarted the plane.
	PipelineID string `json:"pipeline_id"`
	// InputMapping maps each of the pipeline's declared inputs to a field of
	// this trigger's own event. See internal/trigger's Binding.
	InputMapping map[string]string `json:"input_mapping,omitempty"`
	// Expression is a five- or six-field cron expression. Schedules only.
	Expression string `json:"expression,omitempty"`
	// Secret is the shared secret the forge signs with. Git triggers only,
	// and required for them: an endpoint that can verify nothing is an
	// unauthenticated pipeline trigger.
	Secret string `json:"secret,omitempty"`
	// Untrusted marks every value this trigger produces as tainted
	// (ADR 0015). Webhook endpoints are not authenticated, so an `http`
	// trigger reachable from outside should set it; `git` taints regardless.
	Untrusted bool `json:"untrusted,omitempty"`
}

// tenant is the scope this trigger's runs land in. There is no unscoped fire.
func (t TriggerSpec) tenant() string {
	if t.TenantID == "" {
		return DefaultTenant
	}
	return t.TenantID
}

// startTriggers builds every configured trigger and starts the ones that run
// on a loop. Webhook triggers are handlers instead, mounted by rootHandler on
// the listener the API already has.
//
// It runs BEFORE startAPI for exactly that reason, and it refuses rather than
// warns: a trigger that could not be configured is a pipeline nobody will ever
// notice is not running.
func (s *Server) startTriggers(startCtx, runCtx context.Context) error {
	for _, spec := range s.cfg.Triggers {
		if err := s.startTrigger(startCtx, runCtx, spec); err != nil {
			return fmt.Errorf("server: trigger %q: %w", spec.ID, err)
		}
	}
	return nil
}

func (s *Server) startTrigger(startCtx, runCtx context.Context, spec TriggerSpec) error {
	if spec.ID == "" {
		return errors.New("a trigger needs an id")
	}
	if spec.PipelineID == "" {
		return errors.New("a trigger needs the pipeline it drives")
	}
	binding := trigger.Binding{PipelineID: spec.PipelineID, InputMapping: spec.InputMapping}

	// The definition, resolved now, so a binding that names an input the
	// pipeline does not declare is refused at CONFIGURATION time rather than
	// at 3am inside the first step of a run that should never have started
	// (ADR 0007).
	pipeline, err := activePipeline(startCtx, s.defs, spec.tenant(), spec.PipelineID)
	if err != nil {
		return err
	}
	sink := s.triggerSink(spec, s.defs)

	switch spec.Kind {
	case schedule.Kind:
		cron, err := schedule.New(schedule.Config{
			ID:         spec.ID,
			TenantID:   spec.tenant(),
			Expression: spec.Expression,
			Binding:    binding,
			Pipeline:   pipeline,
			Store:      s.infra.store,
			OnError: func(err error) {
				s.log.Error("schedule trigger", "trigger", spec.ID, "error", err)
			},
			OnSkip: func(skip schedule.Skip) {
				s.log.Warn("schedule occurrence passed over",
					"trigger", spec.ID, "reason", skip.Reason)
			},
		})
		if err != nil {
			return err
		}
		s.spawn(func() {
			if err := cron.Start(runCtx, sink); err != nil && !errors.Is(err, context.Canceled) {
				s.log.Error("schedule trigger stopped", "trigger", spec.ID, "error", err)
			}
		})
		return nil

	case httptrigger.Kind:
		hook, err := httptrigger.New(httptrigger.Config{
			ID:        spec.ID,
			TenantID:  spec.tenant(),
			Binding:   binding,
			Pipeline:  pipeline,
			Untrusted: spec.Untrusted,
			OnError: func(err error) {
				s.log.Error("http trigger", "trigger", spec.ID, "error", err)
			},
		})
		if err != nil {
			return err
		}
		s.mountTrigger(spec.ID, hook.Handler(sink))
		return nil

	case gittrigger.Kind:
		hook, err := gittrigger.New(gittrigger.Config{
			ID:       spec.ID,
			TenantID: spec.tenant(),
			Secret:   spec.Secret,
			Binding:  binding,
			Pipeline: pipeline,
			OnError: func(err error) {
				s.log.Error("git trigger", "trigger", spec.ID, "error", err)
			},
		})
		if err != nil {
			return err
		}
		s.mountTrigger(spec.ID, hook.Handler(sink))
		return nil

	default:
		return fmt.Errorf("no trigger kind is registered for %q; known kinds are %s, %s and %s",
			spec.Kind, schedule.Kind, httptrigger.Kind, gittrigger.Kind)
	}
}

// mountTrigger records a webhook handler for rootHandler to serve. It is
// called while Start holds s.mu, before anything can serve a request.
func (s *Server) mountTrigger(id string, handler http.Handler) {
	if s.triggerMux == nil {
		s.triggerMux = http.NewServeMux()
	}
	s.triggerMux.Handle(TriggerPrefix+id, handler)
}

// triggerSink is what every trigger fires into: one run of the pipeline's
// ACTIVE revision, in the trigger's tenant.
//
// The bound inputs are logged and not carried into the run, because there is
// nowhere to carry them: nothing in the run log or the dispatch takes a
// pipeline input, so a trigger supplies a pipeline's declared inputs in name
// only (the other half of ADR 0007). Logging them is what makes that
// observable instead of silent.
func (s *Server) triggerSink(spec TriggerSpec, defs defstore.Store) trigger.Sink {
	return trigger.SinkFunc(func(
		ctx context.Context, tenantID, pipelineID string, inputs map[string]*structpb.Value,
	) error {
		pipeline, err := activePipeline(ctx, defs, tenantID, pipelineID)
		if err != nil {
			return err
		}
		runID, err := s.Submit(ctx, tenantID, pipeline)
		if err != nil {
			return err
		}
		s.log.Info("a trigger started a run",
			"trigger", spec.ID, "kind", spec.Kind, "tenant", tenantID,
			"pipeline", pipelineID, "run", runID, "inputs", strings.Join(inputNames(inputs), ","))
		return nil
	})
}

// activePipeline is the definition a trigger drives: the pipeline's active
// revision, read at the moment it is needed.
//
// The store is passed in rather than read back off the Server. startTriggers
// runs inside Start, which holds s.mu for its whole length, and a helper that
// reached for that lock would deadlock the plane against its own start-up.
func activePipeline(
	ctx context.Context, defs defstore.Store, tenantID, pipelineID string,
) (*dholev1.Pipeline, error) {
	if defs == nil {
		return nil, errors.New("server: no definition store")
	}
	revision, err := defs.Active(ctx, tenantID, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("resolving the active revision of pipeline %q: %w", pipelineID, err)
	}
	return defs.Get(ctx, tenantID, pipelineID, revision.ID)
}

func inputNames(inputs map[string]*structpb.Value) []string {
	names := make([]string, 0, len(inputs))
	for name := range inputs {
		names = append(names, name)
	}
	return names
}
