package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

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

// TriggerSpec is one configured event source, as a deployment DECLARES it in
// the plane's own `--triggers` file.
//
// It is no longer the only way in: PipelineService.CreateTrigger stores one in
// the `triggers` table, which is what lets an operator with a credential and
// no shell on this host create an event source at all (ADR 0013). The declared
// path stays because an operator mid-migration must not be broken by the
// arrival of the stored one, and a DECLARED trigger WINS over a stored row of
// the same id: this file is what the plane reads at every start, so a row
// could not change what the next restart does, and a trigger that fires or
// does not depending on a file the caller cannot see is worse than a refusal.
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

// triggerSyncInterval is how often the plane reconciles what it is running
// against the stored trigger table.
//
// A poll rather than a notification, because the plane that stores a trigger
// is routinely not the only plane that has to run it: two control planes over
// one database each learn about the other's triggers here, and a create that
// only told its own process would leave a webhook served by one plane out of
// two. It is one small primary-key query per tenant.
const triggerSyncInterval = time.Second

// liveTrigger is one trigger this process is currently running.
type liveTrigger struct {
	spec TriggerSpec
	// declared is true for a trigger from the `--triggers` file. Declared
	// triggers are never reconciled away: the file, not the table, is what
	// puts them there.
	declared bool
	// fingerprint is what a stored row is compared against to decide whether
	// it CHANGED. Without it an edited trigger would keep running its old
	// binding for as long as the plane stayed up.
	fingerprint string
	// stop ends a trigger that runs on a loop. Nil for a webhook, which is a
	// handler and stops being served the moment it leaves the mux.
	stop context.CancelFunc
	// handler is the webhook endpoint, nil for a schedule.
	handler http.Handler
}

// startTriggers builds every DECLARED trigger, brings the stored ones up, and
// starts the loop that keeps the two in agreement.
//
// It runs BEFORE startAPI because a webhook trigger is served on the listener
// the API already owns. The declared ones refuse rather than warn: a trigger
// that could not be configured is a pipeline nobody will ever notice is not
// running, and the file naming it was hand-written by whoever started this
// plane. A STORED one that cannot be configured is logged and skipped instead
// — its pipeline may have been deleted after it was created, and a plane that
// refused to start because of a row somebody wrote last month would be a plane
// an operator cannot recover without database access, which is the very thing
// this table exists to stop needing.
func (s *Server) startTriggers(startCtx, runCtx context.Context) error {
	for _, spec := range s.cfg.Triggers {
		if err := s.startTrigger(runCtx, spec, true); err != nil {
			return fmt.Errorf("server: trigger %q: %w", spec.ID, err)
		}
	}
	if err := s.reconcileTriggers(startCtx, runCtx); err != nil {
		s.log.Error("reading the stored triggers", "error", err)
	}
	s.spawn(func() { s.triggerLoop(runCtx) })
	return nil
}

// triggerLoop keeps this process's running triggers in step with the table.
func (s *Server) triggerLoop(runCtx context.Context) {
	ticker := time.NewTicker(triggerSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			return
		case <-ticker.C:
		}
		if err := s.reconcileTriggers(runCtx, runCtx); err != nil {
			if runCtx.Err() != nil {
				return
			}
			s.log.Error("reconciling the stored triggers", "error", err)
		}
	}
}

// reconcileTriggers starts the stored triggers that are not running, restarts
// the ones whose row changed, and stops the ones whose row is gone.
//
// It is what makes CreateTrigger mean something: a row nothing ever reads is a
// trigger an operator believes they created, and a delete that left the
// endpoint up is one they believe is gone.
func (s *Server) reconcileTriggers(readCtx, runCtx context.Context) error {
	store := s.triggerStore()
	if store == nil {
		return nil
	}
	desired := map[string]TriggerSpec{}
	for _, tenantID := range s.tenants() {
		stored, err := store.List(readCtx, tenantID)
		if err != nil {
			return fmt.Errorf("server: listing the triggers of tenant %q: %w", tenantID, err)
		}
		for _, spec := range stored {
			desired[tenantID+"/"+spec.ID] = fromStored(tenantID, spec)
		}
	}

	for key, spec := range desired {
		current, running := s.liveTrigger(key)
		switch {
		case running && current.declared:
			// The file wins. Said at debug level because this reconciles
			// every second, and a shadowed row would otherwise be a line a
			// second in the log for as long as it exists.
			s.log.Debug("a stored trigger is shadowed by one this plane declares",
				"trigger", spec.ID)
		case running && current.fingerprint == fingerprintOf(spec):
		default:
			if running {
				s.stopTrigger(key)
			}
			if err := s.startTrigger(runCtx, spec, false); err != nil {
				s.log.Error("starting a stored trigger", "trigger", spec.ID, "error", err)
			}
		}
	}

	for _, key := range s.liveTriggerKeys() {
		if _, wanted := desired[key]; wanted {
			continue
		}
		if live, ok := s.liveTrigger(key); ok && live.declared {
			continue
		}
		s.stopTrigger(key)
	}
	return nil
}

// startTrigger builds one trigger and starts it. runCtx bounds every loop it
// spawns; the per-trigger context under it is what a delete cancels.
func (s *Server) startTrigger(runCtx context.Context, spec TriggerSpec, declared bool) error {
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
	pipeline, err := activePipeline(runCtx, s.defs, spec.tenant(), spec.PipelineID)
	if err != nil {
		return err
	}
	sink := s.triggerSink(spec, s.defs)
	live := &liveTrigger{spec: spec, declared: declared, fingerprint: fingerprintOf(spec)}

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
		// Its own context under runCtx: a schedule deleted through the
		// contract has to stop firing without the plane restarting, and
		// cancelling runCtx would stop the plane.
		cronCtx, stop := context.WithCancel(runCtx)
		live.stop = stop
		s.spawn(func() {
			if err := cron.Start(cronCtx, sink); err != nil && !errors.Is(err, context.Canceled) {
				s.log.Error("schedule trigger stopped", "trigger", spec.ID, "error", err)
			}
		})

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
		live.handler = hook.Handler(sink)

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
		live.handler = hook.Handler(sink)

	default:
		return fmt.Errorf("no trigger kind is registered for %q; known kinds are %s, %s and %s",
			spec.Kind, schedule.Kind, httptrigger.Kind, gittrigger.Kind)
	}

	s.trigMu.Lock()
	defer s.trigMu.Unlock()
	if s.live == nil {
		s.live = map[string]*liveTrigger{}
	}
	s.live[spec.tenant()+"/"+spec.ID] = live
	s.rebuildTriggerMuxLocked()
	return nil
}

// stopTrigger ends one trigger and takes its endpoint off the listener.
func (s *Server) stopTrigger(key string) {
	s.trigMu.Lock()
	defer s.trigMu.Unlock()
	live, ok := s.live[key]
	if !ok {
		return
	}
	if live.stop != nil {
		live.stop()
	}
	delete(s.live, key)
	s.rebuildTriggerMuxLocked()
}

// rebuildTriggerMuxLocked rebuilds the webhook mux from what is running.
//
// It is rebuilt whole rather than edited because http.ServeMux has no way to
// unregister a pattern, and a deleted trigger whose handler stayed mounted
// would keep starting runs an operator believes they stopped.
func (s *Server) rebuildTriggerMuxLocked() {
	mux := http.NewServeMux()
	for _, live := range s.live {
		if live.handler == nil {
			continue
		}
		mux.Handle(TriggerPrefix+live.spec.ID, live.handler)
	}
	s.triggerMux = mux
}

// serveTrigger is what the root handler mounts under TriggerPrefix: the mux as
// it stands NOW, not as it stood when the listener was built. A trigger
// created through the contract has to be served without a restart, and a
// handler that closed over the mux at start-up never could be.
func (s *Server) serveTrigger(w http.ResponseWriter, r *http.Request) {
	s.trigMu.Lock()
	mux := s.triggerMux
	s.trigMu.Unlock()
	if mux == nil {
		http.NotFound(w, r)
		return
	}
	mux.ServeHTTP(w, r)
}

func (s *Server) liveTrigger(key string) (*liveTrigger, bool) {
	s.trigMu.Lock()
	defer s.trigMu.Unlock()
	live, ok := s.live[key]
	return live, ok
}

func (s *Server) liveTriggerKeys() []string {
	s.trigMu.Lock()
	defer s.trigMu.Unlock()
	keys := make([]string, 0, len(s.live))
	for key := range s.live {
		keys = append(keys, key)
	}
	return keys
}

// stopAllTriggers forgets every running trigger. The loops they started end
// with runCtx, which Stop cancels; this is what stops their endpoints being
// served and lets a second Start begin from nothing.
func (s *Server) stopAllTriggers() {
	s.trigMu.Lock()
	defer s.trigMu.Unlock()
	for _, live := range s.live {
		if live.stop != nil {
			live.stop()
		}
	}
	s.live = nil
	s.triggerMux = nil
}

// triggerStore is the stored trigger table, or nil before Start opened one.
func (s *Server) triggerStore() trigger.Store {
	s.trigMu.Lock()
	defer s.trigMu.Unlock()
	return s.triggers
}

// setTriggerStore is called once, by Start, before anything reconciles.
func (s *Server) setTriggerStore(store trigger.Store) {
	s.trigMu.Lock()
	defer s.trigMu.Unlock()
	s.triggers = store
}

// fromStored is a stored row as this file's spec. One vocabulary,
// deliberately: two shapes for one concept would let the file's meaning of
// `untrusted` and the table's drift apart.
func fromStored(tenantID string, spec trigger.Spec) TriggerSpec {
	return TriggerSpec{
		ID:           spec.ID,
		Kind:         spec.Kind,
		TenantID:     tenantID,
		PipelineID:   spec.PipelineID,
		InputMapping: spec.InputMapping,
		Expression:   spec.Expression,
		Secret:       spec.Secret,
		Untrusted:    spec.Untrusted,
	}
}

// declaredTriggers is what this plane's `--triggers` file declares, in the
// shape the API needs to refuse a create that would shadow one.
func (s *Server) declaredTriggers() []trigger.Spec {
	specs := make([]trigger.Spec, 0, len(s.cfg.Triggers))
	for _, spec := range s.cfg.Triggers {
		specs = append(specs, trigger.Spec{
			ID:           spec.ID,
			Kind:         spec.Kind,
			PipelineID:   spec.PipelineID,
			InputMapping: spec.InputMapping,
			Expression:   spec.Expression,
			Secret:       spec.Secret,
			Untrusted:    spec.Untrusted,
		})
	}
	return specs
}

// fingerprintOf is everything about a trigger that changes what it DOES. Two
// rows with the same fingerprint are the same trigger, and the reconciler
// leaves the running one alone rather than restarting it every second.
func fingerprintOf(spec TriggerSpec) string {
	names := make([]string, 0, len(spec.InputMapping))
	for name := range spec.InputMapping {
		names = append(names, name)
	}
	sort.Strings(names)
	var mapping strings.Builder
	for _, name := range names {
		fmt.Fprintf(&mapping, "%s=%s;", name, spec.InputMapping[name])
	}
	return fmt.Sprintf("%s|%s|%s|%s|%t|%s|%s|%s",
		spec.ID, spec.Kind, spec.tenant(), spec.PipelineID, spec.Untrusted,
		spec.Expression, mapping.String(), spec.Secret)
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
