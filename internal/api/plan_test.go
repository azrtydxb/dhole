package api_test

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/api"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/catalog"
	"github.com/azrtydxb/dhole/internal/dag"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// planEnvIdentity is the environment digest every plan in this file is
// computed against. It is a fixed string rather than a real executor's
// answer, because a cache key that changed with the machine would make every
// assertion here a coin toss.
//
// It reaches the plan the way it reaches a run: announced by the engines of
// planTier, on the instances the fleet reports (ADR 0021). A plan that took it
// from an executor of the API server's own answered a different question from
// the one the scheduler asks.
const planEnvIdentity = "sha256:env-under-test"

// planTier is the tier every plan in this file is computed for, and the tier
// its engines are in. They must be the same string: a plan for a tier with no
// engines in it finds no identity and reports every step uncacheable.
const planTier = "untrusted"

// planEngineKind is the executor backend the plan harness says its engines
// run.
const planEngineKind = "process"

// planEngineKindOther is a second backend kind, so a fleet in this file can be
// heterogeneous: a plan asserted against a fleet whose engines are all the
// same kind as the plane cannot show where the answer came from.
const planEngineKindOther = "container"

// staticFleet is a fleet that is simply a list. Matching itself is the real
// scheduler.Match, so nothing about the decision is faked here — only the
// registry lookup that would otherwise need a bus.
type staticFleet []registry.Instance

func (f staticFleet) Instances(_ context.Context, _ string) ([]registry.Instance, error) {
	return []registry.Instance(f), nil
}

// readyEngine is one instance that will take anything: ready, with slots, and
// on a platform it states.
func readyEngine(caps ...dholev1.Capability) registry.Instance {
	return registry.Instance{
		ID: "engine-1", State: registry.StateReady,
		OS: "linux", Arch: "amd64", Slots: 4,
		Capabilities:        caps,
		EngineTypes:         []string{planEngineKind},
		ProtocolVersions:    []uint32{1},
		Tier:                planTier,
		EnvironmentIdentity: planEnvIdentity,
	}
}

// planHarness is an API server wired for Validate and Plan: a real definition
// store, a real cache, and a fleet.
type planHarness struct {
	client dholev1connect.PipelineServiceClient
	defs   defstore.Store
	cache  *cache.Cache
}

func newPlanHarness(t *testing.T, fleet api.Fleet) *planHarness {
	t.Helper()
	return newPlanHarnessWith(t, fleet, nil)
}

func newPlanHarnessWith(t *testing.T, fleet api.Fleet, plugins api.StepResolver) *planHarness {
	t.Helper()

	c, err := cache.NewSQLite(filepath.Join(t.TempDir(), "cache.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	defs := sqliteDefs(t)
	runs, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = runs.Close() })

	srv, err := api.NewServer(api.Config{
		Definitions:  defs,
		Auth:         fakeAuth{},
		Runs:         runs,
		Cache:        c,
		Fleet:        fleet,
		Catalog:      plugins,
		Tier:         planTier,
		PollInterval: 2 * time.Millisecond,
	})
	require.NoError(t, err)

	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	return &planHarness{
		client: dholev1connect.NewPipelineServiceClient(httpSrv.Client(), httpSrv.URL),
		defs:   defs,
		cache:  c,
	}
}

// cacheablePipeline is two pure steps in a line, both cacheable: a produces a
// blob, b consumes it.
func cacheablePipeline(tenantID string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     "planned",
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{
			{
				Id: "a", Name: "fetch",
				EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
				LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
				Outputs:     []*dholev1.Port{blobPort("out")},
			},
			{
				Id: "b", Name: "build",
				EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
				LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
				Inputs:      []*dholev1.Port{blobPort("in")},
				Outputs:     []*dholev1.Port{blobPort("out")},
			},
		},
		Edges: []*dholev1.Edge{{FromStep: "a", FromPort: "out", ToStep: "b", ToPort: "in"}},
	}
}

// savePlanned stores a pipeline and returns it with its revision.
func savePlanned(
	t *testing.T, h *planHarness, tenantID string, p *dholev1.Pipeline,
) defstore.Revision {
	t.Helper()
	rev, err := h.defs.Save(context.Background(), tenantID, p, "alice")
	require.NoError(t, err)
	return rev
}

// stepByID finds one step of a pipeline.
func stepByID(t *testing.T, p *dholev1.Pipeline, id string) *dholev1.Step {
	t.Helper()
	for _, s := range p.GetSteps() {
		if s.GetId() == id {
			return s
		}
	}
	t.Fatalf("pipeline %q has no step %q", p.GetId(), id)
	return nil
}

// plannedByID finds one step of a plan.
func plannedByID(t *testing.T, steps []*dholev1.PlannedStep, id string) *dholev1.PlannedStep {
	t.Helper()
	for _, s := range steps {
		if s.GetStepId() == id {
			return s
		}
	}
	t.Fatalf("the plan has no step %q", id)
	return nil
}

// recordCacheEntry puts a step's outputs in the cache under the key the plan
// must compute for it. The key comes from cache.Key rather than from a
// constant: if Plan hashed anything else, this entry is unreachable and the
// hit it must report never happens.
func recordCacheEntry(
	t *testing.T,
	h *planHarness,
	tenantID string,
	step *dholev1.Step,
	inputs []*dholev1.Digest,
	lockfile map[string]string,
	outputs []*dholev1.OutputRef,
) {
	t.Helper()
	key, err := cache.Key(step, planEnvIdentity, inputs, lockfile)
	require.NoError(t, err)
	require.NoError(t, h.cache.Record(context.Background(), tenantID, key, outputs))
}

func digest(hex string) *dholev1.Digest {
	return &dholev1.Digest{Algo: "sha256", Hex: hex}
}

// TestPlanReportsCacheHitsAndEngineAssignment is what a person runs Plan for:
// what would actually execute, and where. A step already in the cache is
// reported as a hit, and every step says which kind of engine would take it.
func TestPlanReportsCacheHitsAndEngineAssignment(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine()})
	ctx := context.Background()

	p := cacheablePipeline(tenantA)
	rev := savePlanned(t, h, tenantA, p)

	// Step "a" has run before: its outputs are in the cache under the key its
	// definition hashes to.
	recordCacheEntry(t, h, tenantA, stepByID(t, p, "a"), nil, rev.Lockfile,
		[]*dholev1.OutputRef{{Port: "out", Digest: digest("aaaa")}})

	got, err := h.client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenAlice))
	require.NoError(t, err)

	steps := got.Msg.GetSteps()
	require.Len(t, steps, 2, "a plan must account for every step of the pipeline")
	require.Equal(t, "a", steps[0].GetStepId(),
		"a plan is in execution order, so a step's dependencies come before it")
	require.True(t, steps[0].GetCacheHit(),
		"step a's outputs are in the cache, so the plan must say it would be served from there")
	require.Empty(t, steps[0].GetNonCacheableReason(),
		"a cacheable step has no reason not to be cached")

	for _, s := range steps {
		require.NotEmpty(t, s.GetEngineKind(),
			"step %q lands nowhere: a plan that cannot say which engine takes a step "+
				"answers half the question it was asked", s.GetStepId())
	}
}

// TestPlanReportsNonCacheableReasonForPoolLease: a step that reuses a pooled
// sandbox is not cacheable, and the plan says so in the cache's own words. The
// expected string is asked of cache.Eligible rather than written out here: two
// copies of the wording drift, and the one a person reads would then stop
// matching the one that decides.
func TestPlanReportsNonCacheableReasonForPoolLease(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine()})
	ctx := context.Background()

	p := cacheablePipeline(tenantA)
	stepByID(t, p, "a").LeaseScope = dholev1.LeaseScope_LEASE_SCOPE_POOL
	rev := savePlanned(t, h, tenantA, p)

	cacheable, want := cache.Eligible(stepByID(t, p, "a"), executor.LeasePool, planEnvIdentity)
	require.False(t, cacheable)
	require.NotEmpty(t, want)

	got, err := h.client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenAlice))
	require.NoError(t, err)

	pooled := plannedByID(t, got.Msg.GetSteps(), "a")
	require.Equal(t, want, pooled.GetNonCacheableReason(),
		"the plan must report the cache's own reason, not a second wording of it")
	require.False(t, pooled.GetCacheHit(), "a step that may not be cached cannot be a cache hit")
}

// brokenPipeline is a definition whose edges cannot carry data: one names a
// port that does not exist on the target, the other a port that does not exist
// on the source.
func brokenPipeline(tenantID string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     "broken",
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{
			{
				Id: "a", EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
				Outputs: []*dholev1.Port{blobPort("out")},
			},
			{
				Id: "b", EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
				Inputs: []*dholev1.Port{blobPort("in")},
			},
		},
		Edges: []*dholev1.Edge{
			{FromStep: "a", FromPort: "out", ToStep: "b", ToPort: "missing"},
			{FromStep: "a", FromPort: "absent", ToStep: "b", ToPort: "in"},
		},
	}
}

// TestValidateSurfacesTypeErrorsWithPositions: a diagnostic with no position
// is a sentence in a log. The editor draws a marker on a port, so the step and
// the port have to arrive with it.
func TestValidateSurfacesTypeErrorsWithPositions(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine()})
	ctx := context.Background()

	p := brokenPipeline(tenantA)
	want := dag.TypeCheck(p)
	require.NotEmpty(t, want, "the fixture is supposed to be broken")

	got, err := h.client.Validate(ctx, authed(&dholev1.ValidateRequest{Pipeline: p}, tokenAlice))
	require.NoError(t, err)

	for _, w := range want {
		found := false
		for _, d := range got.Msg.GetDiagnostics() {
			if d.GetStepId() == w.StepID && d.GetPort() == w.PortName && d.GetMessage() == w.Message {
				found = true
				require.Equal(t, "error", d.GetSeverity(),
					"an edge that cannot carry data is an error, not advice")
			}
		}
		require.Truef(t, found,
			"no diagnostic positioned at %s.%s: got %v", w.StepID, w.PortName, got.Msg.GetDiagnostics())
	}
}

// TestValidateReportsEveryDiagnosticNotJustTheFirst: someone fixing a pipeline
// wants the whole list. Reporting one problem per round trip turns a broken
// definition into as many edit-save-validate cycles as it has mistakes.
func TestValidateReportsEveryDiagnosticNotJustTheFirst(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine()})

	p := brokenPipeline(tenantA)
	require.Len(t, dag.TypeCheck(p), 2, "the fixture must carry more than one problem")

	got, err := h.client.Validate(context.Background(),
		authed(&dholev1.ValidateRequest{Pipeline: p}, tokenAlice))
	require.NoError(t, err)

	positions := map[string]bool{}
	for _, d := range got.Msg.GetDiagnostics() {
		positions[d.GetStepId()+"."+d.GetPort()] = true
	}
	for _, w := range dag.TypeCheck(p) {
		require.Truef(t, positions[w.StepID+"."+w.PortName],
			"diagnostic at %s.%s was dropped: got %v", w.StepID, w.PortName, got.Msg.GetDiagnostics())
	}
}

// TestValidateReportsWhyNoEngineMatchesAStep: a step nothing can run is the
// worst failure this system has — it never fails, it simply never happens. The
// reason is the scheduler's own Explain, so the sentence a person reads before
// the run is the sentence the run's log would have carried.
func TestValidateReportsWhyNoEngineMatchesAStep(t *testing.T) {
	fleet := staticFleet{readyEngine()} // ready, and grants nothing
	h := newPlanHarness(t, fleet)

	p := cacheablePipeline(tenantA)
	stepByID(t, p, "a").Capabilities = []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED}
	want := scheduler.Explain(
		executor.Requirements{Capabilities: stepByID(t, p, "a").GetCapabilities()},
		[]registry.Instance(fleet))
	require.NotEmpty(t, want)

	got, err := h.client.Validate(context.Background(),
		authed(&dholev1.ValidateRequest{Pipeline: p}, tokenAlice))
	require.NoError(t, err)

	found := false
	for _, d := range got.Msg.GetDiagnostics() {
		if d.GetStepId() == "a" && strings.Contains(d.GetMessage(), want) {
			found = true
		}
	}
	require.Truef(t, found,
		"a step no engine can take must say why, in the scheduler's words (%q); got %v",
		want, got.Msg.GetDiagnostics())
}

// TestPlanOnAPipelineWithACycleReportsTheCycle: a cyclic pipeline has no
// execution order at all. Answering with the part that happens to be
// orderable, or walking the graph until the deadline, are the two ways to get
// this wrong.
func TestPlanOnAPipelineWithACycleReportsTheCycle(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine()})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p := cacheablePipeline(tenantA)
	p.Edges = append(p.GetEdges(), &dholev1.Edge{
		FromStep: "b", FromPort: "out", ToStep: "a", ToPort: "in",
	})
	stepByID(t, p, "a").Inputs = []*dholev1.Port{blobPort("in")}
	rev := savePlanned(t, h, tenantA, p)

	got, err := h.client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenAlice))
	require.Error(t, err, "a cyclic pipeline has no plan; a partial one would be believed")
	require.Nil(t, got)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.Contains(t, err.Error(), "cycle")
	require.Contains(t, err.Error(), "a")
	require.Contains(t, err.Error(), "b")
}

// TestValidateAndPlanRefuseAnotherTenantsPipeline: both endpoints are scoped
// by the credential like every other one. A revision of another tenant is
// indistinguishable from one that does not exist.
func TestValidateAndPlanRefuseAnotherTenantsPipeline(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine()})
	ctx := context.Background()

	p := cacheablePipeline(tenantA)
	rev := savePlanned(t, h, tenantA, p)

	_, err := h.client.Validate(ctx, authed(&dholev1.ValidateRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenBob))
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	_, err = h.client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenBob))
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// TestPlanReportsACacheHitForAWholePipeline: the answer a person actually
// wants from Plan is "nothing would run". It is only reachable if a hit's
// recorded output digests are fed into the next step's key, exactly as a real
// run would feed the bytes forward.
func TestPlanReportsACacheHitForAWholePipeline(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine()})
	ctx := context.Background()

	p := cacheablePipeline(tenantA)
	rev := savePlanned(t, h, tenantA, p)

	aOut := digest("aaaa")
	recordCacheEntry(t, h, tenantA, stepByID(t, p, "a"), nil, rev.Lockfile,
		[]*dholev1.OutputRef{{Port: "out", Digest: aOut}})
	recordCacheEntry(t, h, tenantA, stepByID(t, p, "b"), []*dholev1.Digest{aOut}, rev.Lockfile,
		[]*dholev1.OutputRef{{Port: "out", Digest: digest("bbbb")}})

	got, err := h.client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenAlice))
	require.NoError(t, err)

	for _, s := range got.Msg.GetSteps() {
		require.Truef(t, s.GetCacheHit(),
			"step %q would run again although its work is recorded under the key its "+
				"inputs hash to", s.GetStepId())
	}
}

// dispatchHarness is the API server on top of a REAL dispatch path: an
// embedded bus, an outbox draining onto it, and a scheduler behind StartRun.
//
// It exists for one assertion, and the assertion is only worth anything
// because the path is real: watching a fake dispatcher not being called tests
// the fake. Watching the subject an engine listens on tests the system.
type dispatchHarness struct {
	*planHarness
	seen chan string
}

func newDispatchHarness(ctx context.Context, t *testing.T) *dispatchHarness {
	t.Helper()

	embedded, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(embedded.Close)

	plane, err := bus.Connect(ctx, embedded.URL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)
	require.NoError(t, plane.EnsureWorkQueue(ctx, engine.DispatchStream, []string{"job.dispatch.>"}))

	conn, err := nats.Connect(embedded.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	runs, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = runs.Close() })

	defs := sqliteDefs(t)
	out := outbox.New(runs, plane, "test-plane")
	fleet := staticFleet{readyEngine()}
	sched, err := scheduler.New(scheduler.Config{
		Store: runs, Outbox: out, Leases: leases, Fleet: fleet,
		Definitions: defs, Tier: planTier,
	})
	require.NoError(t, err)

	// Draining is what turns an enqueued dispatch into a message on the bus.
	// Without it, silence would prove nothing at all.
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		_ = out.Run(ctx)
	}()
	t.Cleanup(func() { <-drained })

	c, err := cache.NewSQLite(filepath.Join(t.TempDir(), "cache.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	srv, err := api.NewServer(api.Config{
		Definitions: defs, Auth: fakeAuth{}, Runs: runs, Advancer: sched,
		Cache: c, Fleet: fleet,
		Tier:         planTier,
		PollInterval: 2 * time.Millisecond,
	})
	require.NoError(t, err)
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	// A third party on the wildcard, subscribed before anything happens. It is
	// core NATS, so it cannot steal the dispatch from the work queue.
	observer, err := bus.Connect(ctx, embedded.URL())
	require.NoError(t, err)
	t.Cleanup(observer.Close)
	seen := make(chan string, 8)
	stop, err := observer.SubscribeEphemeralOnSubjects(ctx, "job.dispatch.>",
		func(subject string, _ []byte) {
			select {
			case seen <- subject:
			default:
			}
		})
	require.NoError(t, err)
	t.Cleanup(stop)

	return &dispatchHarness{
		planHarness: &planHarness{
			client: dholev1connect.NewPipelineServiceClient(httpSrv.Client(), httpSrv.URL),
			defs:   defs,
			cache:  c,
		},
		seen: seen,
	}
}

// TestPlanDoesNotDispatchAnything is the whole point of the endpoint. Plan is
// what a person runs to find out what WOULD happen; if it dispatched, asking
// the question would change the answer.
func TestPlanDoesNotDispatchAnything(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	h := newDispatchHarness(ctx, t)

	p := cacheablePipeline(tenantA)
	rev := savePlanned(t, h.planHarness, tenantA, p)
	require.NoError(t, h.defs.Approve(ctx, tenantA, rev.ID, "carol"))

	_, err := h.client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenAlice))
	require.NoError(t, err)

	select {
	case subject := <-h.seen:
		t.Fatalf("Plan published to %s: asking what would happen made it happen", subject)
	case <-time.After(750 * time.Millisecond):
	}

	// And the watcher is not simply deaf: a real run does reach the subject.
	// Without this half, a Plan that dispatched into a broken subscription
	// would pass the assertion above.
	_, err = h.client.StartRun(ctx, authed(&dholev1.StartRunRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenAlice))
	require.NoError(t, err)

	select {
	case subject := <-h.seen:
		require.True(t, strings.HasPrefix(subject, "job.dispatch."),
			"a dispatch arrived on %s", subject)
	case <-time.After(30 * time.Second):
		t.Fatal("no dispatch reached the bus for a real run: this test cannot see one at all, " +
			"so its silence during Plan proves nothing")
	}
}

// TestPlanRecordsNothingInTheCache: the cache is READ by a plan and never
// written. A plan that recorded what it looked up would make the second plan
// of an unrun pipeline claim the work was already done.
func TestPlanRecordsNothingInTheCache(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine()})
	ctx := context.Background()

	p := cacheablePipeline(tenantA)
	rev := savePlanned(t, h, tenantA, p)

	got, err := h.client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenAlice))
	require.NoError(t, err)
	require.False(t, plannedByID(t, got.Msg.GetSteps(), "a").GetCacheHit(),
		"nothing has run, so nothing can be cached")

	// Asked directly of the store, not of a second plan: a Plan that wrote
	// would otherwise be caught only if it also read its own writing back.
	key, err := cache.Key(stepByID(t, p, "a"), planEnvIdentity, nil, rev.Lockfile)
	require.NoError(t, err)
	_, found, err := h.cache.Lookup(ctx, tenantA, key)
	require.NoError(t, err)
	require.False(t, found,
		"Plan recorded a cache entry: asking what would happen made the next answer wrong")
}

// TestPlanDoesNotSeeAnotherTenantsCacheEntry: cache entries are scoped like
// every other record. One tenant's outputs served as another's answer is the
// worst thing a cache can do.
func TestPlanDoesNotSeeAnotherTenantsCacheEntry(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine()})
	ctx := context.Background()

	p := cacheablePipeline(tenantA)
	rev := savePlanned(t, h, tenantA, p)
	recordCacheEntry(t, h, tenantB, stepByID(t, p, "a"), nil, rev.Lockfile,
		[]*dholev1.OutputRef{{Port: "out", Digest: digest("aaaa")}})

	got, err := h.client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenAlice))
	require.NoError(t, err)
	require.False(t, plannedByID(t, got.Msg.GetSteps(), "a").GetCacheHit(),
		"the entry belongs to another tenant and must be invisible here")
}

// TestPlanLeavesEngineKindEmptyWhenNothingMatches: naming an engine kind for a
// step nothing can take would promise a landing place that does not exist.
func TestPlanLeavesEngineKindEmptyWhenNothingMatches(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine()}) // grants no capability
	ctx := context.Background()

	p := cacheablePipeline(tenantA)
	stepByID(t, p, "a").Capabilities = []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED}
	rev := savePlanned(t, h, tenantA, p)

	got, err := h.client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenAlice))
	require.NoError(t, err)

	require.Empty(t, plannedByID(t, got.Msg.GetSteps(), "a").GetEngineKind(),
		"no engine advertises what step a needs, so nothing may be named as its landing place")
	require.NotEmpty(t, plannedByID(t, got.Msg.GetSteps(), "b").GetEngineKind(),
		"step b asks for nothing special and does land somewhere")
}

// TestValidateReportsPluginProblems: a step is half a declaration — the other
// half is its plugin's. A reference that resolves to nothing, and a step that
// widens its plugin's effect class into something cacheable and freely
// retried, are both things a person wants told before the run rather than
// after the second charge on the card (ADR 0002).
func TestValidateReportsPluginProblems(t *testing.T) {
	ctx := context.Background()

	plugins, err := catalog.NewSQLite(filepath.Join(t.TempDir(), "catalog.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = plugins.Close() })
	require.NoError(t, plugins.Publish(ctx, tenantA, catalog.Manifest{
		Namespace: "acme", Name: "charge", Version: "1.0.0",
		Digest:      digest("cccc"),
		Kind:        catalog.KindStep,
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		InputSchema: []byte(`{"type":"object"}`), OutputSchema: []byte(`{"type":"object"}`),
	}))

	h := newPlanHarnessWith(t, staticFleet{readyEngine()}, plugins)

	p := cacheablePipeline(tenantA)
	// a widens an at-most-once plugin to pure; b names something nobody
	// published at all.
	stepByID(t, p, "a").PluginRef = "acme/charge@1.0.0"
	stepByID(t, p, "b").PluginRef = "acme/missing@1.0.0"

	got, err := h.client.Validate(ctx, authed(&dholev1.ValidateRequest{Pipeline: p}, tokenAlice))
	require.NoError(t, err)

	var widened, unresolved *dholev1.Diagnostic
	for _, d := range got.Msg.GetDiagnostics() {
		switch d.GetStepId() {
		case "a":
			if strings.Contains(d.GetMessage(), "widens") {
				widened = d
			}
		case "b":
			if strings.Contains(d.GetMessage(), "acme/missing@1.0.0") {
				unresolved = d
			}
		}
	}
	require.NotNilf(t, widened, "a widened effect class must be reported: %v", got.Msg.GetDiagnostics())
	require.Equal(t, "warning", widened.GetSeverity(),
		"the override is honoured, so it is a warning; it just never happens quietly")
	require.NotNilf(t, unresolved, "a step naming an unpublished plugin must be reported: %v",
		got.Msg.GetDiagnostics())
	require.Equal(t, "error", unresolved.GetSeverity())
}

// tenantProbe records the tenant every collaborator was asked with. It
// enforces nothing, which is the point: a collaborator that refused the wrong
// tenant would answer the scoping question on the server's behalf and hide a
// server that had stopped asking with the principal's tenant at all.
type tenantProbe struct {
	mu      sync.Mutex
	tenants []string
}

func (p *tenantProbe) note(tenantID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tenants = append(p.tenants, tenantID)
}

func (p *tenantProbe) seen() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.tenants...)
}

type probingFleet struct {
	probe *tenantProbe
	fleet staticFleet
}

func (f probingFleet) Instances(_ context.Context, tenantID string) ([]registry.Instance, error) {
	f.probe.note(tenantID)
	return []registry.Instance(f.fleet), nil
}

type probingCache struct {
	probe *tenantProbe
}

func (c probingCache) Lookup(
	_ context.Context, tenantID string, _ *dholev1.Digest,
) ([]*dholev1.OutputRef, bool, error) {
	c.probe.note(tenantID)
	return nil, false, nil
}

// TestValidateAndPlanReachEveryCollaboratorWithThePrincipalsTenant: the tenant
// a call is served under comes from the credential and from nowhere else.
// Comparing it against stores that enforce nothing is what makes a server that
// had stopped scoping visible, rather than a store quietly covering for it.
func TestValidateAndPlanReachEveryCollaboratorWithThePrincipalsTenant(t *testing.T) {
	ctx := context.Background()
	probe := &tenantProbe{}
	defs := newRecordingDefs()

	srv, err := api.NewServer(api.Config{
		Definitions: defs,
		Auth:        fakeAuth{},
		Cache:       probingCache{probe: probe},
		Fleet:       probingFleet{probe: probe, fleet: staticFleet{readyEngine()}},
		Tier:        planTier,
	})
	require.NoError(t, err)
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	client := dholev1connect.NewPipelineServiceClient(httpSrv.Client(), httpSrv.URL)

	p := cacheablePipeline(tenantB)
	rev, err := defs.Save(ctx, tenantB, p, "bob")
	require.NoError(t, err)
	before := len(defs.seenTenants())

	_, err = client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenBob))
	require.NoError(t, err)
	_, err = client.Validate(ctx, authed(&dholev1.ValidateRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenBob))
	require.NoError(t, err)

	stores := defs.seenTenants()[before:]
	require.NotEmpty(t, stores, "neither endpoint reached the definition store at all")
	for _, got := range stores {
		require.Equal(t, tenantB, got, "the server read a definition under a tenant that is not the caller's")
	}
	require.NotEmpty(t, probe.seen(), "neither endpoint consulted the fleet or the cache")
	for _, got := range probe.seen() {
		require.Equal(t, tenantB, got, "the server asked a collaborator under the wrong tenant")
	}
}

// TestPlanReportsTheMatchedEnginesKindNotTheLocalOne is the heterogeneous
// fleet: one process engine and one container engine, on a plane whose own
// configured environment is a process executor.
//
// A plan that reports the LOCAL kind answers "process" for every step,
// including the one only the container engine can take. That is right by
// accident on a uniform fleet and wrong on any other — which is exactly the
// fleet somebody asks the question about.
func TestPlanReportsTheMatchedEnginesKindNotTheLocalOne(t *testing.T) {
	// engine-a is the same kind as the plane's local environment; engine-b is
	// not, and is the only one advertising PRIVILEGED.
	fleet := staticFleet{
		{
			ID: "engine-a", State: registry.StateReady,
			OS: "linux", Arch: "amd64", Slots: 4,
			EngineTypes:      []string{planEngineKind},
			ProtocolVersions: []uint32{1},
		},
		{
			ID: "engine-b", State: registry.StateReady,
			OS: "linux", Arch: "amd64", Slots: 4,
			Capabilities:     []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED},
			EngineTypes:      []string{planEngineKindOther},
			ProtocolVersions: []uint32{1},
		},
	}
	h := newPlanHarness(t, fleet)
	ctx := context.Background()

	p := cacheablePipeline(tenantA)
	stepByID(t, p, "a").Capabilities = []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED}
	rev := savePlanned(t, h, tenantA, p)

	got, err := h.client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: p.GetId(), RevisionId: rev.ID,
	}, tokenAlice))
	require.NoError(t, err)

	require.Equal(t, planEngineKindOther, plannedByID(t, got.Msg.GetSteps(), "a").GetEngineKind(),
		"only the container engine advertises what step a needs, so the plan must name "+
			"the kind of the engine that matched — not the kind this plane happens to run")
	require.Equal(t, planEngineKind, plannedByID(t, got.Msg.GetSteps(), "b").GetEngineKind(),
		"step b needs nothing special and would land on the first engine that matches, "+
			"which is the process one")
}

// TestValidateReportsAnEngineTypeNoEngineOffers. The planner and the
// dispatcher must read the engine type off the same field, or a pipeline
// validates against engines that will never take its steps — the disagreement
// this filter was withheld for until a step could name the type itself.
func TestValidateReportsAnEngineTypeNoEngineOffers(t *testing.T) {
	fleet := staticFleet{readyEngine()} // ready, and offers no engine kind
	h := newPlanHarness(t, fleet)

	p := cacheablePipeline(tenantA)
	stepByID(t, p, "a").EngineType = "kubernetes"

	got, err := h.client.Validate(context.Background(),
		authed(&dholev1.ValidateRequest{Pipeline: p}, tokenAlice))
	require.NoError(t, err)

	found := false
	for _, d := range got.Msg.GetDiagnostics() {
		if d.GetStepId() == "a" &&
			strings.Contains(d.GetMessage(), "no ready engine offers engine type kubernetes") {
			found = true
		}
	}
	require.Truef(t, found,
		"a step naming an engine type nothing offers must say so before the run: got %v",
		got.Msg.GetDiagnostics())
}

// TestADeclaredFileIsPartOfTheStepsCacheKey is the property ADR 0023 gets for
// free by making a file an INPUT like any other: the file's digest is folded
// into the key, so replacing the file is a cache MISS rather than a hit that
// serves the outputs of bytes nobody is running any more.
//
// It asserts the miss and the hit in one pass, because either alone would pass
// for the wrong reason: a key that ignored files entirely would report the hit
// and fail the miss, and a key nobody could compute would report the miss and
// fail the hit.
func TestADeclaredFileIsPartOfTheStepsCacheKey(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine()})
	ctx := context.Background()

	withFile := func(hexDigest string) *dholev1.Pipeline {
		p := &dholev1.Pipeline{
			Id:     "planned-files",
			Tenant: &dholev1.Tenant{Id: tenantA},
			Steps: []*dholev1.Step{{
				Id: "build", Name: "build",
				EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
				LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
				Inputs:      []*dholev1.Port{blobPort("context")},
				Outputs:     []*dholev1.Port{blobPort("image")},
				FileInputs:  []*dholev1.FileInput{{Port: "context", Path: "Dockerfile"}},
			}},
			Files: []*dholev1.File{{
				Path: "Dockerfile", Digest: digest(hexDigest), SizeBytes: 4,
			}},
		}
		return p
	}

	first := withFile("1111")
	rev := savePlanned(t, h, tenantA, first)
	// The step has run once, against the bytes the definition declares.
	recordCacheEntry(t, h, tenantA, stepByID(t, first, "build"),
		[]*dholev1.Digest{digest("1111")}, rev.Lockfile,
		[]*dholev1.OutputRef{{Port: "image", Digest: digest("aaaa")}})

	planned, err := h.client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: first.GetId(), RevisionId: rev.ID,
	}, tokenAlice))
	require.NoError(t, err)
	require.True(t, plannedByID(t, planned.Msg.GetSteps(), "build").GetCacheHit(),
		"the step declares exactly the file it ran against, so its key must be the one that was recorded")

	// The same step, the same everything, a different file.
	second := withFile("2222")
	revTwo := savePlanned(t, h, tenantA, second)
	require.NotEqual(t, rev.ID, revTwo.ID, "changing a carried file left the revision unchanged")

	planned, err = h.client.Plan(ctx, authed(&dholev1.PlanRequest{
		PipelineId: second.GetId(), RevisionId: revTwo.ID,
	}, tokenAlice))
	require.NoError(t, err)
	require.False(t, plannedByID(t, planned.Msg.GetSteps(), "build").GetCacheHit(),
		"a step whose file changed was served from the cache of the file it no longer reads")
	require.Empty(t, plannedByID(t, planned.Msg.GetSteps(), "build").GetNonCacheableReason(),
		"the step is perfectly cacheable; it has simply never run with these bytes")
}

// TestValidateReportsAFileBindingThatNamesNothing: the editor draws markers on
// the ports it is told about, and a step bound to a file the definition does
// not carry is a port that will have nothing on it. Reported here, before a
// run, rather than as an engine failing to fetch an input.
func TestValidateReportsAFileBindingThatNamesNothing(t *testing.T) {
	h := newPlanHarness(t, staticFleet{readyEngine()})

	p := cacheablePipeline(tenantA)
	stepByID(t, p, "b").FileInputs = []*dholev1.FileInput{{Port: "in", Path: "Dockerfile"}}

	got, err := h.client.Validate(context.Background(), authed(&dholev1.ValidateRequest{
		Pipeline: p,
	}, tokenAlice))
	require.NoError(t, err)

	var found *dholev1.Diagnostic
	for _, d := range got.Msg.GetDiagnostics() {
		if strings.Contains(d.GetMessage(), "Dockerfile") {
			found = d
		}
	}
	require.NotNil(t, found, "a binding naming no file was not reported: %v", got.Msg.GetDiagnostics())
	require.Equal(t, "error", found.GetSeverity())
	require.Equal(t, "b", found.GetStepId())
	require.Equal(t, "in", found.GetPort(),
		"the diagnostic must name the port, or the editor cannot draw it")
}
