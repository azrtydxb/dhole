package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

const (
	testTenant   = "acme"
	testRun      = "run-1"
	testPipeline = "diamond"
	testRevision = "rev-1"
	testTier     = "trusted"
)

// recordingBus stands in for NATS on the PUBLISH side only. The parts of this
// package that have to be exercised for real — the fence, the claim race, the
// transaction that couples an event to its dispatch — are the real lease
// manager over an embedded NATS and the real SQLite store, because a fake of
// either would decide the outcome of exactly the tests that matter.
type recordingBus struct {
	mu        sync.Mutex
	published []publication
	fail      error
}

type publication struct {
	subject string
	payload []byte
}

func (b *recordingBus) Publish(_ context.Context, subject string, msg proto.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail != nil {
		return b.fail
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	b.published = append(b.published, publication{subject: subject, payload: payload})
	return nil
}

func (b *recordingBus) Request(context.Context, string, proto.Message, proto.Message) error {
	panic("recordingBus: Request is not used by the outbox")
}

func (b *recordingBus) SubscribePull(context.Context, string, string, string) (bus.Subscription, error) {
	panic("recordingBus: SubscribePull is not used by the outbox")
}

func (b *recordingBus) SubscribeEphemeral(context.Context, string, func([]byte)) (func(), error) {
	panic("recordingBus: SubscribeEphemeral is not used by the outbox")
}

// dispatches drains everything published so far into JobDispatch messages.
func (b *recordingBus) dispatches(t *testing.T) []*dholev1.JobDispatch {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*dholev1.JobDispatch, 0, len(b.published))
	for _, p := range b.published {
		d := &dholev1.JobDispatch{}
		require.NoError(t, proto.Unmarshal(p.payload, d))
		out = append(out, d)
	}
	return out
}

// staticFleet is the engine registry the scheduler matches against. Task 31
// replaces it with the KV-backed one; nothing here depends on where the list
// came from.
type staticFleet struct {
	instances []registry.Instance
}

func (f staticFleet) Instances(context.Context, string) ([]registry.Instance, error) {
	return f.instances, nil
}

// staticDefs resolves the pipeline revision a run pinned.
type staticDefs struct {
	pipeline *dholev1.Pipeline
}

func (d staticDefs) Get(_ context.Context, tenantID, pipelineID, revisionID string) (*dholev1.Pipeline, error) {
	if tenantID == "" {
		return nil, runstore.ErrTenantRequired
	}
	if pipelineID != testPipeline || revisionID != testRevision {
		return nil, runstore.ErrTenantRequired
	}
	return d.pipeline, nil
}

// diamond is the Task 3 pipeline: b and c depend on a, d depends on both.
func diamond() *dholev1.Pipeline {
	step := func(id string, ins, outs []string) *dholev1.Step {
		s := &dholev1.Step{
			Id:          id,
			Name:        id,
			PluginRef:   "cmd://echo",
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
		}
		for _, p := range ins {
			s.Inputs = append(s.Inputs, &dholev1.Port{Name: p})
		}
		for _, p := range outs {
			s.Outputs = append(s.Outputs, &dholev1.Port{Name: p})
		}
		return s
	}
	return &dholev1.Pipeline{
		Id:     testPipeline,
		Tenant: &dholev1.Tenant{Id: testTenant},
		Steps: []*dholev1.Step{
			step("a", nil, []string{"out"}),
			step("b", []string{"in"}, []string{"out"}),
			step("c", []string{"in"}, []string{"out"}),
			step("d", []string{"in1", "in2"}, nil),
		},
		Edges: []*dholev1.Edge{
			{FromStep: "a", FromPort: "out", ToStep: "b", ToPort: "in"},
			{FromStep: "a", FromPort: "out", ToStep: "c", ToPort: "in"},
			{FromStep: "b", FromPort: "out", ToStep: "d", ToPort: "in1"},
			{FromStep: "c", FromPort: "out", ToStep: "d", ToPort: "in2"},
		},
	}
}

// privilegedDiamond is the same pipeline with its root step demanding a
// capability no engine in these tests advertises.
func privilegedDiamond() *dholev1.Pipeline {
	p := diamond()
	p.GetSteps()[0].Capabilities = []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED}
	return p
}

// readyEngine is an instance that can run every step of the diamond.
func readyEngine(id string) registry.Instance {
	return registry.Instance{
		ID:               id,
		State:            registry.StateReady,
		OS:               "linux",
		Arch:             "amd64",
		Slots:            4,
		ProtocolVersions: []uint32{1},
	}
}

type harness struct {
	store  runstore.Store
	bus    *recordingBus
	outbox *outbox.Outbox
	leases *lease.KV
	sched  *scheduler.Scheduler
	url    string
	fleet  staticFleet
	defs   staticDefs
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newHarness(ctx context.Context, t *testing.T, instances ...registry.Instance) *harness {
	t.Helper()
	return newHarnessWith(ctx, t, diamond(), instances...)
}

func newHarnessWith(
	ctx context.Context, t *testing.T, pipeline *dholev1.Pipeline, instances ...registry.Instance,
) *harness {
	t.Helper()

	store, err := runstore.NewSQLite(t.TempDir() + "/run.db")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	conn, err := nats.Connect(srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	recorder := &recordingBus{}
	ob := outbox.New(store, recorder)
	fleet := staticFleet{instances: instances}
	defs := staticDefs{pipeline: pipeline}

	sched, err := scheduler.New(scheduler.Config{
		Store:       store,
		Outbox:      ob,
		Leases:      leases,
		Fleet:       fleet,
		Definitions: defs,
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		EnvIdentity: "sha256:env",
	})
	require.NoError(t, err)

	h := &harness{
		store: store, bus: recorder, outbox: ob, leases: leases,
		sched: sched, url: srv.URL(), fleet: fleet, defs: defs,
	}
	h.seedRun(ctx, t)
	return h
}

// seedRun writes the RUN_CREATED event a run starts from. There is no other
// state anywhere: the scheduler's whole position in the run is this log.
func (h *harness) seedRun(ctx context.Context, t *testing.T) {
	t.Helper()
	payload, err := scheduler.MarshalRunCreated(scheduler.RunCreated{
		PipelineID: testPipeline,
		RevisionID: testRevision,
	})
	require.NoError(t, err)
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID:    testRun,
		Sequence: 1,
		Type:     runstore.RunCreated,
		Payload:  payload,
		At:       time.Now().UTC(),
	}))
}

// drain publishes whatever the scheduler owed and returns the step ids that
// reached the bus, in order.
func (h *harness) drain(ctx context.Context, t *testing.T) []string {
	t.Helper()
	for {
		n, err := h.outbox.Drain(ctx)
		require.NoError(t, err)
		if n == 0 {
			break
		}
	}
	var ids []string
	for _, d := range h.bus.dispatches(t) {
		ids = append(ids, d.GetStepId())
	}
	return ids
}

// secondPlane is another control plane over the same store and the same bus:
// its own lease manager on its own connection, exactly as a second process
// would have. Nothing is shared in memory between the two.
func (h *harness) secondPlane(ctx context.Context, t *testing.T) *scheduler.Scheduler {
	t.Helper()

	conn, err := nats.Connect(h.url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	sched, err := scheduler.New(scheduler.Config{
		Store:       h.store,
		Outbox:      h.outbox,
		Leases:      leases,
		Fleet:       h.fleet,
		Definitions: h.defs,
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		EnvIdentity: "sha256:env",
	})
	require.NoError(t, err)
	return sched
}

// planeWithLeases is another control plane sharing everything but its lease
// manager, so a test can slow one side of a race down to a fixed order.
func (h *harness) planeWithLeases(
	_ context.Context, t *testing.T, leases lease.Manager,
) *scheduler.Scheduler {
	t.Helper()
	sched, err := scheduler.New(scheduler.Config{
		Store:       h.store,
		Outbox:      h.outbox,
		Leases:      leases,
		Fleet:       h.fleet,
		Definitions: h.defs,
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		EnvIdentity: "sha256:env",
	})
	require.NoError(t, err)
	return sched
}

// countEvents counts one kind of event for one step in the run's log.
func (h *harness) countEvents(
	ctx context.Context, t *testing.T, kind runstore.EventType, stepID string,
) int {
	t.Helper()
	events, err := h.store.Replay(ctx, testTenant, testRun)
	require.NoError(t, err)
	n := 0
	for _, e := range events {
		if e.Type == kind && e.StepID == stepID {
			n++
		}
	}
	return n
}

// dispatchPayload reads back what was recorded alongside a step's dispatch.
func (h *harness) dispatchPayload(
	ctx context.Context, t *testing.T, stepID string,
) scheduler.Dispatched {
	t.Helper()
	events, err := h.store.Replay(ctx, testTenant, testRun)
	require.NoError(t, err)
	for _, e := range events {
		if e.Type != runstore.StepDispatched || e.StepID != stepID {
			continue
		}
		payload, err := scheduler.UnmarshalDispatched(e.Payload)
		require.NoError(t, err)
		return payload
	}
	t.Fatalf("step %q has no STEP_DISPATCHED event", stepID)
	return scheduler.Dispatched{}
}

// succeed reports the step finished, exactly as an engine would: with the
// fence it was dispatched under.
func (h *harness) succeed(ctx context.Context, t *testing.T, stepID string) {
	t.Helper()
	for _, d := range h.bus.dispatches(t) {
		if d.GetStepId() != stepID {
			continue
		}
		require.NoError(t, h.sched.OnStatus(ctx, &dholev1.JobStatus{
			RunId:      d.GetRunId(),
			StepId:     d.GetStepId(),
			Attempt:    d.GetAttempt(),
			FenceToken: d.GetFenceToken(),
			Phase:      dholev1.Phase_PHASE_SUCCEEDED,
			Outputs: []*dholev1.OutputRef{
				{Port: "out", Digest: &dholev1.Digest{Algo: "sha256", Hex: "beef"}},
			},
		}))
		return
	}
	t.Fatalf("step %q was never dispatched", stepID)
}

// TestAdvanceDispatchesOnlyReadySteps is the property that separates a
// scheduler from a for-loop: readiness comes from the DAG and the run's event
// log, so a step whose predecessors have not finished is not sent anywhere.
// Dispatching d early would run it against inputs that do not exist yet.
func TestAdvanceDispatchesOnlyReadySteps(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(ctx, t, readyEngine("e1"))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t),
		"only the root of the diamond is ready before anything has run")

	h.succeed(ctx, t, "a")
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a", "b", "c"}, h.drain(ctx, t),
		"b and c became ready when a succeeded; d still waits on both of them")
}

// TestMatchFiltersByCapabilityOSAndArch pins the three axes a dispatch is
// routed on. Match is pure — no bus, no store, no registry — because matching
// is the decision most worth testing exhaustively and I/O in it would make
// that expensive enough to skip.
func TestMatchFiltersByCapabilityOSAndArch(t *testing.T) {
	req := executor.Requirements{
		OS:           "linux",
		Arch:         "arm64",
		Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED},
	}
	privileged := []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED}

	wanted := registry.Instance{
		ID: "all-three", State: registry.StateReady,
		OS: "linux", Arch: "arm64", Slots: 1, Capabilities: privileged,
	}
	fleet := []registry.Instance{
		wanted,
		{ID: "wrong-arch", State: registry.StateReady,
			OS: "linux", Arch: "amd64", Slots: 1, Capabilities: privileged},
		{ID: "wrong-os", State: registry.StateReady,
			OS: "darwin", Arch: "arm64", Slots: 1, Capabilities: privileged},
		{ID: "no-capability", State: registry.StateReady,
			OS: "linux", Arch: "arm64", Slots: 1},
		{ID: "draining", State: registry.StateDraining,
			OS: "linux", Arch: "arm64", Slots: 1, Capabilities: privileged},
	}

	require.Equal(t, []registry.Instance{wanted}, scheduler.Match(req, fleet),
		"only an instance advertising the capability on the right platform may take the step")
}

// TestUnschedulableStepReportsWhy: a step nothing can run must say so in the
// run's log. A step that silently never runs is the worst failure a scheduler
// has — there is nothing to look at, so the operator concludes the system is
// slow rather than that it is stuck.
func TestUnschedulableStepReportsWhy(t *testing.T) {
	ctx := testContext(t)
	// A fleet that is ready and on the right platform, but grants nothing.
	h := newHarnessWith(ctx, t, privilegedDiamond(), readyEngine("e1"))
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Empty(t, h.drain(ctx, t), "nothing may be dispatched to an engine that cannot run it")

	events, err := h.store.Replay(ctx, testTenant, testRun)
	require.NoError(t, err)

	var reasons []string
	for _, e := range events {
		if e.Type == scheduler.StepUnschedulable {
			reasons = append(reasons, string(e.Payload))
		}
	}
	require.Len(t, reasons, 1, "the one ready step is the one that could not be placed")
	require.Contains(t, reasons[0], "no engine advertises capability PRIVILEGED")
}

// TestAdvanceIsIdempotent: Advance is called after every status and again
// after every restart, so it must be safe to call at any moment. A second
// call against unchanged state has to be a no-op — the alternative is a step
// dispatched twice for every duplicate delivery the bus makes.
func TestAdvanceIsIdempotent(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(ctx, t, readyEngine("e1"))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))

	require.Equal(t, []string{"a"}, h.drain(ctx, t),
		"three Advances over one unchanged run state are one dispatch")
	require.Equal(t, 1, h.countEvents(ctx, t, runstore.StepDispatched, "a"))
}

// TestConcurrentAdvanceDispatchesStepOnce is the multi-plane case: two control
// planes advancing the same run at the same moment. The lease is what settles
// it — claiming supersedes, so the plane whose fence was overtaken finds its
// token refused and writes nothing. Without that check both planes would
// commit a dispatch and the step would run twice.
//
// The lease here is the real KV manager over an embedded NATS, and the store
// is the real SQLite one. A fake of either would answer instantly and in one
// goroutine, which is precisely the interleaving this test is looking for.
func TestConcurrentAdvanceDispatchesStepOnce(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(ctx, t, readyEngine("e1"))
	// Four planes rather than two: one pair can miss each other by luck, and a
	// concurrency test that only sometimes overlaps is one that only sometimes
	// tests anything.
	planes := []*scheduler.Scheduler{h.sched}
	for range 3 {
		planes = append(planes, h.secondPlane(ctx, t))
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, s := range planes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// Losing the race is not an error: the other plane has the step.
			_ = s.Advance(ctx, testTenant, testRun)
		}()
	}
	close(start)
	wg.Wait()

	require.Equal(t, 1, h.countEvents(ctx, t, runstore.StepDispatched, "a"),
		"four planes, one attempt, one dispatch event")
	require.Equal(t, []string{"a"}, h.drain(ctx, t),
		"and exactly one dispatch reached the bus")
}

// TestOnStatusIgnoresStaleFence: an engine presumed dead comes back holding a
// step that has since been re-dispatched. Its report carries the old fence and
// must be discarded — applying it would overwrite a newer attempt's result
// with the answer of a run nobody is waiting for any more.
func TestOnStatusIgnoresStaleFence(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(ctx, t, readyEngine("e1"))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))
	stale := h.bus.dispatches(t)[0]

	// Something else claims the step: the fence moves and the dispatch above
	// is superseded.
	_, err := h.leases.Claim(ctx, testTenant, testRun, "a", 2, time.Minute)
	require.NoError(t, err)

	require.NoError(t, h.sched.OnStatus(ctx, &dholev1.JobStatus{
		RunId:      stale.GetRunId(),
		StepId:     stale.GetStepId(),
		Attempt:    stale.GetAttempt(),
		FenceToken: stale.GetFenceToken(),
		Phase:      dholev1.Phase_PHASE_SUCCEEDED,
	}), "a stale report is ignored, not an error: the engine did nothing wrong")

	require.Equal(t, 0, h.countEvents(ctx, t, runstore.StepSucceeded, "a"),
		"the superseded attempt's result must not be recorded")
	require.Equal(t, []string{"a"}, h.drain(ctx, t),
		"and nothing downstream of it may become ready")
}

// TestAdvanceAndOnStatusRefuseAnUnscopedTenant. Every stored record and every
// bus subject in this system carries a tenant. An empty one is a bug in the
// caller, never a wildcard, and a scheduler that treated it as one would read
// and write another tenant's run.
func TestAdvanceAndOnStatusRefuseAnUnscopedTenant(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(ctx, t, readyEngine("e1"))

	err := h.sched.Advance(ctx, "", testRun)
	require.Error(t, err)
	require.ErrorIs(t, err, runstore.ErrTenantRequired)
	require.Contains(t, err.Error(), "tenant scope required")

	err = h.sched.OnStatus(ctx, &dholev1.JobStatus{
		RunId: testRun, StepId: "a", Attempt: 1,
		FenceToken: scheduler.EncodeFence("", lease.Token{Value: "claim.x", Fence: 1}),
		Phase:      dholev1.Phase_PHASE_SUCCEEDED,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, runstore.ErrTenantRequired)
}

// TestDispatchRecordsWhyAStepIsNotCacheable is Task 16's visibility half. A
// step that quietly stopped being cached looks like a permanent performance
// mystery; the reason belongs in the run's log, next to the dispatch it
// applied to.
func TestDispatchRecordsWhyAStepIsNotCacheable(t *testing.T) {
	ctx := testContext(t)
	pooled := diamond()
	pooled.GetSteps()[0].LeaseScope = dholev1.LeaseScope_LEASE_SCOPE_POOL
	h := newHarnessWith(ctx, t, pooled, readyEngine("e1"))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))

	payload := h.dispatchPayload(ctx, t, "a")
	require.False(t, payload.Cacheable)
	require.Equal(t, "pool lease reuses state that cannot be hashed", payload.CacheIneligibleReason)

	h.succeed(ctx, t, "a")
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	step := h.dispatchPayload(ctx, t, "b")
	require.True(t, step.Cacheable, "a step-scoped pure step is still cacheable")
	require.Empty(t, step.CacheIneligibleReason)
}

// refusingStore is a run store that fails the test if it is read at all. It
// exists to prove WHERE an unscoped call is refused: passing an empty tenant
// down and letting the store catch it works today only because that store is
// careful, and the guarantee must not depend on the diligence of whatever is
// underneath.
type refusingStore struct {
	runstore.Store
	t *testing.T
}

func (s refusingStore) Replay(context.Context, string, string) ([]runstore.Event, error) {
	s.t.Fatal("the scheduler read the store for an unscoped tenant")
	return nil, nil
}

func (s refusingStore) LastSequence(context.Context, string) (uint64, error) {
	s.t.Fatal("the scheduler read the store for an unscoped tenant")
	return 0, nil
}

// TestAdvanceRefusesAnUnscopedTenantBeforeReadingAnything: the refusal is the
// scheduler's own, made before a single query goes out.
func TestAdvanceRefusesAnUnscopedTenantBeforeReadingAnything(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(ctx, t, readyEngine("e1"))

	sched, err := scheduler.New(scheduler.Config{
		Store:       refusingStore{Store: h.store, t: t},
		Outbox:      h.outbox,
		Leases:      h.leases,
		Fleet:       h.fleet,
		Definitions: h.defs,
		Tier:        testTier,
	})
	require.NoError(t, err)

	require.ErrorIs(t, sched.Advance(ctx, "", testRun), runstore.ErrTenantRequired)
}

// gatingLeases holds a claim open until the test lets it through, which is how
// the one interleaving a scheduler cannot be talked out of is reproduced
// exactly rather than hoped for: a plane decided a step was ready, and by the
// time its claim came back another plane had already dispatched it.
type gatingLeases struct {
	lease.Manager
	entered chan struct{}
	release chan struct{}
}

func (g *gatingLeases) Claim(
	ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration,
) (lease.Token, error) {
	select {
	case <-g.entered:
	default:
		close(g.entered)
	}
	<-g.release
	return g.Manager.Claim(ctx, tenantID, runID, stepID, attempt, ttl)
}

// TestAdvanceSkipsAStepAnotherPlaneDispatchedWhileItWasClaiming. Claiming a
// lease is a round trip, and the world moves during it. A plane that comes
// back from a claim to find the step already dispatched must write nothing:
// its own snapshot of the log was taken before the other plane committed, so
// trusting it would dispatch the same attempt twice.
func TestAdvanceSkipsAStepAnotherPlaneDispatchedWhileItWasClaiming(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(ctx, t, readyEngine("e1"))

	gate := &gatingLeases{
		Manager: h.leases,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	slow := h.planeWithLeases(ctx, t, gate)

	done := make(chan error, 1)
	go func() { done <- slow.Advance(ctx, testTenant, testRun) }()

	// The slow plane has read the log — which says nothing is dispatched — and
	// is now inside its claim.
	<-gate.entered

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))

	close(gate.release)
	require.NoError(t, <-done, "losing a step to another plane is not an error")

	require.Equal(t, 1, h.countEvents(ctx, t, runstore.StepDispatched, "a"),
		"the plane that was overtaken must not record a second dispatch")
	require.Equal(t, []string{"a"}, h.drain(ctx, t),
		"and must not put a second dispatch on the bus")
}

// TestRunCompletesAndStopsAdvancing walks the whole diamond. Two things are
// being pinned: a run with nothing ready and nothing in flight is completed
// exactly once, and a step's inputs are the outputs of the steps whose ports
// feed it — the edges are the only place that comes from.
func TestRunCompletesAndStopsAdvancing(t *testing.T) {
	ctx := testContext(t)
	h := newHarness(ctx, t, readyEngine("e1"))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	h.drain(ctx, t)
	h.succeed(ctx, t, "a")
	h.drain(ctx, t)
	h.succeed(ctx, t, "b")
	h.succeed(ctx, t, "c")
	require.Equal(t, []string{"a", "b", "c", "d"}, h.drain(ctx, t))

	var last *dholev1.JobDispatch
	for _, d := range h.bus.dispatches(t) {
		if d.GetStepId() == "d" {
			last = d
		}
	}
	require.Len(t, last.GetInputs(), 2, "d consumes one output from each of b and c")

	h.succeed(ctx, t, "d")
	h.drain(ctx, t)
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))

	require.Equal(t, 1, h.countEvents(ctx, t, runstore.RunCompleted, ""),
		"a finished run is completed once and stays completed")
}
