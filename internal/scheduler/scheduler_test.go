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
	engineagent "github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/wire"
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

// subjects is every subject published to so far, in order. The subject is the
// routing decision, so a test that only reads the payloads cannot tell where a
// dispatch went — which is how a step naming an engine kind came to run on an
// engine of another one.
func (b *recordingBus) subjects() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.published))
	for _, p := range b.published {
		out = append(out, p.subject)
	}
	return out
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
		// The tier and the environment it names are not decoration: the
		// scheduler hashes cache keys against the identity the engines of the
		// tier it dispatches to announced, so an instance that named neither
		// would make every step in every test uncacheable (ADR 0021).
		Tier:                testTier,
		EnvironmentIdentity: testEnvIdentity,
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
	return newHarnessWithLeaseTTL(ctx, t, pipeline, scheduler.DefaultLeaseTTL, instances...)
}

// newHarnessWithLeaseTTL is newHarnessWith with the lease deadline in the
// test's hands. A lease that expires is the ONLY way an orphan exists, and the
// lease manager here is the real KV one over a real NATS server — its clock is
// the server's, not a variable a test can advance — so a case about expiry has
// to shorten the TTL and wait.
func newHarnessWithLeaseTTL(
	ctx context.Context, t *testing.T, pipeline *dholev1.Pipeline,
	ttl time.Duration, instances ...registry.Instance,
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
	ob := outbox.New(store, recorder, "test-plane")
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
		LeaseTTL:    ttl,
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

// TestAnOrphanedAttemptIsRecordedAndReDispatched is the gap between a dead
// engine and a run that carries on.
//
// lease.Expire has always been able to tell you a step's holder died, and
// nothing could act on it: plan counts any step with attempts > 0 as in flight,
// forever, and no event said an attempt had died. So the step sat in a run that
// was neither finished nor progressing, and the only evidence was a lease that
// had quietly disappeared from a KV bucket.
//
// The engine here is not stopped or drained — it simply stops renewing, which
// is what a crashed engine, a severed network and a wedged host all look like.
func TestAnOrphanedAttemptIsRecordedAndReDispatched(t *testing.T) {
	ctx := testContext(t)
	const ttl = 250 * time.Millisecond
	h := newHarnessWithLeaseTTL(ctx, t, diamond(), ttl, readyEngine("e1"))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))
	first := h.bus.dispatches(t)[0]

	// Nobody renews. The holder is gone.
	time.Sleep(2 * ttl)

	lost, err := h.sched.SweepOrphans(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, lost, "the sweeper must find the step whose lease died")
	require.Equal(t, 1, h.countEvents(ctx, t, scheduler.StepAttemptLost, "a"),
		"an attempt that died has to be IN the log; a run's log is the only place anyone looks")

	// And the point of recording it: the step runs again, under a new fence,
	// so a resurrected engine's late report is recognised as stale.
	require.Equal(t, []string{"a", "a"}, h.drain(ctx, t))
	second := h.bus.dispatches(t)[1]
	require.Equal(t, uint32(2), second.GetAttempt())
	require.NotEqual(t, first.GetFenceToken(), second.GetFenceToken(),
		"a re-dispatch under the SAME fence would let the dead engine's report overwrite the new one")

	// Sweeping again finds nothing: the orphan was claimed by this sweep, and
	// a second plane sweeping concurrently must not re-report it.
	lost, err = h.sched.SweepOrphans(ctx)
	require.NoError(t, err)
	require.Zero(t, lost)
}

// TestAnOrphanedAtMostOnceStepIsNotSilentlyRepeated is the other half, and the
// one that would be a real incident. An engine that stopped answering may have
// run the step to completion and died before reporting it, so re-dispatching a
// step whose effect class forbids an automatic retry would charge the card
// twice. It waits for a person instead (ADR 0002).
func TestAnOrphanedAtMostOnceStepIsNotSilentlyRepeated(t *testing.T) {
	ctx := testContext(t)
	const ttl = 250 * time.Millisecond

	pipeline := diamond()
	pipeline.GetSteps()[0].EffectClass = dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE
	h := newHarnessWithLeaseTTL(ctx, t, pipeline, ttl, readyEngine("e1"))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))

	time.Sleep(2 * ttl)
	lost, err := h.sched.SweepOrphans(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, lost)

	require.Equal(t, []string{"a"}, h.drain(ctx, t),
		"an at-most-once step whose engine died must not be dispatched again on its own")
	require.Equal(t, 1, h.countEvents(ctx, t, scheduler.StepAwaitingReplay, "a"),
		"and the run must say so, because nothing will move until a person acts")
}

// TestADispatchIsWrittenAtTheHighestVersionEveryMatchedEngineSpeaks is what
// makes bumping the wire version a rolling upgrade rather than a flag day.
//
// A dispatch goes to a TIER's subject and the queue decides which member takes
// it, so the plane cannot address one at a version negotiated per engine — it
// has to write one every candidate can read. It used to stamp its own maximum
// on every dispatch, which was invisible while there had only ever been one
// version: the moment the plane moved to version 2, every engine still on
// version 1 answered "unsupported protocol" to every step it was handed,
// having been admitted to the fleet precisely because the plane accepts
// engines one version behind.
func TestADispatchIsWrittenAtTheHighestVersionEveryMatchedEngineSpeaks(t *testing.T) {
	ctx := testContext(t)

	current := readyEngine("e-current")
	current.ProtocolVersions = []uint32{wire.ProtocolVersion}
	behind := readyEngine("e-behind")
	behind.ProtocolVersions = []uint32{wire.ProtocolVersion - 1}

	t.Run("a fleet on this version is dispatched at this version", func(t *testing.T) {
		h := newHarness(ctx, t, current)
		require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
		require.Equal(t, []string{"a"}, h.drain(ctx, t))
		require.Equal(t, wire.ProtocolVersion, h.bus.dispatches(t)[0].GetProtocolVersion())
	})

	t.Run("one engine a version behind holds the whole tier back", func(t *testing.T) {
		h := newHarness(ctx, t, current, behind)
		require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
		require.Equal(t, []string{"a"}, h.drain(ctx, t))
		require.Equal(t, wire.ProtocolVersion-1, h.bus.dispatches(t)[0].GetProtocolVersion(),
			"the queue may hand this to the older engine, so it must be readable by it")
	})
}

// TestMatchFiltersByEngineType. Placement had no lever but capability: a
// pipeline that needed a pod asked for NETWORK and hoped the only engine
// advertising it was the Kubernetes one, and a step that had to run as a host
// process could not be expressed at all. A step now names the executor kind it
// needs, and an engine that never said which kinds it offers is not credited
// with any — an unstated engine type is unknown, not universal, the same rule
// the platform axes already follow.
func TestMatchFiltersByEngineType(t *testing.T) {
	req := executor.Requirements{OS: "linux", Arch: "arm64", EngineType: "process"}

	wanted := registry.Instance{
		ID: "process-engine", State: registry.StateReady,
		OS: "linux", Arch: "arm64", Slots: 1, EngineTypes: []string{"process"},
	}
	fleet := []registry.Instance{
		wanted,
		{ID: "kubernetes-engine", State: registry.StateReady,
			OS: "linux", Arch: "arm64", Slots: 1, EngineTypes: []string{"kubernetes"}},
		{ID: "says-nothing", State: registry.StateReady,
			OS: "linux", Arch: "arm64", Slots: 1},
	}

	require.Equal(t, []registry.Instance{wanted}, scheduler.Match(req, fleet),
		"only an engine that offers the executor kind the step named may take it")
	require.Len(t, scheduler.Match(executor.Requirements{OS: "linux", Arch: "arm64"}, fleet), 3,
		"a step that names no engine type still goes anywhere it fits")
}

// TestExplainNamesTheEngineTypeNothingOffers. A step that cannot be placed must
// never be held silently: the run then looks slow rather than stuck, and there
// is nothing to look at. Explain already names the platform and the capability
// that nobody has; the engine type has to be named the same way, or the newest
// reason a step cannot run is the one reason the operator is not told.
func TestExplainNamesTheEngineTypeNothingOffers(t *testing.T) {
	fleet := []registry.Instance{
		{ID: "kubernetes-engine", State: registry.StateReady,
			OS: "linux", Arch: "arm64", Slots: 1, EngineTypes: []string{"kubernetes"}},
	}
	why := scheduler.Explain(
		executor.Requirements{OS: "linux", Arch: "arm64", EngineType: "process"}, fleet)
	require.Equal(t, "no ready engine offers engine type process", why)
}

// TestAStepNamingAnEngineTypeNothingOffersIsUnschedulableNotHeld closes the
// wiring between the definition and the placement: the type has to be read off
// the step by the DISPATCHER, not only understood by Match, or a pipeline can
// name an engine type and be dispatched to something else entirely. And when
// nothing offers it the step must say so — a step that is silently never sent
// anywhere makes the run look slow instead of stuck.
func TestAStepNamingAnEngineTypeNothingOffersIsUnschedulableNotHeld(t *testing.T) {
	ctx := testContext(t)
	p := diamond()
	p.GetSteps()[0].EngineType = "kubernetes"
	// readyEngine advertises no engine types at all, which is the fleet an
	// engine written before Step.engine_type existed produces.
	h := newHarnessWith(ctx, t, p, readyEngine("e1"))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Empty(t, h.drain(ctx, t), "a step must not be dispatched to an engine of the wrong kind")

	events, err := h.store.Replay(ctx, testTenant, testRun)
	require.NoError(t, err)
	var reasons []string
	for _, e := range events {
		if e.Type == scheduler.StepUnschedulable {
			reasons = append(reasons, string(e.Payload))
		}
	}
	require.Len(t, reasons, 1)
	require.Contains(t, reasons[0], "no ready engine offers engine type kubernetes")
}

// TestAStepIsDispatchedToTheEngineKindItNamed is the same wiring from the
// other side: the step is placed the moment an engine offering that kind is in
// the fleet, so the filter refuses the wrong engine rather than every engine.
func TestAStepIsDispatchedToTheEngineKindItNamed(t *testing.T) {
	ctx := testContext(t)
	p := diamond()
	p.GetSteps()[0].EngineType = "kubernetes"
	engine := readyEngine("e1")
	engine.EngineTypes = []string{"kubernetes"}
	h := newHarnessWith(ctx, t, p, engine)

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))
}

// TestAFileTheDefinitionCarriesReachesTheEngineAsADeclaredInput is how a step
// gets a file under ADR 0023: not from a repository — this system has none to
// read — but from the definition, as an input like any other.
//
// The assertion is on the DISPATCH, because that is the whole contract with an
// engine: an engine never calls back to ask what to run, so a file that did
// not travel in the dispatch does not exist as far as the step is concerned.
// It must arrive as an InputRef bearing the file's digest, which is what the
// engine materialises at inputs/<port> exactly as it does an edge's bytes.
func TestAFileTheDefinitionCarriesReachesTheEngineAsADeclaredInput(t *testing.T) {
	ctx := testContext(t)

	fileDigest := &dholev1.Digest{Algo: "sha256", Hex: "d0cke7"}
	pipeline := &dholev1.Pipeline{
		Id:     testPipeline,
		Tenant: &dholev1.Tenant{Id: testTenant},
		Steps: []*dholev1.Step{{
			Id:          "build",
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			Inputs:      []*dholev1.Port{{Name: "context"}},
			Outputs:     []*dholev1.Port{{Name: "image"}},
			FileInputs:  []*dholev1.FileInput{{Port: "context", Path: "Dockerfile"}},
		}},
		Files: []*dholev1.File{{Path: "Dockerfile", Digest: fileDigest, SizeBytes: 18}},
	}
	h := newHarnessWith(ctx, t, pipeline, readyEngine("e1"))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"build"}, h.drain(ctx, t),
		"a step whose only input is a carried file has no predecessor to wait for")

	dispatches := h.bus.dispatches(t)
	require.Len(t, dispatches, 1)
	inputs := dispatches[0].GetInputs()
	require.Len(t, inputs, 1, "the file the definition carries did not travel in the dispatch")
	require.Equal(t, "context", inputs[0].GetPort())
	require.Equal(t, fileDigest.GetHex(), inputs[0].GetDigest().GetHex(),
		"the engine was pointed at bytes that are not the ones the revision pins")
}

// TestAStepNamingAnEngineKindIsPublishedOnThatKindsSubject is the half of the
// placement the filter never had. Match decides whether a step CAN be placed;
// the SUBJECT decides where it GOES. Proven on kw: a step naming engine_type
// vm, in a tier holding both a vm-backed and a kubernetes-backed engine, ran
// on the kubernetes one — both engines pull the same
// job.dispatch.<tier>.<caps> work queue and whichever grabbed it first ran it.
// The step wrote /proc/sys/kernel/osrelease and it read the node's kernel.
//
// So the kind is a token of the subject, exactly as the tier and the
// capability set already are, and only engines of that kind subscribe to it.
func TestAStepNamingAnEngineKindIsPublishedOnThatKindsSubject(t *testing.T) {
	ctx := testContext(t)
	p := diamond()
	p.GetSteps()[0].EngineType = "kubernetes"
	instance := readyEngine("e1")
	instance.EngineTypes = []string{"kubernetes"}
	h := newHarnessWith(ctx, t, p, instance)

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))

	caps := engineagent.CapsHash(p.GetSteps()[0].GetCapabilities())
	require.Equal(t,
		[]string{bus.SubjectDispatch(testTier, caps) + ".kubernetes"},
		h.bus.subjects(),
		"a step naming a kind must be published where only that kind is listening")
}

// TestAStepNamingNoEngineKindKeepsTheUnrestrictedDispatchSubject is the common
// case, and it must stay byte-identical to what it was before kind routing
// existed: that subject is what every engine already subscribes to, including
// one built before the kind token was invented.
func TestAStepNamingNoEngineKindKeepsTheUnrestrictedDispatchSubject(t *testing.T) {
	ctx := testContext(t)
	p := diamond()
	h := newHarnessWith(ctx, t, p, readyEngine("e1"))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))

	caps := engineagent.CapsHash(p.GetSteps()[0].GetCapabilities())
	require.Equal(t, []string{bus.SubjectDispatch(testTier, caps)}, h.bus.subjects(),
		"a step naming no kind goes on the subject every engine of every kind pulls")
}

// TestAStepParkedAtAGateIsNotSweptAndIsDispatchedAgainWhenReleased is the
// scheduler's half of resuming a parked agent step.
//
// Two rules, and each of them was a way for the run to end wrongly. A step
// stopped at a gate is waiting for a PERSON: whatever held its lease finished
// its part and went away, so sweeping it would give a person a lease TTL to
// decide in and fail the run on them while they slept. And a gate lifted by
// STEP_RESUMED says the step may CARRY ON rather than that it is finished —
// it has a dispatch behind it, so the attempt count alone says it is still in
// flight, and nothing would ever dispatch it again.
func TestAStepParkedAtAGateIsNotSweptAndIsDispatchedAgainWhenReleased(t *testing.T) {
	ctx := testContext(t)
	const ttl = 250 * time.Millisecond
	h := newHarnessWithLeaseTTL(ctx, t, diamond(), ttl, readyEngine("e1"))

	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))
	require.Equal(t, []string{"a"}, h.drain(ctx, t))

	// The step parks: it asks for an approval and stops renewing, exactly as
	// an agent step does when its model asks for an at-most-once action.
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID: testRun, StepID: "a", Type: scheduler.StepAwaitingApproval,
		Payload: []byte(`{"prompt":"approve?","parks_step":true}`), At: time.Now().UTC(),
	}))
	time.Sleep(2 * ttl)

	lost, err := h.sched.SweepOrphans(ctx)
	require.NoError(t, err)
	require.Zero(t, lost, "a step waiting for a person was failed for not renewing a lease")
	require.Zero(t, h.countEvents(ctx, t, scheduler.StepAttemptLost, "a"))
	require.Equal(t, []string{"a"}, h.drain(ctx, t), "a gated step was dispatched anyway")

	// A person approves. The gate is lifted without a verdict, because the
	// step has not produced anything yet.
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID: testRun, StepID: "a", Type: scheduler.StepResumed,
		Payload: []byte(`{"approver":"release-boss","approved":true}`), At: time.Now().UTC(),
	}))
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))

	require.Equal(t, []string{"a", "a"}, h.drain(ctx, t),
		"the released step was never dispatched again, so the run waits forever")
	second := h.bus.dispatches(t)[1]
	require.Equal(t, uint32(2), second.GetAttempt(),
		"the resumed step must run under a fresh attempt and fence")
}
