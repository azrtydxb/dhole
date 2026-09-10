package effects_test

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
	"github.com/azrtydxb/dhole/internal/effects"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// TestPolicyPerEffectClass pins the table ADR 0002 exists for. The effect
// class is the only thing that can decide whether repeating a step is safe,
// so it is the only input to the retry policy.
func TestPolicyPerEffectClass(t *testing.T) {
	pure := effects.RetryPolicy(&dholev1.Step{
		Id: "build", EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
	})
	require.Equal(t, 3, pure.MaxAttempts, "a pure step may be repeated freely")
	require.False(t, pure.RequiresExclusiveLease,
		"nothing outside the step observes it running, so nothing has to be excluded")

	idempotent := effects.RetryPolicy(&dholev1.Step{
		Id: "notify", EffectClass: dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
	})
	require.True(t, idempotent.RequiresIdempotencyKey,
		"the far end can only recognise the repeat if the key is the same one")
	require.Greater(t, idempotent.MaxAttempts, 1, "an idempotent step is retried automatically")

	atMostOnce := effects.RetryPolicy(&dholev1.Step{
		Id: "charge", EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
	})
	require.Equal(t, 1, atMostOnce.MaxAttempts,
		"one attempt, ever: the alternative is charging the card twice")
	require.True(t, atMostOnce.RequiresExclusiveLease,
		"execution must be excluded, not merely ordered")
}

// TestIdempotentRetryReusesIdempotencyKey is the whole point of the key: the
// far end recognises the SECOND delivery as the same request as the first and
// declines to act twice. A key that varied with the attempt would look like a
// new request every time and would buy nothing at all.
//
// IdempotencyKey therefore takes `attempt` and deliberately ignores it. That
// parameter is a trap for a later reader who sees an unused argument and folds
// it into the hash to be tidy; this test is what stops that change reaching
// production as a double charge.
func TestIdempotentRetryReusesIdempotencyKey(t *testing.T) {
	first := effects.IdempotencyKey("run-1", "charge", 1)
	second := effects.IdempotencyKey("run-1", "charge", 2)
	require.Equal(t, first, second,
		"attempt 2 of a step is the SAME request as attempt 1 — if the key moves, the far end charges twice")
	require.NotEmpty(t, first)

	require.NotEqual(t, first, effects.IdempotencyKey("run-1", "refund", 1),
		"two steps of one run are two different requests")
	require.NotEqual(t, first, effects.IdempotencyKey("run-2", "charge", 1),
		"the same step in another run is another request")

	// Ambiguous concatenation would make these two collide.
	require.NotEqual(t,
		effects.IdempotencyKey("run", "1charge", 1),
		effects.IdempotencyKey("run1", "charge", 1),
		"the key must not be a bare concatenation of its parts")
}

// TestUnspecifiedEffectClassGetsTheMostConservativePolicy. A step whose class
// was never set has promised nothing, and an undeclared promise is not a
// promise: guessing "pure" here would auto-retry an unlabelled card charge.
func TestUnspecifiedEffectClassGetsTheMostConservativePolicy(t *testing.T) {
	for _, step := range []*dholev1.Step{
		{Id: "undeclared"},
		{Id: "explicit", EffectClass: dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED},
		{Id: "from-the-future", EffectClass: dholev1.EffectClass(99)},
		nil,
	} {
		p := effects.RetryPolicy(step)
		require.Equal(t, 1, p.MaxAttempts,
			"an undeclared class must not be retried automatically")
		require.True(t, p.RequiresExclusiveLease,
			"an undeclared class must be treated as if it had an external effect")
		require.Equal(t, effects.RetryPolicy(&dholev1.Step{
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		}), p, "the default is the at-most-once policy, not the pure one")
	}
}

// TestBackoffGrowsWithTheAttempt: a step that failed because the far end was
// overloaded must not be retried at the same rate that overloaded it.
func TestBackoffGrowsWithTheAttempt(t *testing.T) {
	p := effects.RetryPolicy(&dholev1.Step{EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE})
	require.Positive(t, p.Backoff, "a pure step's retry is delayed, not immediate")

	first := effects.Backoff(p, 1)
	second := effects.Backoff(p, 2)
	third := effects.Backoff(p, 3)
	require.Equal(t, p.Backoff, first)
	require.Greater(t, second, first, "the second wait is longer than the first")
	require.Greater(t, third, second, "and it keeps growing")
	require.LessOrEqual(t, effects.Backoff(p, 40), effects.MaxBackoff,
		"growth is capped, and must not overflow into a negative duration")
	require.Positive(t, effects.Backoff(p, 40))
	require.Equal(t, time.Duration(0), effects.Backoff(effects.Policy{}, 1),
		"a policy with no backoff waits for nothing")
}

// TestAtMostOnceStepIsNeverAutoRetried drives the real scheduler — real
// SQLite log, real lease manager over an embedded NATS, real outbox — through
// a step failure and requires that nothing is sent a second time.
//
// This is the property the whole effect class exists for. An automatic retry
// here is a second charge on somebody's card, and the only acceptable
// behaviour is to stop and record that a human has to decide.
func TestAtMostOnceStepIsNeverAutoRetried(t *testing.T) {
	for _, tc := range []struct {
		name  string
		class dholev1.EffectClass
	}{
		{"declared at-most-once", dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE},
		// A step whose class was never set gets the same treatment: an
		// undeclared step must not be freely retried either.
		{"undeclared", dholev1.EffectClass_EFFECT_CLASS_UNSPECIFIED},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testContext(t)
			h := newHarness(ctx, t, onePipeline(tc.class), time.Now)

			require.NoError(t, h.sched.Advance(ctx, tenantID, runID))
			require.Equal(t, []string{"charge"}, h.drain(ctx, t))

			h.fail(ctx, t, "charge")
			require.NoError(t, h.sched.Advance(ctx, tenantID, runID))
			require.NoError(t, h.sched.Advance(ctx, tenantID, runID))

			require.Equal(t, []string{"charge"}, h.drain(ctx, t),
				"the failed attempt must not be dispatched again")
			require.Equal(t, 1, h.count(ctx, t, runstore.StepDispatched, "charge"),
				"and no second dispatch may be recorded either")
			require.Equal(t, 1, h.count(ctx, t, scheduler.StepAwaitingReplay, "charge"),
				"the run's log has to say a human is being waited on, once")
			require.Equal(t, 0, h.count(ctx, t, runstore.RunCompleted, ""),
				"a run waiting for a replay decision is not finished")
		})
	}
}

// --- harness ------------------------------------------------------------
//
// The scheduler is driven for real here: a fake lease manager or an in-memory
// log would decide the outcome of exactly the property under test.

const (
	tenantID   = "acme"
	runID      = "run-1"
	pipelineID = "payments"
	revisionID = "rev-1"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// onePipeline is a single step of the given effect class: enough to fail one
// step and watch what the scheduler does next.
func onePipeline(class dholev1.EffectClass) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     pipelineID,
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:          "charge",
			Name:        "charge",
			PluginRef:   "cmd://charge",
			EffectClass: class,
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
		}},
	}
}

type publishingBus struct {
	mu        sync.Mutex
	published [][]byte
}

func (b *publishingBus) Publish(_ context.Context, _ string, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.published = append(b.published, payload)
	return nil
}

func (b *publishingBus) Request(context.Context, string, proto.Message, proto.Message) error {
	panic("not used")
}

func (b *publishingBus) SubscribePull(context.Context, string, string, string) (bus.Subscription, error) {
	panic("not used")
}

func (b *publishingBus) SubscribeEphemeral(context.Context, string, func([]byte)) (func(), error) {
	panic("not used")
}

type harness struct {
	store  runstore.Store
	bus    *publishingBus
	outbox *outbox.Outbox
	sched  *scheduler.Scheduler
}

func newHarness(
	ctx context.Context, t *testing.T, pipeline *dholev1.Pipeline, now func() time.Time,
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

	recorder := &publishingBus{}
	ob := outbox.New(store, recorder, "test-plane")

	sched, err := scheduler.New(scheduler.Config{
		Store:  store,
		Outbox: ob,
		Leases: leases,
		Fleet: staticFleet{{
			ID: "e1", State: registry.StateReady,
			OS: "linux", Arch: "amd64", Slots: 4, ProtocolVersions: []uint32{1},
		}},
		Definitions: staticDefs{pipeline},
		Tier:        "trusted",
		OS:          "linux",
		Arch:        "amd64",
		Now:         now,
	})
	require.NoError(t, err)

	payload, err := scheduler.MarshalRunCreated(scheduler.RunCreated{
		PipelineID: pipelineID, RevisionID: revisionID,
	})
	require.NoError(t, err)
	require.NoError(t, store.Append(ctx, tenantID, runstore.Event{
		RunID: runID, Sequence: 1, Type: runstore.RunCreated,
		Payload: payload, At: now().UTC(),
	}))

	return &harness{store: store, bus: recorder, outbox: ob, sched: sched}
}

type staticFleet []registry.Instance

func (f staticFleet) Instances(context.Context, string) ([]registry.Instance, error) {
	return f, nil
}

type staticDefs struct{ pipeline *dholev1.Pipeline }

func (d staticDefs) Get(_ context.Context, tenant, pipeline, revision string) (*dholev1.Pipeline, error) {
	if tenant != tenantID || pipeline != pipelineID || revision != revisionID {
		return nil, runstore.ErrTenantRequired
	}
	return d.pipeline, nil
}

// drain publishes what the scheduler owed and reports the step ids that
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
	for _, d := range h.dispatches(t) {
		ids = append(ids, d.GetStepId())
	}
	return ids
}

func (h *harness) dispatches(t *testing.T) []*dholev1.JobDispatch {
	t.Helper()
	h.bus.mu.Lock()
	defer h.bus.mu.Unlock()
	out := make([]*dholev1.JobDispatch, 0, len(h.bus.published))
	for _, payload := range h.bus.published {
		d := &dholev1.JobDispatch{}
		require.NoError(t, proto.Unmarshal(payload, d))
		out = append(out, d)
	}
	return out
}

// fail reports the LATEST attempt of a step as failed, with the fence it was
// dispatched under, exactly as its engine would.
func (h *harness) fail(ctx context.Context, t *testing.T, stepID string) {
	t.Helper()
	var latest *dholev1.JobDispatch
	for _, d := range h.dispatches(t) {
		if d.GetStepId() == stepID {
			latest = d
		}
	}
	require.NotNil(t, latest, "step %q was never dispatched", stepID)
	require.NoError(t, h.sched.OnStatus(ctx, &dholev1.JobStatus{
		RunId:      latest.GetRunId(),
		StepId:     latest.GetStepId(),
		Attempt:    latest.GetAttempt(),
		FenceToken: latest.GetFenceToken(),
		Phase:      dholev1.Phase_PHASE_FAILED,
		ExitCode:   1,
		Error:      "the far end said no",
	}))
}

func (h *harness) count(ctx context.Context, t *testing.T, kind runstore.EventType, stepID string) int {
	t.Helper()
	events, err := h.store.Replay(ctx, tenantID, runID)
	require.NoError(t, err)
	n := 0
	for _, e := range events {
		if e.Type == kind && e.StepID == stepID {
			n++
		}
	}
	return n
}
