package approval_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/steps/approval"
)

const (
	testRun      = "run-1"
	testPipeline = "gated"
	testRevision = "rev-1"
	testTier     = "trusted"
	approver     = "alice"
	// denier is a second real principal: a decision is refused for being a
	// second decision, not for being made by a stranger.
	denier = "bob"
)

// TestApprovalGateBlocksUntilDecided is the human half of ADR 0003: a run that
// waits for a person costs a row, and it does not move an inch until that
// person decides.
//
// Two failures hide here. One is a gate that blocks nothing — the step behind
// it dispatched while the request was still outstanding. The other is a
// decision recorded without who made it, which is an audit trail that cannot
// answer the only question ever asked of it.
func TestApprovalGateBlocksUntilDecided(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		h := newHarness(ctx, t, open)

		require.NoError(t, h.step.Request(ctx, testRun, "gate", "ship release 4.2?"))
		require.NoError(t, h.sched.Advance(ctx, h.tenant, testRun))
		require.Empty(t, h.drain(ctx, t),
			"nothing runs while the approval is outstanding — not the gate, not what is behind it")

		require.NoError(t, h.step.Decide(ctx, testRun, "gate", approver, true))

		require.Equal(t, []string{"after"}, h.drain(ctx, t),
			"the step behind the gate ran once the decision was recorded")

		decided := h.event(ctx, t, approval.StepApprovalDecided)
		payload, err := approval.UnmarshalDecision(decided.Payload)
		require.NoError(t, err)
		require.Equal(t, approver, payload.Approver,
			"an approval whose approver is not in the log is not an approval")
		require.True(t, payload.Approved)
		require.Equal(t, "gate", decided.StepID)
	})
}

// TestApprovalDenialFailsRunWithReason: a denial is a decision, not a pause.
// The run ends, and it ends naming the person who ended it — otherwise the
// only record of a refused deployment is a run that stopped for no stated
// reason.
func TestApprovalDenialFailsRunWithReason(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		h := newHarness(ctx, t, open)

		require.NoError(t, h.step.Request(ctx, testRun, "gate", "ship release 4.2?"))
		require.NoError(t, h.step.Decide(ctx, testRun, "gate", denier, false))

		failed := h.event(ctx, t, scheduler.RunFailed)
		require.Contains(t, string(failed.Payload), denier,
			"the terminal event has to name who denied it")

		denial, err := approval.UnmarshalDenial(failed.Payload)
		require.NoError(t, err)
		require.Equal(t, denier, denial.Approver)
		require.Equal(t, "gate", denial.StepID)
		require.NotEmpty(t, denial.Reason)

		// The payload also still reads as an ordinary run failure, so the
		// scheduler's own reader is not a second, drifting format.
		failure, err := scheduler.UnmarshalRunFailure(failed.Payload)
		require.NoError(t, err)
		require.Equal(t, []string{"gate"}, failure.Steps)

		require.NoError(t, h.sched.Advance(ctx, h.tenant, testRun))
		require.Empty(t, h.drain(ctx, t), "a denied run is over: nothing else is dispatched")
	})
}

// TestSecondDecisionIsRefused. Approvals get double-clicked, and the two
// options are an idempotent no-op and a refusal. This system refuses, and
// names the standing decision in the refusal.
//
// A no-op would answer "approved" to a person who clicked DENY on a run that
// someone else had already approved — the two decisions disagree, and the
// second decider would be told theirs took effect. A refusal is the only
// answer that stays true when the second click is not the same as the first.
func TestSecondDecisionIsRefused(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		h := newHarness(ctx, t, open)

		require.NoError(t, h.step.Request(ctx, testRun, "gate", "ship it?"))
		require.NoError(t, h.step.Decide(ctx, testRun, "gate", approver, true))

		err := h.step.Decide(ctx, testRun, "gate", approver, true)
		require.ErrorIs(t, err, approval.ErrAlreadyDecided)
		require.Contains(t, err.Error(), approver, "the refusal names the standing decision")

		err = h.step.Decide(ctx, testRun, "gate", denier, false)
		require.ErrorIs(t, err, approval.ErrAlreadyDecided)

		require.Equal(t, 1, h.countEvents(ctx, t, approval.StepApprovalDecided),
			"one gate, one decision, however many times it is clicked")
		require.Zero(t, h.countEvents(ctx, t, scheduler.RunFailed),
			"the late denial did not fail a run that had already been approved")
	})
}

// TestDecideRefusesAnUnknownOrEmptyApprover. An approval is an authorisation
// act by a named person; a decision by nobody is a run advancing itself.
func TestDecideRefusesAnUnknownOrEmptyApprover(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		h := newHarness(ctx, t, open)
		require.NoError(t, h.step.Request(ctx, testRun, "gate", "ship it?"))

		err := h.step.Decide(ctx, testRun, "gate", "", true)
		require.ErrorIs(t, err, approval.ErrApproverRequired)

		err = h.step.Decide(ctx, testRun, "gate", "   ", true)
		require.ErrorIs(t, err, approval.ErrApproverRequired)

		// A subject that is not a principal of this tenant is not an
		// approver, however plausible the string is.
		err = h.step.Decide(ctx, testRun, "gate", "mallory", true)
		require.ErrorIs(t, err, approval.ErrUnknownApprover)

		require.Zero(t, h.countEvents(ctx, t, approval.StepApprovalDecided))
		require.NoError(t, h.sched.Advance(ctx, h.tenant, testRun))
		require.Empty(t, h.drain(ctx, t), "the gate is still shut")
	})
}

// TestApprovalIsTenantScoped. Every stored record carries a tenant, and an
// approver is a principal OF a tenant: the same subject in another tenant is a
// different person, and their click must not open this gate.
func TestApprovalIsTenantScoped(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		h := newHarness(ctx, t, open)

		_, err := approval.New(approval.Config{
			Store: h.store, TenantID: "", Approvers: h.approvers, Resume: h.sched,
		})
		require.ErrorIs(t, err, runstore.ErrTenantRequired)
		require.Contains(t, err.Error(), "tenant scope required")

		// A second tenant, with its own principal of the same name.
		other := uniqueTenant(t)
		require.NoError(t, identity.NewLocal(h.identity).CreateUser(ctx, other, approver, "hunter2"))
		elsewhere, err := approval.New(approval.Config{
			Store: h.store, TenantID: other, Approvers: h.approvers, Resume: h.sched,
		})
		require.NoError(t, err)

		require.NoError(t, h.step.Request(ctx, testRun, "gate", "ship it?"))
		// The other tenant's approver decides on a run that is not theirs:
		// under their own scope there is no such gate.
		require.Error(t, elsewhere.Decide(ctx, testRun, "gate", approver, true))

		require.Zero(t, h.countEvents(ctx, t, approval.StepApprovalDecided))
		require.NoError(t, h.sched.Advance(ctx, h.tenant, testRun))
		require.Empty(t, h.drain(ctx, t), "another tenant's decision did not open this gate")
	})
}

// TestDecideBeforeRequestIsRefused. A decision on a gate nobody asked for is
// not an approval of anything, and recording it would let a caller mark any
// step of any run succeeded.
func TestDecideBeforeRequestIsRefused(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		h := newHarness(ctx, t, open)

		err := h.step.Decide(ctx, testRun, "gate", approver, true)
		require.ErrorIs(t, err, approval.ErrNotAwaiting)
		require.Zero(t, h.countEvents(ctx, t, approval.StepApprovalDecided))
	})
}

// ---------------------------------------------------------------------------
// Harness. The store, the identity store, the lease manager and the scheduler
// are real; only the publish side of the bus and the engine fleet are stood
// in for, because neither decides any property asserted above.

type storeOpener func(t *testing.T) (runstore.Store, *sql.DB, runstore.Dialect)

func eachStore(t *testing.T, fn func(t *testing.T, open storeOpener)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "run.db")
		fn(t, func(t *testing.T) (runstore.Store, *sql.DB, runstore.Dialect) {
			t.Helper()
			store, err := runstore.NewSQLite(path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })
			db, err := runstore.OpenSQLite(path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			return store, db, runstore.DialectSQLite
		})
	})
	t.Run("postgres", func(t *testing.T) {
		if scopedDSN == "" {
			t.Skip("DHOLE_TEST_POSTGRES_DSN not set")
		}
		dsn := scopedDSN
		fn(t, func(t *testing.T) (runstore.Store, *sql.DB, runstore.Dialect) {
			t.Helper()
			ctx := context.Background()
			store, err := runstore.NewPostgres(ctx, dsn)
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })
			db, err := runstore.OpenPostgres(ctx, dsn)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			return store, db, runstore.DialectPostgres
		})
	})
}

// Postgres is one shared database for the whole suite, and these cases write
// to tables other packages assert counts on — the outbox above all. Each test
// binary therefore gets its OWN SCHEMA: the migrations run inside it, every
// table this package touches is private to it, and nothing here can be
// mistaken for another package's backlog. The schema goes when the run does.
const testSchema = "dhole_approval_test"

func TestMain(m *testing.M) {
	code := func() int {
		dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
		if dsn == "" {
			return m.Run()
		}
		schema := fmt.Sprintf("%s_%d", testSchema, os.Getpid())
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			fmt.Fprintf(os.Stderr, "postgres: %v\n", err)
			return 1
		}
		defer func() { _ = db.Close() }()
		if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS " + schema); err != nil {
			fmt.Fprintf(os.Stderr, "postgres: create schema: %v\n", err)
			return 1
		}
		defer func() { _, _ = db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE") }()
		scopedDSN = dsn + "&search_path=" + schema
		return m.Run()
	}()
	os.Exit(code)
}

// scopedDSN is the shared database reached through this run's own schema.
var scopedDSN string

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

var tenantSeq atomic.Int64

func uniqueTenant(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("approval-%d-%d", time.Now().UnixNano(), tenantSeq.Add(1))
}

type harness struct {
	tenant    string
	store     runstore.Store
	identity  identity.Store
	approvers approval.Approvers
	bus       *recordingBus
	outbox    *outbox.Outbox
	sched     *scheduler.Scheduler
	step      *approval.Step
}

func newHarness(ctx context.Context, t *testing.T, open storeOpener) *harness {
	t.Helper()
	store, db, dialect := open(t)
	tenant := uniqueTenant(t)

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
	sched, err := scheduler.New(scheduler.Config{
		Store:       store,
		Outbox:      ob,
		Leases:      leases,
		Fleet:       staticFleet{instances: []registry.Instance{readyEngine("e1")}},
		Definitions: staticDefs{pipeline: gatedPipeline()},
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		EnvIdentity: "sha256:env",
	})
	require.NoError(t, err)

	// A real principal store: "is this person an approver of this tenant" is
	// the question, and a fake that answers yes would retire the test.
	principals := identity.NewSQLStoreWithDialect(db, dialect)
	local := identity.NewLocal(principals)
	require.NoError(t, local.CreateUser(ctx, tenant, approver, "hunter2"))
	require.NoError(t, local.CreateUser(ctx, tenant, denier, "hunter2"))

	step, err := approval.New(approval.Config{
		Store:     store,
		TenantID:  tenant,
		Approvers: principals,
		Resume:    sched,
	})
	require.NoError(t, err)

	h := &harness{
		tenant: tenant, store: store, identity: principals, approvers: principals,
		bus: recorder, outbox: ob, sched: sched, step: step,
	}
	h.seedRun(ctx, t)
	return h
}

func (h *harness) seedRun(ctx context.Context, t *testing.T) {
	t.Helper()
	payload, err := scheduler.MarshalRunCreated(scheduler.RunCreated{
		PipelineID: testPipeline,
		RevisionID: testRevision,
	})
	require.NoError(t, err)
	require.NoError(t, h.store.Append(ctx, h.tenant, runstore.Event{
		RunID:    testRun,
		Sequence: 1,
		Type:     runstore.RunCreated,
		Payload:  payload,
		At:       time.Now().UTC(),
	}))
}

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
		// The Postgres database is shared: the outbox drains rows other
		// cases and other packages left behind, and a count that includes
		// them is not testing what it claims to.
		if d.GetTenant().GetId() != h.tenant {
			continue
		}
		ids = append(ids, d.GetStepId())
	}
	return ids
}

func (h *harness) events(ctx context.Context, t *testing.T) []runstore.Event {
	t.Helper()
	events, err := h.store.Replay(ctx, h.tenant, testRun)
	require.NoError(t, err)
	return events
}

func (h *harness) event(ctx context.Context, t *testing.T, kind runstore.EventType) runstore.Event {
	t.Helper()
	for _, e := range h.events(ctx, t) {
		if e.Type == kind {
			return e
		}
	}
	t.Fatalf("no %s event in the run log", kind)
	return runstore.Event{}
}

func (h *harness) countEvents(ctx context.Context, t *testing.T, kind runstore.EventType) int {
	t.Helper()
	n := 0
	for _, e := range h.events(ctx, t) {
		if e.Type == kind {
			n++
		}
	}
	return n
}

// gatedPipeline is an approval gate and the step it protects.
func gatedPipeline() *dholev1.Pipeline {
	step := func(id string, ins, outs []string) *dholev1.Step {
		s := &dholev1.Step{
			Id:          id,
			Name:        id,
			PluginRef:   "cmd://echo",
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			LeaseScope:  dholev1.LeaseScope_LEASE_SCOPE_STEP,
		}
		for _, port := range ins {
			s.Inputs = append(s.Inputs, &dholev1.Port{Name: port})
		}
		for _, port := range outs {
			s.Outputs = append(s.Outputs, &dholev1.Port{Name: port})
		}
		return s
	}
	return &dholev1.Pipeline{
		Id:    testPipeline,
		Steps: []*dholev1.Step{step("gate", nil, []string{"out"}), step("after", []string{"in"}, nil)},
		Edges: []*dholev1.Edge{{FromStep: "gate", FromPort: "out", ToStep: "after", ToPort: "in"}},
	}
}

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

type staticFleet struct {
	instances []registry.Instance
}

func (f staticFleet) Instances(context.Context, string) ([]registry.Instance, error) {
	return f.instances, nil
}

type staticDefs struct {
	pipeline *dholev1.Pipeline
}

func (d staticDefs) Get(_ context.Context, tenantID, _, _ string) (*dholev1.Pipeline, error) {
	if tenantID == "" {
		return nil, runstore.ErrTenantRequired
	}
	return d.pipeline, nil
}

type recordingBus struct {
	mu   sync.Mutex
	sent [][]byte
}

var _ bus.Bus = (*recordingBus)(nil)

func (b *recordingBus) Publish(_ context.Context, _ string, msg proto.Message) error {
	encoded, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sent = append(b.sent, encoded)
	return nil
}

func (b *recordingBus) Request(context.Context, string, proto.Message, proto.Message) error {
	return fmt.Errorf("not used")
}

func (b *recordingBus) SubscribePull(context.Context, string, string, string) (bus.Subscription, error) {
	return nil, fmt.Errorf("not used")
}

func (b *recordingBus) SubscribeEphemeral(context.Context, string, func([]byte)) (func(), error) {
	return nil, fmt.Errorf("not used")
}

func (b *recordingBus) dispatches(t *testing.T) []*dholev1.JobDispatch {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*dholev1.JobDispatch, 0, len(b.sent))
	for _, raw := range b.sent {
		d := &dholev1.JobDispatch{}
		require.NoError(t, proto.Unmarshal(raw, d))
		out = append(out, d)
	}
	return out
}
