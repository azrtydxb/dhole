package wait_test

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
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/outbox"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/wait"
)

const (
	testRun      = "run-1"
	testPipeline = "waiting"
	testRevision = "rev-1"
	testTier     = "trusted"
)

// TestDurableWaitSurvivesRestart is the whole point of ADR 0003. The wait is a
// row, not a goroutine: the process that scheduled it is gone by the time it
// comes due, and the run resumes anyway.
//
// The failure this catches is a timer held in memory — a time.AfterFunc, a
// goroutine that sleeps — which passes every test that never restarts anything
// and loses every in-flight wait on the first deploy.
func TestDurableWaitSurvivesRestart(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx := testContext(t)
		tenant := uniqueTenant(t)
		url := startBus(t)

		first := newPlane(ctx, t, open(t), url, tenant)
		first.seedRun(ctx, t, tenant)

		// The wait is 200ms out, scheduled by the plane that is about to die.
		due := time.Now().UTC().Add(200 * time.Millisecond)
		require.NoError(t, first.timers.Schedule(ctx, tenant, testRun, "hold", due))

		require.NoError(t, first.sched.Advance(ctx, tenant, testRun))
		require.Empty(t, first.drain(ctx, t),
			"a step waiting on a timer is not dispatched to an engine")

		// Stop the world. Nothing about this run survives in memory.
		first.stop(t)

		second := newPlane(ctx, t, open(t), url, tenant)

		// Before the due time nothing fires: a wait that resumes early is not
		// a wait.
		fired, err := second.runner.Tick(ctx, due.Add(-time.Millisecond))
		require.NoError(t, err)
		require.Zero(t, fired, "the timer is not due yet")
		require.Empty(t, second.drain(ctx, t))

		fired, err = second.runner.Tick(ctx, due.Add(time.Millisecond))
		require.NoError(t, err)
		require.Equal(t, 1, fired, "the timer scheduled by the dead plane came due")

		require.Equal(t, []string{"after"}, second.drain(ctx, t),
			"the run resumed on the restarted plane and moved past the wait")
		second.succeed(ctx, t, tenant, "after")
		require.NoError(t, second.sched.Advance(ctx, tenant, testRun))

		require.Equal(t, 1, second.countEvents(ctx, t, tenant, testRun, runstore.RunCompleted),
			"the run completed after a wait that outlived the process that scheduled it")
	})
}

// TestMissedScheduleWindowFiresOnceOnRecovery is the classic catch-up bug: a
// nightly job whose window passed entirely during a weekend outage fires forty
// times when the control plane comes back, because recovery walked the missed
// intervals instead of the missed TIMER.
//
// A timer is one row with one due time. Coming back three days late fires it
// once, exactly as coming back on time would.
func TestMissedScheduleWindowFiresOnceOnRecovery(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx := testContext(t)
		tenant := uniqueTenant(t)
		url := startBus(t)

		p := newPlane(ctx, t, open(t), url, tenant)
		p.seedRun(ctx, t, tenant)

		// Due at midnight; the plane was down for three days.
		due := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
		require.NoError(t, p.timers.Schedule(ctx, tenant, testRun, "hold", due))
		recovered := due.Add(72 * time.Hour)

		outstanding, ok, err := p.timers.Pending(ctx, tenant, testRun, "hold")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, due, outstanding)

		fired, err := p.runner.Tick(ctx, recovered)
		require.NoError(t, err)
		require.Equal(t, 1, fired, "a timer three days overdue is still one timer")

		// The row leaves the poll on the tick that fired it. A timer left
		// outstanding is re-scanned every second forever, and every one of
		// those scans is another chance to fire it again.
		_, ok, err = p.timers.Pending(ctx, tenant, testRun, "hold")
		require.NoError(t, err)
		require.False(t, ok, "a fired timer is still outstanding in the table")

		// Every later poll, however many intervals it thinks it missed, has
		// nothing left to fire.
		for i := range 5 {
			fired, err = p.runner.Tick(ctx, recovered.Add(time.Duration(i)*time.Hour))
			require.NoError(t, err)
			require.Zero(t, fired, "poll %d re-fired a timer that had already fired", i)
		}

		require.Equal(t, 1, p.countEvents(ctx, t, tenant, testRun, wait.StepTimerFired),
			"the log names the wait as having ended exactly once")
		require.Equal(t, []string{"after"}, p.drain(ctx, t),
			"the step behind the wait ran once, not once per missed interval")
	})
}

// TestDueHandsEachTimerToOneClaimantOnly holds Due to the rule the outbox
// drainer already lives by: two control planes poll the same table at the same
// second, and a timer handed to both fires its run twice.
//
// The claim is the same one the outbox makes — FOR UPDATE SKIP LOCKED on
// Postgres, the immediate write lock on SQLite — because a third mechanism
// here would be a third thing to get wrong.
func TestDueHandsEachTimerToOneClaimantOnly(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, dialect runstore.Dialect) {
		ctx := testContext(t)
		tenant := uniqueTenant(t)

		// One row per claim and many rows: the two pollers are then really
		// contending, round after round, instead of one of them taking the
		// whole backlog in a single batch before the other has woken up.
		const timers = 60
		writer := wait.NewTimers(open(t))
		due := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
		for i := range timers {
			run := fmt.Sprintf("run-%d", i)
			require.NoError(t, seedRunEvent(ctx, open(t), tenant, run))
			require.NoError(t, writer.Schedule(ctx, tenant, run, "hold", due))
		}

		// Two planes, two store handles, nothing shared in memory.
		planes := []*wait.Timers{
			wait.NewTimers(open(t), wait.WithClaimBatch(1)),
			wait.NewTimers(open(t), wait.WithClaimBatch(1)),
		}
		var (
			mu       sync.Mutex
			handed   []wait.Due
			perPlane = make([]int, len(planes))
			wg       sync.WaitGroup
			start    = make(chan struct{})
		)
		for i, timers := range planes {
			wg.Add(1)
			go func(plane int, tm *wait.Timers) {
				defer wg.Done()
				<-start
				for {
					batch, err := tm.Due(ctx, due.Add(time.Hour))
					if err != nil {
						mu.Lock()
						t.Errorf("due: %v", err)
						mu.Unlock()
						return
					}
					if len(batch) == 0 {
						return
					}
					mu.Lock()
					handed = append(handed, batch...)
					perPlane[plane] += len(batch)
					mu.Unlock()
				}
			}(i, timers)
		}
		close(start)
		wg.Wait()

		seen := map[string]int{}
		mine := 0
		for _, d := range handed {
			if d.TenantID != tenant {
				continue // Another case's rows: the table is shared.
			}
			mine++
			seen[d.RunID+"/"+d.StepID]++
		}
		require.Equal(t, timers, mine, "every timer was handed out")
		if dialect == runstore.DialectPostgres {
			// SKIP LOCKED is exactly this: the second plane walks PAST the
			// rows the first is holding and keeps working. Without it the
			// second plane blocks on the first row, finds it fired when the
			// lock lifts, and does nothing at all.
			require.NotZero(t, perPlane[0])
			require.NotZero(t, perPlane[1],
				"a plane claimed nothing: the two never contended, or one blocked behind the other")
		}
		for key, n := range seen {
			require.Equal(t, 1, n, "timer %s was handed to both planes", key)
		}

		// And the log agrees: a timer claimed twice would have ended its wait
		// twice, whatever the pollers reported to their callers.
		reader := open(t)
		for i := range timers {
			run := fmt.Sprintf("run-%d", i)
			events, err := reader.Replay(ctx, tenant, run)
			require.NoError(t, err)
			ended := 0
			for _, e := range events {
				if e.Type == wait.StepTimerFired {
					ended++
				}
			}
			require.Equal(t, 1, ended, "%s ended its wait more than once", run)
		}
	})
}

// TestScheduleRejectsAnUnscopedTenant holds waits to the same rule as every
// other stored record: there is no unscoped row, even while only one tenant
// exists.
func TestScheduleRejectsAnUnscopedTenant(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx := testContext(t)
		timers := wait.NewTimers(open(t))

		err := timers.Schedule(ctx, "", testRun, "hold", time.Now())
		require.ErrorIs(t, err, runstore.ErrTenantRequired)
		require.Contains(t, err.Error(), "tenant scope required")

		batch, err := timers.Due(ctx, time.Now().Add(time.Hour))
		require.NoError(t, err)
		for _, d := range batch {
			require.NotEmpty(t, d.TenantID, "a timer with no tenant was stored anyway")
		}
	})
}

// TestDueCarriesTheTenantOfEachTimer is what stops a control plane resuming
// one tenant's run under another's scope: Due is the only cross-tenant read in
// this package, so the scope has to travel on every row it returns.
func TestDueCarriesTheTenantOfEachTimer(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx := testContext(t)
		one, two := uniqueTenant(t), uniqueTenant(t)
		store := open(t)
		timers := wait.NewTimers(store)

		due := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
		for _, tenant := range []string{one, two} {
			require.NoError(t, seedRunEvent(ctx, store, tenant, testRun))
			require.NoError(t, timers.Schedule(ctx, tenant, testRun, "hold", due))
		}

		batch, err := timers.Due(ctx, due.Add(time.Minute))
		require.NoError(t, err)

		scopes := map[string]int{}
		for _, d := range batch {
			scopes[d.TenantID]++
		}
		require.Equal(t, 1, scopes[one])
		require.Equal(t, 1, scopes[two])

		for _, tenant := range []string{one, two} {
			events, err := store.Replay(ctx, tenant, testRun)
			require.NoError(t, err)
			require.NotEmpty(t, events)
		}
	})
}

// TestOrphanedTimerIsRetiredWhenItsRunAlreadyFinished settles what happens to
// a wait whose run ended for another reason — cancelled, failed, or finished
// down another branch.
//
// The timer is RETIRED on the poll that would have fired it: marked fired,
// nothing appended and nothing resumed. Deleting the row on completion instead
// would need every path that ends a run to remember to do it, and the one that
// forgets resurrects a finished run days later.
func TestOrphanedTimerIsRetiredWhenItsRunAlreadyFinished(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx := testContext(t)
		tenant := uniqueTenant(t)
		store := open(t)
		timers := wait.NewTimers(store)

		require.NoError(t, seedRunEvent(ctx, store, tenant, testRun))
		due := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
		require.NoError(t, timers.Schedule(ctx, tenant, testRun, "hold", due))

		// The run ends for its own reasons while the wait is outstanding.
		require.NoError(t, store.Append(ctx, tenant, runstore.Event{
			RunID: testRun, Sequence: 99, Type: runstore.RunCompleted,
			At: time.Now().UTC(),
		}))

		resumed := &countingResumer{}
		runner := wait.NewRunner(timers, resumed)
		fired, err := runner.Tick(ctx, due.Add(time.Hour))
		require.NoError(t, err)
		require.Zero(t, fired, "a finished run is not resumed by its outstanding wait")
		require.Zero(t, resumed.count.Load())

		events, err := store.Replay(ctx, tenant, testRun)
		require.NoError(t, err)
		for _, e := range events {
			require.NotEqual(t, wait.StepTimerFired, e.Type,
				"a retired timer appends nothing to a finished run")
		}

		// And it is gone from the poll, not scanned forever.
		fired, err = runner.Tick(ctx, due.Add(2*time.Hour))
		require.NoError(t, err)
		require.Zero(t, fired)
	})
}

// TestRunnerPollsOnItsOwn is the part Tick cannot prove: the loop actually
// looks. A Run that never polls passes every hand-driven case in this file and
// leaves every wait in production outstanding forever.
func TestRunnerPollsOnItsOwn(t *testing.T) {
	ctx := testContext(t)
	tenant := uniqueTenant(t)
	path := filepath.Join(t.TempDir(), "run.db")
	store, err := runstore.NewSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	timers := wait.NewTimers(store)
	require.NoError(t, seedRunEvent(ctx, store, tenant, testRun))
	require.NoError(t, timers.Schedule(ctx, tenant, testRun, "hold",
		time.Now().UTC().Add(20*time.Millisecond)))

	resumed := &countingResumer{}
	runner := wait.NewRunner(timers, resumed, wait.WithPollInterval(10*time.Millisecond))

	loop, stop := context.WithCancel(ctx)
	defer stop()
	go func() { _ = runner.Run(loop) }()

	require.Eventually(t, func() bool { return resumed.count.Load() == 1 },
		5*time.Second, 5*time.Millisecond,
		"the runner never polled: the wait stayed outstanding")
}

// countingResumer stands in for the scheduler where the case is about the
// timer table rather than about the run. The cases that assert a run really
// moves use the real scheduler.
type countingResumer struct {
	count atomic.Int64
}

func (c *countingResumer) Advance(context.Context, string, string) error {
	c.count.Add(1)
	return nil
}

// seedRunEvent writes the RUN_CREATED a run starts from, without a plane.
func seedRunEvent(ctx context.Context, store runstore.Store, tenant, runID string) error {
	payload, err := scheduler.MarshalRunCreated(scheduler.RunCreated{
		PipelineID: testPipeline,
		RevisionID: testRevision,
	})
	if err != nil {
		return err
	}
	return store.Append(ctx, tenant, runstore.Event{
		RunID: runID, Sequence: 1, Type: runstore.RunCreated,
		Payload: payload, At: time.Now().UTC(),
	})
}

// ---------------------------------------------------------------------------
// Harness. The store, the bus, the lease manager and the scheduler are all
// real: a fake of any of them would decide the outcome of exactly the
// properties these tests exist to hold.

type storeOpener func(t *testing.T) runstore.Store

// eachStore runs a case against both dialects. SQLite reopens the same file,
// so "restart" means what it says; Postgres reopens the same database.
func eachStore(t *testing.T, fn func(t *testing.T, open storeOpener, dialect runstore.Dialect)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "run.db")
		fn(t, func(t *testing.T) runstore.Store {
			t.Helper()
			store, err := runstore.NewSQLite(path)
			require.NoError(t, err)
			return store
		}, runstore.DialectSQLite)
	})
	t.Run("postgres", func(t *testing.T) {
		if scopedDSN == "" {
			t.Skip("DHOLE_TEST_POSTGRES_DSN not set")
		}
		dsn := scopedDSN
		fn(t, func(t *testing.T) runstore.Store {
			t.Helper()
			store, err := runstore.NewPostgres(context.Background(), dsn)
			require.NoError(t, err)
			return store
		}, runstore.DialectPostgres)
	})
}

// Postgres is one shared database for the whole suite, and these cases write
// to tables other packages assert counts on — the outbox above all. Each test
// binary therefore gets its OWN SCHEMA: the migrations run inside it, every
// table this package touches is private to it, and nothing here can be
// mistaken for another package's backlog. The schema goes when the run does.
const testSchema = "dhole_wait_test"

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

// uniqueTenant keeps cases apart in the shared Postgres database.
func uniqueTenant(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("wait-%d-%d", time.Now().UnixNano(), tenantSeq.Add(1))
}

func startBus(t *testing.T) string {
	t.Helper()
	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	return srv.URL()
}

// plane is one control plane: everything a process holds, so that stopping it
// really does drop everything in memory.
type plane struct {
	tenant string
	store  runstore.Store
	bus    *recordingBus
	outbox *outbox.Outbox
	sched  *scheduler.Scheduler
	timers *wait.Timers
	runner *wait.Runner
}

func newPlane(ctx context.Context, t *testing.T, store runstore.Store, url, tenant string) *plane {
	t.Helper()

	conn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	recorder := &recordingBus{}
	ob := outbox.New(store, recorder, "test-plane")
	sched, err := scheduler.New(scheduler.Config{
		Store:       store,
		Outbox:      ob,
		Leases:      leases,
		Fleet:       staticFleet{instances: []registry.Instance{readyEngine("e1")}},
		Definitions: staticDefs{pipeline: waitingPipeline()},
		Tier:        testTier,
		OS:          "linux",
		Arch:        "amd64",
		EnvIdentity: "sha256:env",
	})
	require.NoError(t, err)

	timers := wait.NewTimers(store)
	p := &plane{
		tenant: tenant, store: store, bus: recorder, outbox: ob, sched: sched,
		timers: timers, runner: wait.NewRunner(timers, sched),
	}
	t.Cleanup(func() { _ = store.Close() })
	return p
}

// stop is a control-plane restart: the handle closes and every goroutine,
// channel and timer this process held goes with it.
func (p *plane) stop(t *testing.T) {
	t.Helper()
	require.NoError(t, p.store.Close())
}

func (p *plane) seedRun(ctx context.Context, t *testing.T, tenant string) {
	t.Helper()
	payload, err := scheduler.MarshalRunCreated(scheduler.RunCreated{
		PipelineID: testPipeline,
		RevisionID: testRevision,
	})
	require.NoError(t, err)
	require.NoError(t, p.store.Append(ctx, tenant, runstore.Event{
		RunID:    testRun,
		Sequence: 1,
		Type:     runstore.RunCreated,
		Payload:  payload,
		At:       time.Now().UTC(),
	}))
}

// drain publishes whatever the scheduler owed and returns the step ids that
// reached the bus.
func (p *plane) drain(ctx context.Context, t *testing.T) []string {
	t.Helper()
	for {
		n, err := p.outbox.Drain(ctx)
		require.NoError(t, err)
		if n == 0 {
			break
		}
	}
	var ids []string
	for _, d := range p.bus.dispatches(t) {
		// The Postgres database is shared: the outbox drains rows other
		// cases and other packages left behind, and a count that includes
		// them is not testing what it claims to.
		if d.GetTenant().GetId() != p.tenant {
			continue
		}
		ids = append(ids, d.GetStepId())
	}
	return ids
}

func (p *plane) succeed(ctx context.Context, t *testing.T, tenant, stepID string) {
	t.Helper()
	for _, d := range p.bus.dispatches(t) {
		if d.GetStepId() != stepID || d.GetTenant().GetId() != tenant {
			continue
		}
		require.NoError(t, p.sched.OnStatus(ctx, &dholev1.JobStatus{
			RunId:      d.GetRunId(),
			StepId:     d.GetStepId(),
			Attempt:    d.GetAttempt(),
			FenceToken: d.GetFenceToken(),
			Phase:      dholev1.Phase_PHASE_SUCCEEDED,
		}))
		return
	}
	t.Fatalf("step %q was never dispatched", stepID)
}

func (p *plane) countEvents(
	ctx context.Context, t *testing.T, tenant, runID string, kind runstore.EventType,
) int {
	t.Helper()
	events, err := p.store.Replay(ctx, tenant, runID)
	require.NoError(t, err)
	n := 0
	for _, e := range events {
		if e.Type == kind {
			n++
		}
	}
	return n
}

// waitingPipeline is a wait gate and the step behind it: "after" cannot run
// until "hold" has finished waiting.
func waitingPipeline() *dholev1.Pipeline {
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
		Steps: []*dholev1.Step{step("hold", nil, []string{"out"}), step("after", []string{"in"}, nil)},
		Edges: []*dholev1.Edge{{FromStep: "hold", FromPort: "out", ToStep: "after", ToPort: "in"}},
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

// recordingBus stands in for NATS on the publish side only; everything whose
// behaviour is under test here is real.
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
