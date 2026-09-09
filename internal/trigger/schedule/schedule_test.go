package schedule_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/trigger"
	"github.com/azrtydxb/dhole/internal/trigger/schedule"
)

// everySecond is the expression the plan names. Six fields: the seconds field
// is what makes a test of a cron boundary finish before the reviewer does.
const everySecond = "* * * * * *"

// TestScheduleFiresAtCronBoundary is the base case: the trigger supplies the
// pipeline's inputs on its own, repeatedly, off a real clock.
//
// It fires on the wall clock rather than a driven one on purpose — this is the
// one case that would still pass if Start never started anything and the
// caller ticked by hand.
func TestScheduleFiresAtCronBoundary(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx, cancel := context.WithCancel(testContext(t))
		defer cancel()

		tenant := uniqueTenant(t)
		sink := newRecordingSink()
		s, err := schedule.New(schedule.Config{
			ID:         "nightly",
			TenantID:   tenant,
			Expression: everySecond,
			Binding: trigger.Binding{
				PipelineID:   testPipelineID,
				InputMapping: map[string]string{"payload": "scheduled_for"},
			},
			Pipeline:     testPipeline(),
			Store:        open(t),
			PollInterval: 20 * time.Millisecond,
		})
		require.NoError(t, err)
		require.Equal(t, "schedule", s.Kind())

		done := make(chan error, 1)
		go func() { done <- s.Start(ctx, sink) }()

		require.True(t, sink.wait(2, 3*time.Second),
			"a `%s` schedule fired %d times in 3s, expected at least 2",
			everySecond, sink.count())

		for _, f := range sink.fires() {
			require.Equal(t, tenant, f.tenantID)
			require.Equal(t, testPipelineID, f.pipelineID)
			require.Contains(t, f.inputs, "payload",
				"the bound input is not populated")
			require.NotEmpty(t, f.inputs["payload"].GetStringValue())
		}

		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})
}

// TestMissedScheduleWindowFiresOnceOnRecovery is the catch-up bug, refused.
//
// A control plane is down for three hours and a minutely schedule misses 180
// occurrences. On restart the schedule fires ONCE — it is a due time in a row,
// not a queue of intervals to replay — and the run it starts is the missed
// occurrence, so the log says which boundary was served.
//
// The failure this catches is the loop that walks Next() forward from the
// stored due time firing at every step, which is how a nightly job runs forty
// times after a weekend outage and how an hourly one takes a database with it.
func TestMissedScheduleWindowFiresOnceOnRecovery(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx := testContext(t)
		tenant := uniqueTenant(t)
		sink := newRecordingSink()

		// Time is driven, not slept through: an outage measured in hours is
		// not something a test can wait out, and a generous sleep would hide
		// a schedule that never fired at all.
		base := time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC)
		plane := func() *schedule.Schedule {
			s, err := schedule.New(schedule.Config{
				ID:         "hourly-report",
				TenantID:   tenant,
				Expression: "0 * * * * *", // every minute, on the minute
				Binding: trigger.Binding{
					PipelineID:   testPipelineID,
					InputMapping: map[string]string{"payload": "scheduled_for"},
				},
				Pipeline: testPipeline(),
				Store:    open(t),
			})
			require.NoError(t, err)
			return s
		}

		// The plane that records the next due time, then dies.
		out, err := plane().Tick(ctx, sink, base)
		require.NoError(t, err)
		require.False(t, out.Fired, "nothing is due at the instant the schedule is created")

		// Three hours of downtime: 180 occurrences elapsed with nobody home.
		recovered := base.Add(3 * time.Hour)
		out, err = plane().Tick(ctx, sink, recovered)
		require.NoError(t, err)
		require.True(t, out.Fired, "the schedule missed its whole window and did not fire on recovery")
		require.Equal(t, 1, sink.count(), "the missed window fired %d times, expected exactly once", sink.count())

		require.Equal(t, base.Add(time.Minute).Format(time.RFC3339Nano),
			sink.fires()[0].inputs["payload"].GetStringValue(),
			"the run is not the missed occurrence")

		// And the backlog does not come back on the next poll.
		for i := 0; i < 5; i++ {
			out, err = plane().Tick(ctx, sink, recovered)
			require.NoError(t, err)
			require.False(t, out.Fired)
		}
		require.Equal(t, 1, sink.count())
	})
}

// TestConcurrencyBudgetOfOneSkipsOverlappingFire keeps a slow run from
// building a queue behind it.
//
// A minutely schedule whose run takes ten minutes must not accumulate nine
// pending fires and then release them at once; that is how a schedule takes a
// system down at 3am. The overlapping occurrence is SKIPPED, with a reason
// recorded, and the schedule moves on to the next boundary.
//
// The skip is durable — it is the row that says a fire is in flight, not a
// counter in one process — so a second control plane skips it too.
func TestConcurrencyBudgetOfOneSkipsOverlappingFire(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx := testContext(t)
		tenant := uniqueTenant(t)
		sink := newRecordingSink()

		var (
			mu    sync.Mutex
			skips []schedule.Skip
		)
		s, err := schedule.New(schedule.Config{
			ID:         "slow",
			TenantID:   tenant,
			Expression: everySecond,
			Binding: trigger.Binding{
				PipelineID:   testPipelineID,
				InputMapping: map[string]string{"payload": "scheduled_for"},
			},
			Pipeline: testPipeline(),
			Store:    open(t),
			OnSkip: func(sk schedule.Skip) {
				mu.Lock()
				defer mu.Unlock()
				skips = append(skips, sk)
			},
		})
		require.NoError(t, err)

		base := time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC)
		_, err = s.Tick(ctx, sink, base)
		require.NoError(t, err)

		// The first occurrence fires and its run is still going.
		sink.block()
		done := make(chan schedule.Outcome, 1)
		go func() {
			out, err := s.Tick(ctx, sink, base.Add(time.Second))
			require.NoError(t, err)
			done <- out
		}()
		select {
		case <-sink.entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the first occurrence never reached the sink")
		}

		// The next occurrence comes due while it is still in flight.
		out, err := s.Tick(ctx, sink, base.Add(2*time.Second))
		require.NoError(t, err)
		require.False(t, out.Fired, "an overlapping occurrence was fired anyway")
		require.True(t, out.Skipped, "an overlapping occurrence was neither fired nor skipped")
		require.Contains(t, out.Reason, "still in flight",
			"the skip carries no usable reason")
		require.Equal(t, 1, sink.count())

		mu.Lock()
		require.Len(t, skips, 1, "the skip was not recorded")
		require.Equal(t, tenant, skips[0].TenantID)
		require.Equal(t, base.Add(2*time.Second), skips[0].ScheduledFor)
		require.Contains(t, skips[0].Reason, "still in flight")
		mu.Unlock()

		// The budget does not leak one occurrence at a time: the run is still
		// in flight, so the occurrence AFTER the skipped one is skipped too.
		out, err = s.Tick(ctx, sink, base.Add(3*time.Second))
		require.NoError(t, err)
		require.True(t, out.Skipped,
			"the second overlapping occurrence was let through: the skip cleared the in-flight mark")
		require.Equal(t, 1, sink.count())

		sink.unblock()
		require.True(t, (<-done).Fired)

		// The skipped occurrences are not queued: polling again at the same
		// instants fires nothing, because those boundaries are gone.
		for _, at := range []time.Time{base.Add(2 * time.Second), base.Add(3 * time.Second)} {
			out, err = s.Tick(ctx, sink, at)
			require.NoError(t, err)
			require.False(t, out.Fired)
		}
		require.Equal(t, 1, sink.count())

		// The next boundary is served normally.
		out, err = s.Tick(ctx, sink, base.Add(4*time.Second))
		require.NoError(t, err)
		require.True(t, out.Fired)
		require.Equal(t, 2, sink.count())
	})
}

// TestTwoControlPlanesFireOneOccurrenceOnce is the claim, tested with two
// planes because one cannot prove it.
//
// Both poll the same row at the same instant. Exactly one fires: the claim is
// the store's — FOR UPDATE SKIP LOCKED on Postgres, the immediate write lock
// on SQLite — and the due time advances inside it, so the loser sees a
// schedule that is no longer due rather than a second copy of the occurrence.
func TestTwoControlPlanesFireOneOccurrenceOnce(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx := testContext(t)
		tenant := uniqueTenant(t)
		sink := newRecordingSink()

		plane := func() *schedule.Schedule {
			s, err := schedule.New(schedule.Config{
				ID:         "shared",
				TenantID:   tenant,
				Expression: everySecond,
				Binding: trigger.Binding{
					PipelineID:   testPipelineID,
					InputMapping: map[string]string{"payload": "scheduled_for"},
				},
				Pipeline: testPipeline(),
				// Its OWN handle on the same database: two planes, not two
				// pointers to one connection pool.
				Store: open(t),
			})
			require.NoError(t, err)
			return s
		}
		first, second := plane(), plane()

		base := time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC)
		_, err := first.Tick(ctx, sink, base)
		require.NoError(t, err)

		// One occurrence is one race, and one race that happens to come out
		// right proves nothing: the window between reading the row and
		// advancing it is narrow, so the two planes are raced repeatedly and
		// every round must produce exactly one fire.
		const rounds = 25
		for i := 1; i <= rounds; i++ {
			due := base.Add(time.Duration(i) * time.Second)
			var (
				fired atomic.Int64
				start = make(chan struct{})
				wg    sync.WaitGroup
			)
			for _, s := range []*schedule.Schedule{first, second} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					out, err := s.Tick(ctx, sink, due)
					require.NoError(t, err)
					if out.Fired {
						fired.Add(1)
					}
				}()
			}
			close(start)
			wg.Wait()

			require.Equal(t, int64(1), fired.Load(),
				"round %d: two control planes both fired one occurrence", i)
			require.Equal(t, i, sink.count(), "round %d", i)
		}
	})
}

// TestFiresIntoExactlyOneTenant. Every fire carries the scope it was
// configured with, and two tenants running the same schedule id are two
// schedules that never see each other's row.
func TestFiresIntoExactlyOneTenant(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener, _ runstore.Dialect) {
		ctx := testContext(t)
		store := open(t)
		base := time.Date(2026, 3, 1, 2, 0, 0, 0, time.UTC)

		fire := func(tenant string) []fire {
			sink := newRecordingSink()
			s, err := schedule.New(schedule.Config{
				ID:         "shared-id",
				TenantID:   tenant,
				Expression: everySecond,
				Binding: trigger.Binding{
					PipelineID:   testPipelineID,
					InputMapping: map[string]string{"payload": "scheduled_for"},
				},
				Pipeline: testPipeline(),
				Store:    store,
			})
			require.NoError(t, err)
			_, err = s.Tick(ctx, sink, base)
			require.NoError(t, err)
			out, err := s.Tick(ctx, sink, base.Add(time.Second))
			require.NoError(t, err)
			require.True(t, out.Fired)
			return sink.fires()
		}

		one, two := uniqueTenant(t), uniqueTenant(t)
		firstFires, secondFires := fire(one), fire(two)

		require.Len(t, firstFires, 1)
		require.Equal(t, one, firstFires[0].tenantID)
		require.Len(t, secondFires, 1,
			"the second tenant did not fire: the schedule row is not tenant-scoped")
		require.Equal(t, two, secondFires[0].tenantID)
	})
}

// TestConfigurationIsRefusedWithoutATenant. There is no unscoped trigger.
func TestConfigurationIsRefusedWithoutATenant(t *testing.T) {
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "run.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	_, err = schedule.New(schedule.Config{
		ID:         "unscoped",
		Expression: everySecond,
		Binding:    trigger.Binding{PipelineID: testPipelineID},
		Pipeline:   testPipeline(),
		Store:      store,
	})
	require.ErrorIs(t, err, trigger.ErrTenantRequired)
	require.Contains(t, err.Error(), "tenant scope required")
}

// TestMalformedCronIsRejectedAtConfiguration. The expression is in the
// message: "invalid schedule" tells whoever is reading a log at 3am nothing
// about which of forty schedules is broken.
func TestMalformedCronIsRejectedAtConfiguration(t *testing.T) {
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "run.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	for _, expr := range []string{"every tuesday", "* * *", "99 * * * * *", ""} {
		_, err := schedule.New(schedule.Config{
			ID:         "broken",
			TenantID:   "acme",
			Expression: expr,
			Binding:    trigger.Binding{PipelineID: testPipelineID},
			Pipeline:   testPipeline(),
			Store:      store,
		})
		require.Error(t, err, "expression %q was accepted", expr)
		require.Contains(t, err.Error(), fmt.Sprintf("%q", expr),
			"the error does not name the expression it rejected")
	}
}

// TestBindingIsCheckedAgainstDeclaredInputPorts. ADR 0007's payoff: because a
// trigger binds to typed ports, a binding onto a port that does not exist is a
// configuration error, not a run that dies deep inside its first step.
//
// A pipeline's inputs are its free input ports — the ones no edge feeds.
// `artifact` is fed by an edge, so it is not the outside world's to supply,
// and `src` is a blob, which a schedule has no way to produce.
func TestBindingIsCheckedAgainstDeclaredInputPorts(t *testing.T) {
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "run.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	cfg := func(mapping map[string]string) schedule.Config {
		return schedule.Config{
			ID:         "checked",
			TenantID:   "acme",
			Expression: everySecond,
			Binding: trigger.Binding{
				PipelineID:   testPipelineID,
				InputMapping: mapping,
			},
			Pipeline: testPipeline(),
			Store:    store,
		}
	}

	_, err = schedule.New(cfg(map[string]string{"payload": "fired_at"}))
	require.NoError(t, err, "a binding onto a declared structured input must be accepted")

	_, err = schedule.New(cfg(map[string]string{"paylod": "fired_at"}))
	require.Error(t, err, "a binding onto an undeclared input was accepted")
	require.Contains(t, err.Error(), `"paylod"`)
	require.Contains(t, err.Error(), "does not declare")

	_, err = schedule.New(cfg(map[string]string{"artifact": "fired_at"}))
	require.Error(t, err, "a binding onto a port fed by an edge was accepted")
	require.Contains(t, err.Error(), "does not declare")

	_, err = schedule.New(cfg(map[string]string{"src": "fired_at"}))
	require.Error(t, err, "a binding onto a blob port was accepted")
	require.Contains(t, err.Error(), "not a structured port")

	_, err = schedule.New(cfg(map[string]string{"payload": "phase_of_the_moon"}))
	require.Error(t, err, "a binding from an event field the schedule has no way to fill was accepted")
	require.Contains(t, err.Error(), `"phase_of_the_moon"`)
}

// TestStartStopsCleanlyOnCancellation. Start owns what it started: it returns
// the context's error, leaves no goroutine behind, and — the part a goroutine
// count does not prove — leaves no timer still firing.
func TestStartStopsCleanlyOnCancellation(t *testing.T) {
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "run.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	sink := newRecordingSink()
	s, err := schedule.New(schedule.Config{
		ID:         "cancelled",
		TenantID:   "acme",
		Expression: everySecond,
		Binding: trigger.Binding{
			PipelineID:   testPipelineID,
			InputMapping: map[string]string{"payload": "scheduled_for"},
		},
		Pipeline:     testPipeline(),
		Store:        store,
		PollInterval: 5 * time.Millisecond,
	})
	require.NoError(t, err)

	// Settle first: the runtime's own goroutines, and the SQLite driver's,
	// must not be counted as this trigger's.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Start(ctx, sink) }()
	require.True(t, sink.wait(1, 3*time.Second), "the schedule never fired, so nothing was proved")

	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return within 2s of cancellation")
	}

	// A leaked ticker keeps firing. This is the assertion a goroutine count
	// cannot make — and the window has to be longer than the schedule's own
	// period, or a leak that fires once a second sits out the whole check.
	settled := sink.count()
	time.Sleep(2 * time.Second)
	require.Equal(t, settled, sink.count(), "the schedule fired after its context was cancelled")

	// No allowance: the goroutines this test did not start were counted
	// before it started any, and a leaked poll is exactly one more.
	var after int
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if after = runtime.NumGoroutine(); after <= before {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.LessOrEqual(t, after, before,
		"Start leaked a goroutine (%d before, %d after)", before, after)
}

// --- fixtures -------------------------------------------------------------

const testPipelineID = "nightly-report"

// testPipeline declares two free input ports — `payload`, structured, and
// `src`, a blob — plus one port fed by an edge, which is therefore NOT a
// pipeline input and must not be bindable.
func testPipeline() *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id: testPipelineID,
		Steps: []*dholev1.Step{
			{
				Id: "build",
				Inputs: []*dholev1.Port{
					{Name: "payload", Type: &dholev1.PortType{
						Kind: &dholev1.PortType_Structured{
							Structured: &dholev1.StructType{SchemaId: "https://dhole.dev/schema/tick"},
						},
					}},
					{Name: "src", Type: &dholev1.PortType{
						Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{}},
					}},
				},
				Outputs: []*dholev1.Port{
					{Name: "artifact", Type: &dholev1.PortType{
						Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{}},
					}},
				},
			},
			{
				Id: "publish",
				Inputs: []*dholev1.Port{
					{Name: "artifact", Type: &dholev1.PortType{
						Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{}},
					}},
				},
			},
		},
		Edges: []*dholev1.Edge{
			{FromStep: "build", FromPort: "artifact", ToStep: "publish", ToPort: "artifact"},
		},
	}
}

type fire struct {
	tenantID   string
	pipelineID string
	inputs     map[string]*structpb.Value
}

// recordingSink is what a trigger fires into. It blocks only when a test asks
// it to: the concurrency budget cannot be observed without a fire that is
// still in flight when the next one comes due.
type recordingSink struct {
	mu      sync.Mutex
	got     []fire
	hold    chan struct{}
	held    chan struct{}
	entered chan struct{}
}

func newRecordingSink() *recordingSink {
	return &recordingSink{entered: make(chan struct{}, 64)}
}

func (s *recordingSink) Fire(
	_ context.Context, tenantID, pipelineID string, inputs map[string]*structpb.Value,
) error {
	s.mu.Lock()
	s.got = append(s.got, fire{tenantID: tenantID, pipelineID: pipelineID, inputs: inputs})
	// The hold is ONE-SHOT: only the fire that is supposed to overlap waits.
	// A block that caught every fire would turn a trigger that ignores the
	// concurrency budget into a test that hangs instead of one that fails,
	// and a hang says far less than an assertion.
	hold := s.hold
	s.hold = nil
	s.mu.Unlock()

	select {
	case s.entered <- struct{}{}:
	default:
	}
	if hold != nil {
		<-hold
	}
	return nil
}

// block makes the NEXT fire wait until unblock is called.
func (s *recordingSink) block() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.held = make(chan struct{})
	s.hold = s.held
}

func (s *recordingSink) unblock() {
	s.mu.Lock()
	held := s.held
	s.held, s.hold = nil, nil
	s.mu.Unlock()
	if held != nil {
		close(held)
	}
}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

func (s *recordingSink) fires() []fire {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]fire(nil), s.got...)
}

func (s *recordingSink) wait(n int, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if s.count() >= n {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return s.count() >= n
}

// --- both dialects --------------------------------------------------------

type storeOpener func(t *testing.T) runstore.Store

// eachStore runs a case against SQLite and, when a DSN is set, Postgres. The
// opener is a function rather than a store because a control plane that
// restarts, or a second one, needs its own handle on the same database.
func eachStore(t *testing.T, fn func(t *testing.T, open storeOpener, dialect runstore.Dialect)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "run.db")
		fn(t, func(t *testing.T) runstore.Store {
			t.Helper()
			store, err := runstore.NewSQLite(path)
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })
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
			t.Cleanup(func() { _ = store.Close() })
			return store
		}, runstore.DialectPostgres)
	})
}

// Postgres is one shared database for the whole suite, so this binary gets its
// own schema: the migrations run inside it and nothing here can disturb, or be
// disturbed by, another package's rows.
const testSchema = "dhole_trigger_test"

// scopedDSN is the shared database reached through this run's own schema.
var scopedDSN string

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
	return fmt.Sprintf("trigger-%d-%d", time.Now().UnixNano(), tenantSeq.Add(1))
}
