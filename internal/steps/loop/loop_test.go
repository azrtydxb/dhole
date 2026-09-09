package loop_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/dag"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/steps/loop"
)

const (
	testRun  = "run-1"
	loopStep = "retry-until-green"
)

// TestBoundedLoopCeilingAndActionSpaceRefusal is the whole reason ADR 0015
// makes iteration a NODE rather than a cycle: a loop whose exit condition
// never holds has to stop, say so in the run log, and stop being the
// scheduler's problem.
//
// The exit condition here is a constant false. There is no input that makes it
// true and no number of iterations that reaches it — which is exactly the
// shape of the bug this ceiling exists to survive.
func TestBoundedLoopCeilingAndActionSpaceRefusal(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		store, _, _ := open(t)
		tenant := uniqueTenant(t)

		var ran atomic.Int64
		l, err := loop.New(loop.Node{
			Subgraph:      subgraph("body"),
			MaxIterations: 3,
			ExitCondition: "false",
		}, loop.Options{
			Store:    store,
			TenantID: tenant,
			Body: func(_ context.Context, it loop.Iteration) (map[string]any, error) {
				ran.Add(1)
				return map[string]any{"attempt": it.Number}, nil
			},
		})
		require.NoError(t, err)

		res, err := l.Run(ctx, testRun, loopStep, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, loop.ErrIterationCeiling)
		require.Equal(t, int64(3), ran.Load(),
			"the body ran exactly MaxIterations times — not one more, not forever")
		require.Equal(t, 3, res.Iterations)
		require.False(t, res.Exited)

		// The ceiling is IN THE LOG. A loop that stopped without saying so
		// leaves an operator staring at a run that simply ended.
		ceiling := onlyEvent(ctx, t, store, tenant, loop.EventCeilingReached)
		record, err := loop.UnmarshalIteration(ceiling.Payload)
		require.NoError(t, err)
		require.Contains(t, record.Reason, "iteration ceiling reached")
		require.Contains(t, record.Reason, loopStep)
		require.Equal(t, 3, record.Of)
		// It stopped because THIS loop ran out, not because a shared budget
		// happened to run out first. The two endings are different bugs to
		// fix, and a ceiling that is really the budget catching an off-by-one
		// is a ceiling nobody has tested.
		require.Contains(t, record.Reason, "ran its maximum of 3 iterations")
		require.NotContains(t, record.Reason, "budget")

		// Unrolled: one iteration event per pass, each naming its own step id,
		// so the run view can expand the container into what actually ran.
		started := eventsOfType(ctx, t, store, tenant, loop.EventIterationStarted)
		require.Len(t, started, 3)
		var ids []string
		for i, e := range started {
			rec, err := loop.UnmarshalIteration(e.Payload)
			require.NoError(t, err)
			require.Equal(t, i+1, rec.Iteration)
			require.Equal(t, loopStep, rec.Loop)
			ids = append(ids, e.StepID)
		}
		require.Equal(t,
			[]string{loopStep + "#1", loopStep + "#2", loopStep + "#3"}, ids,
			"each iteration has its own step id; a run view cannot unroll three events that all say the same thing")
	})
}

// TestLoopExitsWhenTheConditionHolds is the other half of the ceiling: a loop
// that CAN finish must finish on its condition, before the ceiling, and record
// that it exited rather than that it ran out.
func TestLoopExitsWhenTheConditionHolds(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		store, _, _ := open(t)
		tenant := uniqueTenant(t)

		l, err := loop.New(loop.Node{
			Subgraph:      subgraph("body"),
			MaxIterations: 10,
			ExitCondition: "state.green",
		}, loop.Options{
			Store:    store,
			TenantID: tenant,
			Body: func(_ context.Context, it loop.Iteration) (map[string]any, error) {
				return map[string]any{"green": it.Number == 2}, nil
			},
		})
		require.NoError(t, err)

		res, err := l.Run(ctx, testRun, loopStep, nil)
		require.NoError(t, err)
		require.True(t, res.Exited)
		require.Equal(t, 2, res.Iterations)
		require.Empty(t, eventsOfType(ctx, t, store, tenant, loop.EventCeilingReached),
			"a loop that exited on its condition did not hit its ceiling")
		require.Len(t, eventsOfType(ctx, t, store, tenant, loop.EventExited), 1)
	})
}

// TestTopLevelGraphRemainsAcyclicWithLoopNode. ADR 0015's first claim: the
// graph stays acyclic and analysable BECAUSE iteration is a node. The loop
// node is an ordinary step to dag.Build, and its subgraph is a pipeline
// validated on its own — which is what keeps a cycle hidden inside a loop body
// from being a cycle nobody checked.
func TestTopLevelGraphRemainsAcyclicWithLoopNode(t *testing.T) {
	store, _, _ := openMemory(t)
	tenant := uniqueTenant(t)

	top := &dholev1.Pipeline{
		Id: "release",
		Steps: []*dholev1.Step{
			{Id: "build"},
			{Id: loopStep, PluginRef: loop.PluginRef},
			{Id: "publish"},
		},
		Edges: []*dholev1.Edge{
			{FromStep: "build", FromPort: "out", ToStep: loopStep, ToPort: "in"},
			{FromStep: loopStep, FromPort: "out", ToStep: "publish", ToPort: "in"},
		},
	}
	g, err := dag.Build(top)
	require.NoError(t, err, "a loop node is a node; the top-level graph is still a DAG")
	require.Equal(t, [][]string{{"build"}, {loopStep}, {"publish"}}, g.TopoLevels())

	// The subgraph is validated INDEPENDENTLY, at configuration time. A cycle
	// inside the body is refused even though the graph above it is perfect.
	cyclic := &dholev1.Pipeline{
		Id:    "body",
		Steps: []*dholev1.Step{{Id: "a"}, {Id: "b"}},
		Edges: []*dholev1.Edge{
			{FromStep: "a", FromPort: "o", ToStep: "b", ToPort: "i"},
			{FromStep: "b", FromPort: "o", ToStep: "a", ToPort: "i"},
		},
	}
	_, err = loop.New(loop.Node{
		Subgraph: cyclic, MaxIterations: 2, ExitCondition: "true",
	}, loop.Options{Store: store, TenantID: tenant, Body: noBody})
	require.Error(t, err)
	require.ErrorIs(t, err, loop.ErrSubgraphInvalid)
	require.Contains(t, err.Error(), "cycle",
		"the refusal names what dag.Build found, so the author can fix it")

	// And the same subgraph, acyclic, is accepted.
	_, err = loop.New(loop.Node{
		Subgraph: subgraph("body"), MaxIterations: 2, ExitCondition: "true",
	}, loop.Options{Store: store, TenantID: tenant, Body: noBody})
	require.NoError(t, err)
}

// TestUnboundedLoopIsRefusedAtConfiguration. A MaxIterations of zero is not
// "no limit configured yet", and a negative one is not a limit at all. Both
// are the unbounded loop this task exists to prevent, so both are refused
// where the mistake was made rather than discovered at 3am.
func TestUnboundedLoopIsRefusedAtConfiguration(t *testing.T) {
	store, _, _ := openMemory(t)
	tenant := uniqueTenant(t)

	for _, max := range []int{0, -1, -1000} {
		t.Run(fmt.Sprintf("max=%d", max), func(t *testing.T) {
			l, err := loop.New(loop.Node{
				Subgraph: subgraph("body"), MaxIterations: max, ExitCondition: "true",
			}, loop.Options{Store: store, TenantID: tenant, Body: noBody})
			require.Error(t, err)
			require.ErrorIs(t, err, loop.ErrUnbounded)
			require.Nil(t, l, "no loop is better than an unbounded one")
		})
	}
}

// TestExitConditionErrorStopsTheLoop. internal/policy fails closed: a rule
// that errors denies. A loop's exit condition is the same kind of question
// asked the other way round, so an expression that cannot be evaluated STOPS
// the loop. Carrying on would be the one interpretation that turns a typo into
// an unbounded-in-practice loop that only the ceiling saves.
func TestExitConditionErrorStopsTheLoop(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		store, _, _ := open(t)
		tenant := uniqueTenant(t)

		var ran atomic.Int64
		l, err := loop.New(loop.Node{
			Subgraph:      subgraph("body"),
			MaxIterations: 5,
			// `state.done` type-checks (state is map<string, dyn>) and fails
			// at evaluation, because the body never sets that key.
			ExitCondition: "state.done",
		}, loop.Options{
			Store:    store,
			TenantID: tenant,
			Body: func(context.Context, loop.Iteration) (map[string]any, error) {
				ran.Add(1)
				return map[string]any{"something": "else"}, nil
			},
		})
		require.NoError(t, err)

		res, err := l.Run(ctx, testRun, loopStep, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, loop.ErrExitCondition)
		require.Equal(t, int64(1), ran.Load(),
			"the loop stopped on the first unanswerable condition; it did not keep going")
		require.Equal(t, 1, res.Iterations)
		require.False(t, res.Exited)
		require.Empty(t, eventsOfType(ctx, t, store, tenant, loop.EventCeilingReached),
			"stopping on an error is not hitting the ceiling; the log must not confuse them")
		require.Len(t, eventsOfType(ctx, t, store, tenant, loop.EventFailed), 1)
	})
}

// TestExitConditionMustCompileAndAnswerYesOrNo. A condition that does not
// parse, or that answers with something that is not a bool, is a configuration
// error. Caught here it is a message to the author; caught at evaluation it is
// a loop that fails at runtime for everyone.
func TestExitConditionMustCompileAndAnswerYesOrNo(t *testing.T) {
	store, _, _ := openMemory(t)
	tenant := uniqueTenant(t)

	for name, expr := range map[string]string{
		"does not parse":     "state.done &&&",
		"is not a bool":      `"nearly"`,
		"names nothing":      "",
		"unknown identifier": "finished",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := loop.New(loop.Node{
				Subgraph: subgraph("body"), MaxIterations: 2, ExitCondition: expr,
			}, loop.Options{Store: store, TenantID: tenant, Body: noBody})
			require.Error(t, err)
			require.ErrorIs(t, err, loop.ErrExitCondition)
		})
	}
}

// TestNestedLoopsShareOneTotalBudget. Nesting is where a per-loop ceiling
// stops being a bound: an outer loop of 10 around an inner loop of 10 is 100
// body iterations, and three levels is a thousand. The budget is therefore a
// SINGLE counter shared across the nest, seeded by the outermost loop — the
// worst case is the seed, never the product of the ceilings.
func TestNestedLoopsShareOneTotalBudget(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		store, _, _ := open(t)
		tenant := uniqueTenant(t)

		var inner atomic.Int64
		innerLoop, err := loop.New(loop.Node{
			Subgraph:      subgraph("inner-body"),
			MaxIterations: 100,
			ExitCondition: "false",
		}, loop.Options{
			Store:    store,
			TenantID: tenant,
			Body: func(context.Context, loop.Iteration) (map[string]any, error) {
				inner.Add(1)
				return nil, nil
			},
		})
		require.NoError(t, err)

		var outer atomic.Int64
		outerLoop, err := loop.New(loop.Node{
			Subgraph:      subgraph("outer-body"),
			MaxIterations: 4,
			ExitCondition: "false",
		}, loop.Options{
			Store:                store,
			TenantID:             tenant,
			TotalIterationBudget: 6,
			Body: func(ctx context.Context, it loop.Iteration) (map[string]any, error) {
				outer.Add(1)
				// The nested loop runs under the outer loop's context, which
				// is how it finds the budget it must share.
				_, err := innerLoop.Run(ctx, testRun, it.StepID+"/inner", nil)
				if err != nil && !errors.Is(err, loop.ErrIterationCeiling) {
					return nil, err
				}
				return nil, nil
			},
		})
		require.NoError(t, err)

		_, err = outerLoop.Run(ctx, testRun, "outer", nil)
		require.Error(t, err)
		require.ErrorIs(t, err, loop.ErrIterationCeiling)

		total := outer.Load() + inner.Load()
		require.Equal(t, int64(6), total,
			"the nest spent exactly its shared budget; without sharing this would be 4*100")
		require.Less(t, inner.Load(), int64(100),
			"the inner loop did not get its own full ceiling inside the nest")

		// The exhaustion is recorded in the same vocabulary as any other
		// ceiling, so a run view has one thing to look for.
		var reasons []string
		for _, e := range eventsOfType(ctx, t, store, tenant, loop.EventCeilingReached) {
			rec, err := loop.UnmarshalIteration(e.Payload)
			require.NoError(t, err)
			require.Contains(t, rec.Reason, "iteration ceiling reached")
			reasons = append(reasons, rec.Reason)
		}
		require.NotEmpty(t, reasons)
		require.True(t, strings.Contains(strings.Join(reasons, "\n"), "budget"),
			"the log says the nest ran out of budget, not that one loop ran out of iterations")
	})
}

// TestLoopIsTenantScoped. Every stored record carries a tenant. An empty one
// is a caller bug, never a wildcard.
func TestLoopIsTenantScoped(t *testing.T) {
	store, _, _ := openMemory(t)

	_, err := loop.New(loop.Node{
		Subgraph: subgraph("body"), MaxIterations: 2, ExitCondition: "true",
	}, loop.Options{Store: store, TenantID: "", Body: noBody})
	require.ErrorIs(t, err, runstore.ErrTenantRequired)
	require.Contains(t, err.Error(), "tenant scope required")
}

// --- helpers ------------------------------------------------------------

func noBody(context.Context, loop.Iteration) (map[string]any, error) { return nil, nil }

// subgraph is a minimal, valid loop body: two steps and an edge between them.
func subgraph(id string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:    id,
		Steps: []*dholev1.Step{{Id: "test"}, {Id: "fix"}},
		Edges: []*dholev1.Edge{{FromStep: "test", FromPort: "o", ToStep: "fix", ToPort: "i"}},
	}
}

func eventsOfType(
	ctx context.Context, t *testing.T, store runstore.Store, tenant string, kind runstore.EventType,
) []runstore.Event {
	t.Helper()
	all, err := store.Replay(ctx, tenant, testRun)
	require.NoError(t, err)
	var out []runstore.Event
	for _, e := range all {
		if e.Type == kind {
			out = append(out, e)
		}
	}
	return out
}

func onlyEvent(
	ctx context.Context, t *testing.T, store runstore.Store, tenant string, kind runstore.EventType,
) runstore.Event {
	t.Helper()
	found := eventsOfType(ctx, t, store, tenant, kind)
	require.Len(t, found, 1, "expected exactly one %s event", kind)
	return found[0]
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

var tenantSeq atomic.Int64

func uniqueTenant(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("loop-%d-%d", time.Now().UnixNano(), tenantSeq.Add(1))
}

// --- both dialects ------------------------------------------------------

type storeOpener func(t *testing.T) (runstore.Store, *sql.DB, runstore.Dialect)

func openMemory(t *testing.T) (runstore.Store, *sql.DB, runstore.Dialect) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "run.db")
	store, err := runstore.NewSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	db, err := runstore.OpenSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return store, db, runstore.DialectSQLite
}

// eachStore runs a case against BOTH dialects. A store proven only against
// SQLite is a store that has never met the deployment target.
func eachStore(t *testing.T, fn func(t *testing.T, open storeOpener)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		dir := t.TempDir()
		var seq atomic.Int64
		fn(t, func(t *testing.T) (runstore.Store, *sql.DB, runstore.Dialect) {
			t.Helper()
			path := filepath.Join(dir, fmt.Sprintf("run-%d.db", seq.Add(1)))
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
		fn(t, func(t *testing.T) (runstore.Store, *sql.DB, runstore.Dialect) {
			t.Helper()
			ctx := context.Background()
			store, err := runstore.NewPostgres(ctx, scopedDSN)
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })
			db, err := runstore.OpenPostgres(ctx, scopedDSN)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			return store, db, runstore.DialectPostgres
		})
	})
}

// Postgres is one shared database for the whole suite, so this binary gets its
// own schema: nothing here can be mistaken for another package's rows.
const testSchema = "dhole_loop_test"

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
