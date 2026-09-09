package dynamic_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/dag"
	"github.com/azrtydxb/dhole/internal/dynamic"
	"github.com/azrtydxb/dhole/internal/runstore"
)

const (
	testRun       = "run-1"
	generatorStep = "fan-out"
)

// TestGeneratorEmitsSubgraphSplicedIntoRun is the whole point of the step
// type: a generator decides at RUNTIME what work there is, and the work it
// decided on has to actually run.
//
// The graph is executed by a driver that knows nothing about this test — it
// walks whatever levels dag.Build derives and runs every step it finds — so
// "the three steps ran" is a statement about the spliced graph and not about
// a list the test wrote out by hand.
func TestGeneratorEmitsSubgraphSplicedIntoRun(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		store := open(t)
		tenant := uniqueTenant(t)

		parent := authored()
		// The fragment a generator emitted: three shards nobody authored.
		fragment := shards("shard-a", "shard-b", "shard-c")

		spliced, err := dynamic.Splice(parent, generatorStep, fragment)
		require.NoError(t, err)

		order := runPipeline(ctx, t, store, tenant, testRun, spliced)

		require.Equal(t, []string{"seed", generatorStep}, order[:2],
			"the fragment runs after the generator that emitted it, never beside it")
		require.ElementsMatch(t, []string{"shard-a", "shard-b", "shard-c"}, order[2:])

		completed := eventsOfType(ctx, t, store, tenant, runstore.RunCompleted)
		require.Len(t, completed, 1, "the run completed")
	})
}

// TestSpliceRejectsDuplicateStepID. A generator invents step ids; a person
// authored theirs. When the two collide, the authored step must not quietly
// become the generated one — that is somebody's build step replaced by
// something they never wrote, with nothing on screen to say so.
func TestSpliceRejectsDuplicateStepID(t *testing.T) {
	parent := authored()
	// "seed" is authored. The generator emits a step of the same name.
	fragment := shards("shard-a", "seed")

	spliced, err := dynamic.Splice(parent, generatorStep, fragment)
	require.Error(t, err)
	require.ErrorIs(t, err, dynamic.ErrDuplicateStepID)
	require.Nil(t, spliced, "a refused splice returns no graph to run by accident")
	// The message NAMES the collision. "duplicate step id" alone leaves an
	// operator diffing a generated fragment against their own pipeline.
	require.Contains(t, err.Error(), `"seed"`)

	// And the authored step is untouched: the parent handed in is not
	// rewritten, so nothing downstream can be holding a half-spliced graph.
	require.Len(t, parent.GetSteps(), 2)
	require.Empty(t, parent.GetSteps()[0].GetPluginRef(),
		"the authored step is still the authored step")
}

// TestSplicedGraphIsStillAcyclic. The acyclic graph is what the scheduler's
// topological order and the cache's key derivation are both built on (ADR
// 0001). A generator is the one place a cycle can arrive after the authored
// graph was checked, so the check happens again AT SPLICE TIME — and it is
// dag.Build's check, not a second implementation that can disagree with it.
func TestSplicedGraphIsStillAcyclic(t *testing.T) {
	fragment := shards("build", "test")
	fragment.Edges = []*dholev1.Edge{
		{FromStep: "build", FromPort: "shard", ToStep: "test", ToPort: "shard"},
		{FromStep: "test", FromPort: "shard", ToStep: "build", ToPort: "shard"},
	}
	// Both steps produce as well as consume, so the edges above are wireable
	// and the ONLY thing wrong with this fragment is that it goes round.
	for _, s := range fragment.GetSteps() {
		s.Outputs = []*dholev1.Port{blobPort("shard")}
	}

	spliced, err := dynamic.Splice(authored(), generatorStep, fragment)
	require.Error(t, err)
	require.ErrorIs(t, err, dynamic.ErrNotADAG)
	require.Nil(t, spliced)
	// dag.Build names the path that closes the cycle; carrying its words
	// through is the difference between a fixable message and "invalid".
	require.Contains(t, err.Error(), "cycle")
	require.Contains(t, err.Error(), "build")
	require.Contains(t, err.Error(), "test")
}

// TestReplayUsesTheRecordedFragmentAndDoesNotAskAgain is ADR 0003 applied to
// the one step type that can invent work.
//
// A run is replayed from its event log. If the fragment is not IN that log, a
// replay re-runs the generator — and a generator that lists files, or asks an
// API for a matrix, answers differently the second time. The replayed run
// would then be a different run from the one that happened, which is the one
// thing an event-sourced run may never be.
//
// The generator here therefore answers DIFFERENTLY on every call, and the
// second reader is a FRESH Generator over the same store: a control plane
// that restarted, holding nothing in memory. A deterministic fake could not
// tell "read the log" from "asked again and got lucky".
func TestReplayUsesTheRecordedFragmentAndDoesNotAskAgain(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		store := open(t)
		tenant := uniqueTenant(t)

		var firstCalls atomic.Int64
		first, err := dynamic.New(dynamic.Options{
			Store: store, TenantID: tenant, MaxExpansions: 8,
			Emit: func(context.Context, dynamic.Input) (*dholev1.Pipeline, error) {
				return shards(fmt.Sprintf("shard-%d", firstCalls.Add(1))), nil
			},
		})
		require.NoError(t, err)

		realised, err := first.Realise(ctx, testRun, generatorStep, authored())
		require.NoError(t, err)
		require.Equal(t, int64(1), firstCalls.Load())
		require.Contains(t, stepIDs(realised), "shard-1")

		// A different process, a different generator, the same log.
		var secondCalls atomic.Int64
		second, err := dynamic.New(dynamic.Options{
			Store: store, TenantID: tenant, MaxExpansions: 8,
			Emit: func(context.Context, dynamic.Input) (*dholev1.Pipeline, error) {
				return shards(fmt.Sprintf("something-else-%d", secondCalls.Add(1))), nil
			},
		})
		require.NoError(t, err)

		replayed, err := second.Realise(ctx, testRun, generatorStep, authored())
		require.NoError(t, err)
		require.Equal(t, int64(0), secondCalls.Load(),
			"the replay read the fragment out of the log; it did not ask the generator again")
		require.Equal(t, stepIDs(realised), stepIDs(replayed),
			"the replayed run is the run that happened, step for step")

		// One realisation, recorded once, carrying the ids a run view can draw.
		recorded := eventsOfType(ctx, t, store, tenant, dynamic.EventFragmentRealised)
		require.Len(t, recorded, 1)
		require.Equal(t, generatorStep, recorded[0].StepID)
		rec, err := dynamic.UnmarshalRecord(recorded[0].Payload)
		require.NoError(t, err)
		require.Equal(t, []string{"shard-1"}, rec.Steps)
		// And the exact fragment, not a summary of it: the log is what the graph
		// is rebuilt from, so it has to hold a pipeline rather than a list.
		fromLog, err := rec.Fragment()
		require.NoError(t, err)
		require.Equal(t, []string{"shard-1"}, stepIDs(fromLog))
	})
}

// TestSpliceTypeChecksWhereTheFragmentMeetsTheGenerator. Building is not
// enough. A fragment whose entry consumes `shard` as structured data, spliced
// below a generator that produces a blob, is a graph that builds perfectly
// and fails at minute eight of a run with a step that cannot read its input.
// It is refused here, with the editor's own diagnostics.
func TestSpliceTypeChecksWhereTheFragmentMeetsTheGenerator(t *testing.T) {
	fragment := &dholev1.Pipeline{Id: "fragment", Steps: []*dholev1.Step{
		// Same port NAME as the generator's output, different type.
		{Id: "shard-a", Inputs: []*dholev1.Port{structuredPort("shard", shardSchema)}},
	}}

	spliced, err := dynamic.Splice(authored(), generatorStep, fragment)
	require.Error(t, err)
	require.Nil(t, spliced)
	require.ErrorIs(t, err, dynamic.ErrPortMismatch)

	var rejected *dynamic.Rejection
	require.ErrorAs(t, err, &rejected)
	require.Len(t, rejected.Diagnostics, 1)
	// Addressed to a port, so an editor can put a marker on it.
	require.Equal(t, "shard-a", rejected.Diagnostics[0].StepID)
	require.Equal(t, "shard", rejected.Diagnostics[0].PortName)
	require.Contains(t, rejected.Diagnostics[0].Message,
		"cannot connect fan-out.shard to shard-a.shard")
	require.Contains(t, rejected.Diagnostics[0].Message, "blob is not structured")
}

// TestSpliceRejectsAFragmentConsumingAPortTheGeneratorDoesNotProduce. The
// other half of "the ports do not match where it is spliced": a name the
// generator never declared. Left unchecked, the entry step would simply have
// no incoming edge, become a root of the graph, and be dispatched with an
// input nothing ever fills.
func TestSpliceRejectsAFragmentConsumingAPortTheGeneratorDoesNotProduce(t *testing.T) {
	fragment := &dholev1.Pipeline{Id: "fragment", Steps: []*dholev1.Step{
		{Id: "shard-a", Inputs: []*dholev1.Port{blobPort("not-a-port")}},
	}}

	_, err := dynamic.Splice(authored(), generatorStep, fragment)
	require.ErrorIs(t, err, dynamic.ErrPortMismatch)

	var rejected *dynamic.Rejection
	require.ErrorAs(t, err, &rejected)
	require.Len(t, rejected.Diagnostics, 1)
	require.Equal(t, "shard-a", rejected.Diagnostics[0].StepID)
	require.Equal(t, "not-a-port", rejected.Diagnostics[0].PortName)
	require.Contains(t, rejected.Diagnostics[0].Message, "not-a-port")
	require.Contains(t, rejected.Diagnostics[0].Message, generatorStep)
}

// TestSpliceRejectsAnUnknownAnchor. A fragment has to be spliced somewhere,
// and "somewhere" is a step that exists. Splicing at a step nobody defined
// would attach the fragment to nothing and leave it a set of roots the
// scheduler dispatches immediately.
func TestSpliceRejectsAnUnknownAnchor(t *testing.T) {
	_, err := dynamic.Splice(authored(), "no-such-step", shards("shard-a"))
	require.ErrorIs(t, err, dynamic.ErrNoSuchStep)
	require.Contains(t, err.Error(), `"no-such-step"`)

	_, err = dynamic.Splice(authored(), "", shards("shard-a"))
	require.ErrorIs(t, err, dynamic.ErrNoSuchStep)
}

// TestSpliceRefusesAnEmptyFragmentOutLoud. A generator that emitted nothing
// may be perfectly correct — a matrix with no rows — but it must SAY so.
// Splicing nothing and returning the parent unchanged makes an empty matrix
// indistinguishable from a generator that crashed before it spoke.
func TestSpliceRefusesAnEmptyFragmentOutLoud(t *testing.T) {
	_, err := dynamic.Splice(authored(), generatorStep, &dholev1.Pipeline{Id: "fragment"})
	require.ErrorIs(t, err, dynamic.ErrEmptyFragment)
	require.Contains(t, err.Error(), "no steps")
	require.Contains(t, err.Error(), generatorStep)

	_, err = dynamic.Splice(authored(), generatorStep, nil)
	require.ErrorIs(t, err, dynamic.ErrEmptyFragment)
}

// TestGeneratorIsTenantScoped. Every stored record carries a tenant. An empty
// one is a caller bug, never a wildcard.
func TestGeneratorIsTenantScoped(t *testing.T) {
	store := openStore(t)

	_, err := dynamic.New(dynamic.Options{
		Store: store, TenantID: "", MaxExpansions: 2, Emit: noEmit,
	})
	require.ErrorIs(t, err, runstore.ErrTenantRequired)
	require.Contains(t, err.Error(), "tenant scope required")
}

// TestGeneratorRefusesAnUnboundedExpansionCeiling. Zero is not "no limit
// configured yet" and a negative number is not a limit; both are the
// unbounded expansion this ceiling exists to prevent, refused where the
// mistake was made.
func TestGeneratorRefusesAnUnboundedExpansionCeiling(t *testing.T) {
	store := openStore(t)

	for _, n := range []int{0, -1} {
		_, err := dynamic.New(dynamic.Options{
			Store: store, TenantID: "t", MaxExpansions: n, Emit: noEmit,
		})
		require.ErrorIs(t, err, dynamic.ErrUnbounded, "MaxExpansions %d", n)
	}
}

// TestNestedGeneratorsShareTheRunsExpansionBudget is this package's answer to
// a generator inside a generator's fragment: it is BOUNDED, like Task 50's
// loops, and the bound is one budget for the whole run rather than one per
// generator. A per-generator ceiling stops being a bound the moment
// generators nest — ten emitting ten is a hundred, three deep is a thousand —
// so the counter is the run's own log, which no nested generator can lift and
// which survives a restart because it IS the log.
func TestNestedGeneratorsShareTheRunsExpansionBudget(t *testing.T) {
	eachStore(t, func(t *testing.T, open storeOpener) {
		ctx := testContext(t)
		store := open(t)
		tenant := uniqueTenant(t)

		var calls atomic.Int64
		// Every fragment contains a generator, so this nests forever unless
		// something stops it. Something does.
		g, err := dynamic.New(dynamic.Options{
			Store: store, TenantID: tenant, MaxExpansions: 2,
			Emit: func(_ context.Context, in dynamic.Input) (*dholev1.Pipeline, error) {
				calls.Add(1)
				return &dholev1.Pipeline{Id: "fragment", Steps: []*dholev1.Step{{
					Id:        in.StepID + ".inner",
					PluginRef: dynamic.PluginRef,
					Inputs:    []*dholev1.Port{blobPort("shard")},
					Outputs:   []*dholev1.Port{blobPort("shard")},
				}}}, nil
			},
		})
		require.NoError(t, err)

		// The nest, driven the way a scheduler would drive it: realise, find the
		// generator the fragment contains, realise that one too.
		graph := authored()
		at := generatorStep
		var lastErr error
		for depth := 0; depth < 5; depth++ {
			next, err := g.Realise(ctx, testRun, at, graph)
			if err != nil {
				lastErr = err
				break
			}
			graph = next
			at += ".inner"
		}

		require.ErrorIs(t, lastErr, dynamic.ErrExpansionCeiling)
		require.Equal(t, int64(2), calls.Load(),
			"the nest spent the run's budget, not two generators' worth each")

		ceiling := eventsOfType(ctx, t, store, tenant, dynamic.EventCeilingReached)
		require.Len(t, ceiling, 1)
		rec, err := dynamic.UnmarshalRecord(ceiling[0].Payload)
		require.NoError(t, err)
		require.Contains(t, rec.Reason, "expansion ceiling reached")
		require.Contains(t, rec.Reason, "2")

		// The ceiling does NOT break replay. A fragment already in the log is
		// still handed back, budget or no budget: refusing it would make a
		// restart of this run diverge from the run that happened.
		replayed, err := g.Realise(ctx, testRun, generatorStep, authored())
		require.NoError(t, err)
		require.Contains(t, stepIDs(replayed), generatorStep+".inner")
		require.Equal(t, int64(2), calls.Load())
	})
}

// TestTheEditorsFixtureIsTheRecordThisPackageWrites keeps the browser and the
// control plane speaking one language.
//
// web/e2e/testdata/generator-record.json is a GENERATOR_FRAGMENT_REALISED
// payload, byte for byte as this package writes it, and the Playwright suite
// renders the run view from it. A fixture the browser reads and nothing
// checks is a fixture that quietly stops matching the day a json tag changes
// — and the run view would then be drawing a shape the plane no longer emits.
// So this reproduces the payload from the real recording path and requires
// the checked-in file to decode to the same record.
//
// Regenerate with DHOLE_UPDATE_FIXTURE=1 go test ./internal/dynamic.
func TestTheEditorsFixtureIsTheRecordThisPackageWrites(t *testing.T) {
	ctx := testContext(t)
	store := openStore(t)
	tenant := uniqueTenant(t)

	g, err := dynamic.New(dynamic.Options{
		Store: store, TenantID: tenant, MaxExpansions: 4,
		Emit: func(context.Context, dynamic.Input) (*dholev1.Pipeline, error) {
			return shards("shard-a", "shard-b", "shard-c"), nil
		},
	})
	require.NoError(t, err)
	_, err = g.Realise(ctx, testRun, generatorStep, authored())
	require.NoError(t, err)
	written := onlyEvent(ctx, t, store, tenant, dynamic.EventFragmentRealised).Payload

	if os.Getenv("DHOLE_UPDATE_FIXTURE") != "" {
		require.NoError(t, os.MkdirAll(filepath.Dir(editorFixture), 0o755))
		require.NoError(t, os.WriteFile(editorFixture, append(written, '\n'), 0o644))
	}

	onDisk, err := os.ReadFile(editorFixture)
	require.NoError(t, err, "the editor's fixture is missing")

	fromDisk, err := dynamic.UnmarshalRecord(onDisk)
	require.NoError(t, err)
	fromCode, err := dynamic.UnmarshalRecord(written)
	require.NoError(t, err)
	require.Equal(t, fromCode.Generator, fromDisk.Generator)
	require.Equal(t, fromCode.Steps, fromDisk.Steps)

	// And it still decodes to a pipeline, which is what makes it a record
	// rather than a list of names the browser could have invented.
	fragment, err := fromDisk.Fragment()
	require.NoError(t, err)
	require.Equal(t, fromCode.Steps, stepIDs(fragment))
}

// --- fixtures -----------------------------------------------------------

const shardSchema = "https://dhole.dev/schemas/shard.json"

// editorFixture is the run-log payload the Playwright suite renders.
const editorFixture = "../../web/e2e/testdata/generator-record.json"

func noEmit(context.Context, dynamic.Input) (*dholev1.Pipeline, error) { return nil, nil }

// stepIDs is the graph as a list of ids, which is what every assertion about
// "what ran" is really about.
func stepIDs(p *dholev1.Pipeline) []string {
	out := make([]string, 0, len(p.GetSteps()))
	for _, s := range p.GetSteps() {
		out = append(out, s.GetId())
	}
	return out
}

func blobPort(name string) *dholev1.Port {
	return &dholev1.Port{Name: name, Type: &dholev1.PortType{
		Kind: &dholev1.PortType_Blob{Blob: &dholev1.BlobType{}},
	}}
}

func structuredPort(name, schema string) *dholev1.Port {
	return &dholev1.Port{Name: name, Type: &dholev1.PortType{
		Kind: &dholev1.PortType_Structured{
			Structured: &dholev1.StructType{SchemaId: schema},
		},
	}}
}

// authored is the pipeline a person wrote: a source, and a generator that
// declares one output. Nothing here says what the generator will emit —
// that is the whole reason the node is opaque until it runs.
func authored() *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id: "matrix",
		Steps: []*dholev1.Step{
			{Id: "seed", Outputs: []*dholev1.Port{structuredPort("items", shardSchema)}},
			{
				Id:        generatorStep,
				PluginRef: dynamic.PluginRef,
				Inputs:    []*dholev1.Port{structuredPort("items", shardSchema)},
				Outputs:   []*dholev1.Port{blobPort("shard")},
			},
		},
		Edges: []*dholev1.Edge{
			{FromStep: "seed", FromPort: "items", ToStep: generatorStep, ToPort: "items"},
		},
	}
}

// shards is a fragment of independent steps, each consuming the generator's
// declared output port by name.
func shards(ids ...string) *dholev1.Pipeline {
	p := &dholev1.Pipeline{Id: "fragment"}
	for _, id := range ids {
		p.Steps = append(p.Steps, &dholev1.Step{
			Id:     id,
			Inputs: []*dholev1.Port{blobPort("shard")},
		})
	}
	return p
}

// --- a driver, not a scheduler ------------------------------------------

// runPipeline executes every step of a pipeline in dependency order and
// records the run in the log, returning the ids in the order they ran.
//
// It is deliberately generic: it is handed a graph and runs what is in it. A
// driver that knew the three shard ids could not tell a spliced graph from a
// hard-coded expectation.
func runPipeline(
	ctx context.Context, t *testing.T,
	store runstore.Store, tenant, runID string, p *dholev1.Pipeline,
) []string {
	t.Helper()
	g, err := dag.Build(p)
	require.NoError(t, err)

	require.NoError(t, store.Append(ctx, tenant, runstore.Event{
		RunID: runID, Type: runstore.RunCreated, At: time.Now().UTC(),
	}))

	var order []string
	for _, level := range g.TopoLevels() {
		for _, id := range level {
			require.NoError(t, store.Append(ctx, tenant, runstore.Event{
				RunID: runID, StepID: id, Type: runstore.StepSucceeded, At: time.Now().UTC(),
			}))
			order = append(order, id)
		}
	}
	require.NoError(t, store.Append(ctx, tenant, runstore.Event{
		RunID: runID, Type: runstore.RunCompleted, At: time.Now().UTC(),
	}))
	return order
}

// --- helpers ------------------------------------------------------------

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
	return fmt.Sprintf("dyn-%d-%d", time.Now().UnixNano(), tenantSeq.Add(1))
}

// --- both dialects ------------------------------------------------------

// openStore opens a SQLite store, for the cases that need one but are not
// about persistence.
func openStore(t *testing.T) runstore.Store {
	t.Helper()
	return openSQLite(t, filepath.Join(t.TempDir(), "run.db"))
}

func openSQLite(t *testing.T, path string) runstore.Store {
	t.Helper()
	store, err := runstore.NewSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

type storeOpener func(t *testing.T) runstore.Store

// eachStore runs a case against BOTH dialects. A store proven only against
// SQLite is a store that has never met the deployment target.
func eachStore(t *testing.T, fn func(t *testing.T, open storeOpener)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		dir := t.TempDir()
		var seq atomic.Int64
		fn(t, func(t *testing.T) runstore.Store {
			t.Helper()
			return openSQLite(t, filepath.Join(dir, fmt.Sprintf("run-%d.db", seq.Add(1))))
		})
	})
	t.Run("postgres", func(t *testing.T) {
		if scopedDSN == "" {
			t.Skip("DHOLE_TEST_POSTGRES_DSN not set")
		}
		fn(t, func(t *testing.T) runstore.Store {
			t.Helper()
			store, err := runstore.NewPostgres(context.Background(), scopedDSN)
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })
			return store
		})
	})
}

const testSchema = "dhole_dynamic_test"

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
