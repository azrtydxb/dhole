package taint_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/taint"
	"github.com/azrtydxb/dhole/internal/trigger"
)

// sqliteStore opens a real run store in a temporary file. Nothing here uses a
// fake log: the property under test is that a mark survives being written to a
// database and read back, and an in-memory stand-in would assert only that a
// pointer is still the pointer it was.
func sqliteStore(t *testing.T) runstore.Store {
	t.Helper()
	store, err := runstore.NewSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// TestTaintBlocksEffectfulStepUntilSanitised is ADR 0015's third sentence:
// tainted data reaching an effectful step is blocked unless an explicit
// sanitisation gate clears it.
//
// The value is marked by the TRIGGER package — the marker Task 41 already
// applies to every webhook payload — because a taint package that only reads
// its own marks reads every run already fired as clean.
func TestTaintBlocksEffectfulStepUntilSanitised(t *testing.T) {
	ctx := context.Background()
	ref := trigger.MarkTainted(structpb.NewStringValue("refs/heads/main"), "git:github:pushes")

	deploy := taint.Dispatch{
		Subject:     "step:deploy",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		Inputs:      map[string]*structpb.Value{"ref": ref},
	}

	denied := checkDispatch(t, deploy)
	require.False(t, denied.Allow, "an AT_MOST_ONCE step must not act on untrusted data")
	require.Contains(t, denied.Reason, "git:github:pushes",
		"the refusal must name the trigger that admitted the value")

	// A gate is a step somebody RUNS, on somebody's authority, naming what it
	// cleared — not a node whose presence in the graph is enough.
	gate := taint.Gate{StepID: "step:sanitise", Fields: []string{"ref"}}
	cleared, record, err := gate.Sanitise(ctx, sqliteStore(t), taint.Sanitisation{
		TenantID:  "tenant-a",
		RunID:     "run-1",
		Principal: "user:pascal",
		Fields:    []string{"ref"},
		Inputs:    deploy.Inputs,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"ref"}, record.Fields)

	deploy.Inputs = cleared
	allowed := checkDispatch(t, deploy)
	require.True(t, allowed.Allow, "sanitised data reaches the effectful step: %s", allowed.Reason)
	require.Equal(t, "refs/heads/main", cleared["ref"].GetStringValue(),
		"clearing a mark returns the value the mark carried, unchanged")
}

// TestTaintPropagatesThroughPureSteps is the middle sentence of ADR 0015: the
// mark travels through typed ports. A pure step is allowed to CONSUME
// untrusted data — that is what a parser or a validator is for — but what it
// produces is derived from it and is untrusted too.
func TestTaintPropagatesThroughPureSteps(t *testing.T) {
	in := []*dholev1.OutputRef{taint.MarkRef(&dholev1.OutputRef{Port: "payload"}, "git:github:pushes")}
	out := []*dholev1.OutputRef{{Port: "parsed"}, {Port: "summary"}}

	require.True(t, checkDispatch(t, taint.Dispatch{
		Subject:     "step:parse",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
		InputRefs:   in,
	}).Allow, "a pure step may consume tainted data")

	taint.Propagate(in, out)

	for _, o := range out {
		require.True(t, taint.RefTainted(o),
			"output %q of a step fed untrusted data is untrusted", o.GetPort())
		require.Contains(t, taint.RefSources(o), "git:github:pushes",
			"the propagated mark keeps the origin it came from")
	}
}

// TestTaintReachingPrivilegedEngineIsRefused is the other half of the third
// sentence: an engine advertising PRIVILEGED is a host-level foothold, so
// untrusted data must not be dispatched to one even when the step itself is
// pure. The denial is shaped like every other policy denial and its reason
// names the taint.
func TestTaintReachingPrivilegedEngineIsRefused(t *testing.T) {
	in := []*dholev1.OutputRef{taint.MarkRef(&dholev1.OutputRef{Port: "payload"}, "http:public:hooks")}

	decision := checkDispatch(t, taint.Dispatch{
		Subject:            "step:parse",
		EffectClass:        dholev1.EffectClass_EFFECT_CLASS_PURE,
		EngineCapabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED},
		InputRefs:          in,
	})

	require.False(t, decision.Allow)
	require.Contains(t, decision.Reason, "http:public:hooks",
		"the reason must name the taint, not merely say no")
	require.Contains(t, decision.Reason, "PRIVILEGED")
	require.NotEmpty(t, decision.Rule, "a denial names the rule that decided it")
}

// TestGateRecordsWhoSanitisedWhat holds the gate to the audit ADR 0012 asks
// of every decision: clearing a taint is somebody's act, and the log has to
// say whose and over what. The event goes through the REAL run store, because
// a record that exists only in memory answers nothing after a restart.
func TestGateRecordsWhoSanitisedWhat(t *testing.T) {
	ctx := context.Background()
	store := sqliteStore(t)

	gate := taint.Gate{StepID: "step:sanitise", Fields: []string{"ref", "author"}}
	inputs := map[string]*structpb.Value{
		"ref":    trigger.MarkTainted(structpb.NewStringValue("refs/heads/main"), "git:github:pushes"),
		"author": trigger.MarkTainted(structpb.NewStringValue("mallory"), "git:github:pushes"),
	}

	_, _, err := gate.Sanitise(ctx, store, taint.Sanitisation{
		TenantID:  "tenant-a",
		RunID:     "run-1",
		Principal: "user:pascal",
		Fields:    []string{"ref"},
		Inputs:    inputs,
	})
	require.NoError(t, err)

	events, err := store.Replay(ctx, "tenant-a", "run-1")
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, taint.EventSanitised, events[0].Type)

	record, err := taint.UnmarshalRecord(events[0].Payload)
	require.NoError(t, err)
	require.Equal(t, "user:pascal", record.Principal, "the log names who cleared it")
	require.Equal(t, "step:sanitise", record.Gate)
	require.Equal(t, []string{"ref"}, record.Fields, "the log names exactly what was cleared")
	require.Contains(t, record.Sources, "git:github:pushes",
		"the log names the taint that was cleared, so the clearance can be judged later")
}

// TestTaintSurvivesStoreAndReplay is the property the wrapper representation
// exists for. A trigger hands the scheduler `map[string]*structpb.Value` and
// nothing else, so a mark kept BESIDE the value is lost at the first store and
// replay — and a taint that can be lost is worse than none, because the system
// then reports as trusted data that never was.
//
// It runs against the real run store on both dialects, on values and on the
// output refs the cache round-trips, because that is where the loss would
// happen.
func TestTaintSurvivesStoreAndReplay(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		open func(t *testing.T) runstore.Store
	}{
		{"sqlite", sqliteStore},
		{"postgres", postgresStore},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := tc.open(t)
			tenant := uniqueTenant(t)

			marked := trigger.MarkTainted(structpb.NewStringValue("refs/heads/main"), "git:github:pushes")
			inputs, err := protojson.Marshal(&structpb.Struct{
				Fields: map[string]*structpb.Value{"ref": marked},
			})
			require.NoError(t, err)

			out := taint.MarkRef(&dholev1.OutputRef{Port: "parsed", Key: "k"}, "git:github:pushes")
			encoded, err := proto.Marshal(out)
			require.NoError(t, err)

			// Sequence 0: the log allocates its own position.
			require.NoError(t, store.Append(ctx, tenant, runstore.Event{
				RunID: "run-1", Type: runstore.RunCreated, Payload: inputs, At: time.Now().UTC(),
			}))
			require.NoError(t, store.Append(ctx, tenant, runstore.Event{
				RunID: "run-1", StepID: "parse", Type: runstore.StepSucceeded,
				Payload: encoded, At: time.Now().UTC(),
			}))

			events, err := store.Replay(ctx, tenant, "run-1")
			require.NoError(t, err)
			require.Len(t, events, 2)

			replayedInputs := &structpb.Struct{}
			require.NoError(t, protojson.Unmarshal(events[0].Payload, replayedInputs))
			replayed := replayedInputs.GetFields()["ref"]
			require.True(t, taint.IsTainted(replayed),
				"a value stored and replayed must still read as untrusted")
			require.Equal(t, "git:github:pushes", taint.Source(replayed),
				"and must still name where it came from")
			require.Equal(t, "refs/heads/main", taint.Value(replayed).GetStringValue())

			replayedRef := &dholev1.OutputRef{}
			require.NoError(t, proto.Unmarshal(events[1].Payload, replayedRef))
			require.True(t, taint.RefTainted(replayedRef),
				"an output ref stored and replayed must still read as untrusted")
			require.Equal(t, []string{"git:github:pushes"}, taint.RefSources(replayedRef))
		})
	}
}

// TestPropagateIsConservative: an output derived from ANY tainted input is
// tainted. The mixed case is the one that matters — a step reading one trusted
// file and one webhook body produces untrusted output, and a propagation that
// asked for agreement between its inputs would call it clean.
func TestPropagateIsConservative(t *testing.T) {
	in := []*dholev1.OutputRef{
		{Port: "config"}, // clean: produced by a trusted step
		taint.MarkRef(&dholev1.OutputRef{Port: "payload"}, "http:public:hooks"),
	}
	out := []*dholev1.OutputRef{{Port: "plan"}}

	taint.Propagate(in, out)

	require.True(t, taint.RefTainted(out[0]),
		"one untrusted input is enough to make the output untrusted")
	require.Contains(t, taint.RefSources(out[0]), "http:public:hooks")

	clean := []*dholev1.OutputRef{{Port: "plan"}}
	taint.Propagate([]*dholev1.OutputRef{{Port: "config"}}, clean)
	require.False(t, taint.RefTainted(clean[0]),
		"propagation invents no taint where no input carried one")
}

// TestNestedTaintIsDetected: a payload is a JSON body, so the untrusted part
// is almost never the top-level value. A check that looks only at the top
// level reads `{"commits": [<tainted>]}` as clean.
func TestNestedTaintIsDetected(t *testing.T) {
	inner := trigger.MarkTainted(structpb.NewStringValue("mallory"), "git:github:pushes")

	list := structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{
		structpb.NewStringValue("clean"), inner,
	}})
	require.True(t, taint.IsTainted(list), "a tainted value inside a list is tainted")

	nested := structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
		"head": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
			"author": inner,
			"sha":    structpb.NewStringValue("deadbeef"),
		}}),
		"forced": structpb.NewBoolValue(false),
	}})
	require.True(t, taint.IsTainted(nested), "a tainted value inside a struct is tainted")
	require.Equal(t, []string{"git:github:pushes"}, taint.Sources(nested))

	require.False(t, taint.IsTainted(structpb.NewStructValue(&structpb.Struct{
		Fields: map[string]*structpb.Value{"sha": structpb.NewStringValue("deadbeef")},
	})), "a payload carrying no mark anywhere is clean")

	// And the buried case must block an effectful step just as the plain one does.
	decision := checkDispatch(t, taint.Dispatch{
		Subject:     "step:deploy",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		Inputs:      map[string]*structpb.Value{"push": nested},
	})
	require.False(t, decision.Allow)
	require.Contains(t, decision.Reason, "git:github:pushes")
}

// TestDoubleMarkingDoesNotNest fixes the behaviour deliberately: a second mark
// on an already-marked value is a NO-OP that keeps the FIRST source. The first
// boundary is the one nearest the attacker and the one an incident asks
// about; re-marking cannot make untrusted data more untrusted, and nesting
// wrappers would turn a payload into an onion nothing downstream can read.
func TestDoubleMarkingDoesNotNest(t *testing.T) {
	once := taint.Mark(structpb.NewStringValue("refs/heads/main"), "git:github:pushes")
	twice := taint.Mark(once, "http:public:hooks")

	require.True(t, proto.Equal(once, twice), "marking a marked value changes nothing")
	require.Equal(t, "git:github:pushes", taint.Source(twice), "the first source survives")
	require.Equal(t, "refs/heads/main", taint.Value(twice).GetStringValue(),
		"the value is one unwrap away, not two")
	require.Equal(t, []string{"git:github:pushes"}, taint.Sources(twice))
}

// TestGateClearsNothingByExisting: sanitisation is an ACT, not a location. A
// gate configured over a field has cleared nothing until somebody runs it,
// naming themselves and naming the field.
func TestGateClearsNothingByExisting(t *testing.T) {
	ctx := context.Background()
	store := sqliteStore(t)
	ref := trigger.MarkTainted(structpb.NewStringValue("refs/heads/main"), "git:github:pushes")
	inputs := map[string]*structpb.Value{"ref": ref}

	// Merely constructing the gate — the graph containing the node — clears
	// nothing.
	gate := taint.Gate{StepID: "step:sanitise", Fields: []string{"ref"}}
	require.True(t, taint.IsTainted(inputs["ref"]))
	require.False(t, checkDispatch(t, taint.Dispatch{
		Subject:     "step:deploy",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		Inputs:      inputs,
	}).Allow, "a gate in the graph is not a clearance")

	for _, tc := range []struct {
		name string
		s    taint.Sanitisation
	}{
		{"no principal", taint.Sanitisation{
			TenantID: "t", RunID: "r", Fields: []string{"ref"}, Inputs: inputs,
		}},
		{"no fields named", taint.Sanitisation{
			TenantID: "t", RunID: "r", Principal: "user:pascal", Inputs: inputs,
		}},
		{"a field the gate does not declare", taint.Sanitisation{
			TenantID: "t", RunID: "r", Principal: "user:pascal",
			Fields: []string{"author"}, Inputs: inputs,
		}},
		{"no tenant", taint.Sanitisation{
			RunID: "r", Principal: "user:pascal", Fields: []string{"ref"}, Inputs: inputs,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := gate.Sanitise(ctx, store, tc.s)
			require.Error(t, err)
			require.True(t, taint.IsTainted(inputs["ref"]),
				"a refused sanitisation leaves the mark where it was")
		})
	}

	// The mark is cleared on a COPY: nothing about running a gate mutates the
	// values a parallel branch is still reading.
	cleared, _, err := gate.Sanitise(ctx, store, taint.Sanitisation{
		TenantID: "t", RunID: "r", Principal: "user:pascal",
		Fields: []string{"ref"}, Inputs: inputs,
	})
	require.NoError(t, err)
	require.False(t, taint.IsTainted(cleared["ref"]))
	require.True(t, taint.IsTainted(inputs["ref"]),
		"the gate clears its own result, not the log's record of what arrived")
}

// TestStepCannotLaunderItsOwnOutputs: if a step could hand back outputs it
// declared clean, every step would be a sanitisation gate and the model would
// be decorative. Propagation is applied by the control plane to whatever the
// engine reported, and it only ever ADDS.
func TestStepCannotLaunderItsOwnOutputs(t *testing.T) {
	in := []*dholev1.OutputRef{taint.MarkRef(&dholev1.OutputRef{Port: "payload"}, "git:github:pushes")}

	// The engine reports outputs carrying no mark at all — the honest shape of
	// a step trying to launder its inputs.
	out := []*dholev1.OutputRef{{Port: "plan"}}
	taint.Propagate(in, out)
	require.True(t, taint.RefTainted(out[0]),
		"an engine's silence about taint is not a clearance")

	require.False(t, checkDispatch(t, taint.Dispatch{
		Subject:     "step:deploy",
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
		InputRefs:   out,
	}).Allow)

	// And propagation never removes a mark an input already carried, whatever
	// the step upstream of it was.
	stillTainted := []*dholev1.OutputRef{
		taint.MarkRef(&dholev1.OutputRef{Port: "plan"}, "http:public:hooks"),
	}
	taint.Propagate([]*dholev1.OutputRef{{Port: "config"}}, stillTainted)
	require.True(t, taint.RefTainted(stillTainted[0]),
		"clean inputs do not wash an output that was already untrusted")
}

// TestWireShapeIsTask41sWrapper pins the representation to the literal shape
// already in every fired run's inputs: `{"$dhole.taint": {"source", "value"}}`.
// A taint package that reads a different shape reports every payload Task 41
// admitted as clean, which is the exact failure taint tracking exists to
// prevent — so this is asserted against the JSON, not against the package's
// own reader.
func TestWireShapeIsTask41sWrapper(t *testing.T) {
	marked := taint.Mark(structpb.NewStringValue("refs/heads/main"), "git:github:pushes")

	raw, err := protojson.Marshal(marked)
	require.NoError(t, err)
	require.JSONEq(t,
		`{"$dhole.taint":{"source":"git:github:pushes","value":"refs/heads/main"}}`,
		string(raw))

	// Read the other way too: what the trigger package writes, this package
	// reads, and vice versa.
	require.True(t, taint.IsTainted(
		trigger.MarkTainted(structpb.NewStringValue("x"), "http:public:hooks")))
	require.True(t, trigger.IsTainted(marked))
	require.Equal(t, taint.Field, trigger.TaintField)
}

// postgresStore opens the live Postgres run store, or skips with a reason: an
// integration test that silently degrades to nothing reports green while
// testing nothing.
func postgresStore(t *testing.T) runstore.Store {
	t.Helper()
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set: this test needs a live Postgres")
	}
	store, err := runstore.NewPostgres(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

// uniqueTenant keeps parallel runs against one shared Postgres from reading
// each other's events.
func uniqueTenant(t *testing.T) string {
	t.Helper()
	var b [8]byte
	_, err := rand.Read(b[:])
	require.NoError(t, err)
	return "tenant-" + hex.EncodeToString(b[:])
}
