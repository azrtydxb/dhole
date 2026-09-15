package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/policy"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/taint"
)

// THE GAP THESE TESTS CLOSE (ADR 0031).
//
// ADR 0015's rules read `input.tainted`, `input.taint_sources` and
// `input.engine_capabilities`, and the dispatch path set none of them: every
// step reached policy clean and bound for no engine, so a rule refusing
// untrusted data on a privileged engine held at dispatch by never firing.
// These read the Input the scheduler actually hands the engine.

// inputRecorder is a policy engine that allows everything and keeps what it
// was asked, so a test can read the facts a decision was made on.
type inputRecorder struct {
	mu     sync.Mutex
	inputs []policy.Input
}

func (r *inputRecorder) Evaluate(_ context.Context, in policy.Input) (policy.Decision, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inputs = append(r.inputs, in)
	return policy.Decision{Allow: true, Reason: "recorded"}, nil
}

func (r *inputRecorder) about(t *testing.T, subject string) policy.Input {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, in := range r.inputs {
		if in.Subject == subject {
			return in
		}
	}
	t.Fatalf("no decision was asked about %q", subject)
	return policy.Input{}
}

func structuredPort(name string) *dholev1.Port {
	return &dholev1.Port{Name: name, Type: &dholev1.PortType{Kind: &dholev1.PortType_Structured{
		Structured: &dholev1.StructType{SchemaId: "dhole:test/any", Schema: `{}`},
	}}}
}

// seedRunWithInputs starts runID with the values a trigger bound, wrapped
// exactly as a trigger stores them.
func seedRunWithInputs(
	ctx context.Context, t *testing.T, h *policyHarness, runID string, inputs map[string]*structpb.Value,
) {
	t.Helper()
	payload, err := scheduler.MarshalRunCreated(scheduler.RunCreated{
		PipelineID: testPipeline, RevisionID: testRevision, Inputs: inputs, StartedBy: "git:forge",
	})
	require.NoError(t, err)
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID: runID, Type: runstore.RunCreated, Payload: payload, At: time.Now().UTC(),
	}))
}

// succeed records a step as finished with one output on port "out".
func succeed(ctx context.Context, t *testing.T, h *policyHarness, runID, stepID string) {
	t.Helper()
	status, err := proto.Marshal(&dholev1.JobStatus{
		RunId: runID, StepId: stepID, Attempt: 1,
		Outputs: []*dholev1.OutputRef{{Port: "out", Key: stepID + "/out"}},
	})
	require.NoError(t, err)
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID: runID, StepID: stepID, Attempt: 1, Type: runstore.StepSucceeded, Payload: status, At: time.Now().UTC(),
	}))
}

// TestTheDispatchDecisionCarriesTheTaintOfTheStepsInputs: a step bound to a
// value an untrusted trigger admitted is tainted by that trigger; a step fed by
// it through an edge is tainted by it too, transitively, together with any
// other source it reads; a step reading nothing untrusted is clean; and a
// sanitisation recorded on the step in between clears exactly what it says.
func TestTheDispatchDecisionCarriesTheTaintOfTheStepsInputs(t *testing.T) {
	step := func(id string, class dholev1.EffectClass, inputs ...*dholev1.Port) *dholev1.Step {
		return &dholev1.Step{
			Id: id, Name: id, PluginRef: "cmd://" + id, EffectClass: class,
			LeaseScope: dholev1.LeaseScope_LEASE_SCOPE_STEP,
			Inputs:     inputs, Outputs: []*dholev1.Port{{Name: "out"}},
		}
	}
	pure, effectful := dholev1.EffectClass_EFFECT_CLASS_PURE, dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT
	edge := func(from, to, port string) *dholev1.Edge {
		return &dholev1.Edge{FromStep: from, FromPort: "out", ToStep: to, ToPort: port}
	}
	// fetch <- event (git), note <- comment (http); build <- fetch;
	// deploy <- build + note; gate <- fetch, then publish <- gate; lint clean.
	pipeline := &dholev1.Pipeline{
		Id: testPipeline, Tenant: &dholev1.Tenant{Id: testTenant},
		Steps: []*dholev1.Step{
			step("fetch", pure, structuredPort("event")),
			step("note", pure, structuredPort("comment")),
			step("build", effectful, &dholev1.Port{Name: "src"}),
			step("deploy", effectful, &dholev1.Port{Name: "artifact"}, &dholev1.Port{Name: "notes"}),
			step("gate", pure, &dholev1.Port{Name: "raw"}),
			step("publish", effectful, &dholev1.Port{Name: "cleared"}),
			step("lint", pure, structuredPort("config")),
		},
		Edges: []*dholev1.Edge{
			edge("fetch", "build", "src"),
			edge("build", "deploy", "artifact"),
			edge("note", "deploy", "notes"),
			edge("fetch", "gate", "raw"),
			edge("gate", "publish", "cleared"),
		},
	}

	ctx := testContext(t)
	rec := &inputRecorder{}
	h := newPolicyHarness(ctx, t, policyOptions{pipeline: pipeline, engine: rec})
	const run = "run-tainted"
	seedRunWithInputs(ctx, t, h, run, map[string]*structpb.Value{
		"event":   taint.Mark(structpb.NewStringValue("refs/heads/main"), "git:forge"),
		"comment": taint.Mark(structpb.NewStringValue("lgtm"), "http:hook"),
		"config":  structpb.NewStringValue("strict"),
	})

	require.NoError(t, h.sched.Advance(ctx, testTenant, run))
	fetch := rec.about(t, "step:fetch")
	require.True(t, fetch.Tainted, "a step bound to a value a git trigger admitted reached policy clean")
	require.Equal(t, []string{"git:forge"}, fetch.TaintSources)
	lint := rec.about(t, "step:lint")
	require.False(t, lint.Tainted, "a step bound only to a clean value was called tainted")
	require.Empty(t, lint.TaintSources)

	succeed(ctx, t, h, run, "fetch")
	succeed(ctx, t, h, run, "note")
	payload, err := taint.MarshalRecord(taint.Record{
		Gate: "gate", Principal: "alice", Fields: []string{"raw"}, Sources: []string{"git:forge"},
	})
	require.NoError(t, err)
	require.NoError(t, h.store.Append(ctx, testTenant, runstore.Event{
		RunID: run, StepID: "gate", Type: taint.EventSanitised, Payload: payload, At: time.Now().UTC(),
	}))
	succeed(ctx, t, h, run, "gate")
	require.NoError(t, h.sched.Advance(ctx, testTenant, run))

	build := rec.about(t, "step:build")
	require.True(t, build.Tainted, "a step fed by a tainted step through an edge reached policy clean")
	require.Equal(t, []string{"git:forge"}, build.TaintSources)
	publish := rec.about(t, "step:publish")
	require.False(t, publish.Tainted,
		"a step fed only by a gate that recorded clearing the taint is still tainted: %v", publish.TaintSources)

	succeed(ctx, t, h, run, "build")
	require.NoError(t, h.sched.Advance(ctx, testTenant, run))
	deploy := rec.about(t, "step:deploy")
	require.True(t, deploy.Tainted)
	require.Equal(t, []string{"git:forge", "http:hook"}, deploy.TaintSources,
		"taint two hops upstream and from a second branch are not both carried, sorted")
}

// TestTheDispatchDecisionCarriesTheCapabilitiesOfTheEnginesAStepCanReach: a
// dispatch goes to a work queue any matching engine may take it from, so the
// engines a rule must judge are all of those; a step the plane hosts reaches
// none. The step's secrets are decided on the same facts.
func TestTheDispatchDecisionCarriesTheCapabilitiesOfTheEnginesAStepCanReach(t *testing.T) {
	privileged := readyEngine("vm-1")
	privileged.EngineTypes = []string{"vm"}
	privileged.Capabilities = []dholev1.Capability{
		dholev1.Capability_CAPABILITY_SECRETS, dholev1.Capability_CAPABILITY_PRIVILEGED,
	}
	plain := readyEngine("process-1")
	plain.EngineTypes = []string{"process"}
	plain.Capabilities = []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS}

	step := func(id, engineType, ref string) *dholev1.Step {
		return &dholev1.Step{
			Id: id, Name: id, PluginRef: ref, EngineType: engineType,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE, LeaseScope: dholev1.LeaseScope_LEASE_SCOPE_STEP,
			Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS},
		}
	}
	// The step declaring a secret is last: this scheduler issues none, so its
	// dispatch is refused after its decisions, which ends the pass.
	anywhere := step("c-anywhere", "", "cmd://b")
	anywhere.Secrets = []*dholev1.StepSecret{{Name: "harbor-robot", Env: "PASSWORD"}}
	pipeline := &dholev1.Pipeline{
		Id: testPipeline, Tenant: &dholev1.Tenant{Id: testTenant},
		Steps: []*dholev1.Step{
			step("a-on-process", "process", "cmd://a"),
			step("b-on-plane", "", "builtin:wait"),
			anywhere,
		},
	}

	ctx := testContext(t)
	rec := &inputRecorder{}
	h := newPolicyHarness(ctx, t, policyOptions{
		pipeline: pipeline, engine: rec, engines: []registry.Instance{privileged, plain},
	})
	require.NoError(t, h.sched.Advance(ctx, testTenant, testRun))

	secrets := []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS}
	require.Equal(t, secrets, rec.about(t, "step:a-on-process").EngineCapabilities,
		"a step that can reach only the process engine was judged against another engine's capabilities")
	both := []dholev1.Capability{dholev1.Capability_CAPABILITY_SECRETS, dholev1.Capability_CAPABILITY_PRIVILEGED}
	require.Equal(t, both, rec.about(t, "step:c-anywhere").EngineCapabilities,
		"a step any engine may take was not judged against every engine it can reach")
	require.Empty(t, rec.about(t, "step:b-on-plane").EngineCapabilities,
		"a step the plane hosts reaches no engine")

	require.Equal(t, both, rec.about(t, "secret:harbor-robot").EngineCapabilities,
		"a declared secret was not decided on the facts of its step")
}
