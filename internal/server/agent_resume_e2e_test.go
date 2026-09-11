// A parked agent step, resumed.
//
// An agent asks for an at-most-once action, the gate stops it, a person
// decides, and the model's loop is re-entered AT THE CALL IT STOPPED ON.
// Until this file existed the last clause was missing: the step failed with
// the gate's own reason, which stopped the run readably and was a holding
// position rather than a design.
//
// The tests here go through the shipping plane — `server.New`/`Start`, the
// served contract, `DecideApproval` over a real Connect client — because a
// resume driven by a test harness of its own would prove that the agent
// package can resume and say nothing about whether anything resumes it.
package server_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/dhole/internal/steps/agent"
	"github.com/azrtydxb/go-ai-sdk/provider"
)

// TestAParkedAgentStepResumesAtTheCallItStoppedOnAfterAPersonApprovesItsGate.
//
// The model reads a run, then asks to start one. Starting a run is
// AT_MOST_ONCE, so it parks at the same gate a person's approval queue reads.
// A person approves, and the step must finish the work it was in the middle
// of rather than fail with the gate's reason.
//
// The replay-safety half is the sharper one: `read_run` HAPPENED before the
// step parked, and a resume that re-ran the model's loop from the prompt
// would perform it a second time. The run log's own AGENT_ACTION records are
// the count, so the assertion is over what the plane recorded rather than
// over a spy the test wired in.
func TestAParkedAgentStepResumesAtTheCallItStoppedOnAfterAPersonApprovesItsGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	model := &conversationModel{}
	dir := t.TempDir()
	srv := startEmbeddedIn(ctx, t, dir, func(cfg *server.Config) {
		cfg.Models = model.factory()
	})

	// Something for the agent to start: a real pipeline of this tenant, on an
	// active revision, reached through the contract like any other.
	target := unmatchablePipeline("agent-target")
	approveThroughTheContract(ctx, t, srv, dir, target)

	pipeline := agentPipeline("agent-resumes", agent.ActionReadRun, agent.ActionStartRun)
	runID, err := srv.Submit(ctx, tenantID, pipeline)
	require.NoError(t, err)
	model.arm(runID, target.GetId())

	awaitStepEvent(ctx, t, srv, runID, "triage", scheduler.StepAwaitingApproval)

	approver := registerApprover(ctx, t, dir, "release-boss")
	token := issueToken(ctx, t, dir, approver)
	decided, err := decideApproval(ctx, apiClient(t, srv), token, &dholev1.DecideApprovalRequest{
		RunId: runID, StepId: "triage", Approved: true,
	})
	require.NoError(t, err, "the gate a parked agent opened could not be decided")
	require.Equal(t, approver, decided.GetApprover())

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "triage")

	// The answer is the one the model gave AFTER the gate opened, which it
	// could only have reached by continuing the conversation it was stopped
	// in the middle of.
	require.Equal(t, "the deploy is running",
		string(outputBytes(ctx, t, srv, events, "triage", "summary")))

	// Replay safety. `read_run` was taken once, before the park, and the
	// resume did not take it again.
	require.Equal(t, 1, countActions(t, events, agent.ActionReadRun, true),
		"the resumed loop replayed a call the parked loop had already made")
	require.Equal(t, 1, countActions(t, events, agent.ActionStartRun, true),
		"the approved action did not happen exactly once")
	require.Equal(t, 1, countActions(t, events, agent.ActionStartRun, false),
		"the gated ask is not in the log exactly once")

	// Three model calls in total: two before the park and one after. A
	// resume that re-prompted the model would show four or more, and the
	// model would have had to decide `read_run` all over again.
	require.Equal(t, 3, model.callCount(),
		"the model was asked to re-decide calls it had already made")
}

// TestAParkedAgentStepEndsWhenItsGateIsDenied.
//
// A denial ENDS the step. It is not handed back to the model as a tool result
// it can react to, because the gate is the boundary between an agent and an
// at-most-once effect: a model told "no" and left running routes around the
// person who said it, and the approval gate is keyed on (run, step), so there
// is no second gate for whatever it tried next. The run fails naming the
// approver, which is what a refusal is.
func TestAParkedAgentStepEndsWhenItsGateIsDenied(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	model := &conversationModel{}
	dir := t.TempDir()
	srv := startEmbeddedIn(ctx, t, dir, func(cfg *server.Config) {
		cfg.Models = model.factory()
	})

	target := unmatchablePipeline("denied-target")
	approveThroughTheContract(ctx, t, srv, dir, target)

	pipeline := agentPipeline("agent-denied", agent.ActionReadRun, agent.ActionStartRun)
	runID, err := srv.Submit(ctx, tenantID, pipeline)
	require.NoError(t, err)
	model.arm(runID, target.GetId())

	awaitStepEvent(ctx, t, srv, runID, "triage", scheduler.StepAwaitingApproval)

	approver := registerApprover(ctx, t, dir, "release-boss")
	token := issueToken(ctx, t, dir, approver)
	_, err = decideApproval(ctx, apiClient(t, srv), token, &dholev1.DecideApprovalRequest{
		RunId: runID, StepId: "triage", Approved: false,
	})
	require.NoError(t, err)

	events := awaitRunEnded(ctx, t, srv, runID, scheduler.RunFailed)
	failed := requireEvent(t, events, "triage", runstore.StepFailed)
	require.Contains(t, string(failed.Payload), approver,
		"the step failed without naming who refused it")

	require.Zero(t, countActions(t, events, agent.ActionStartRun, true),
		"a denied action happened anyway")
	require.Equal(t, 2, model.callCount(),
		"the model's loop was re-entered after a person refused it")
}

// countActions counts the AGENT_ACTION records for one action with one
// verdict. Allowed is recorded BEFORE the action is taken, so a count of two
// is two attempts and not two reports of one.
func countActions(t *testing.T, events []runstore.Event, action string, allowed bool) int {
	t.Helper()
	n := 0
	for _, e := range events {
		if e.Type != agent.EventAction {
			continue
		}
		var record agent.ActionRecord
		require.NoError(t, json.Unmarshal(e.Payload, &record))
		if record.Action == action && record.Allowed == allowed {
			n++
		}
	}
	return n
}

// unmatchablePipeline is something real for an agent to START whose steps no
// engine of this plane can claim, so the started run stays open and never
// competes with the run under test for anything.
func unmatchablePipeline(id string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     id,
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:           "work",
			PluginRef:    "oci://example.invalid/work",
			EffectClass:  dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
			Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK},
		}},
	}
}

// issueToken mints a credential for a subject the tenant already knows, the
// way `dhole token issue` does.
func issueToken(ctx context.Context, t *testing.T, dir, subject string) string {
	t.Helper()
	token, err := principals(t, dir).IssueToken(ctx, identity.Principal{
		TenantID: tenantID, Subject: subject, Kind: identity.PrincipalService,
	}, time.Hour)
	require.NoError(t, err)
	return token
}

// startEmbeddedIn is startEmbeddedWith over a caller-owned directory, so the
// test can open the plane's own credential database beside it.
func startEmbeddedIn(
	ctx context.Context, t *testing.T, dir string, with func(*server.Config),
) *server.Server {
	t.Helper()
	cfg := server.Config{
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
	}
	with(&cfg)
	srv, err := server.New(cfg)
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() { stopPlane(t, srv) })
	return srv
}

// conversationModel decides what to ask for from the CONVERSATION it is given,
// the way a model does, rather than from a queue of scripted turns.
//
// That is what makes the replay assertions mean anything. A queue cannot
// replay: hand it the same prompt twice and it has already run out of turns.
// This one, re-prompted from scratch, asks for `read_run` a SECOND time —
// which is precisely the failure a resume has to make impossible, and the
// thing the test would otherwise be unable to see.
type conversationModel struct {
	mu     sync.Mutex
	calls  int
	runID  string
	target string
}

// arm tells the model what to name in its calls. It is set after the run
// exists because `read_run` has to name a real run: a tool error would abort
// the loop rather than park it.
func (m *conversationModel) arm(runID, target string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runID, m.target = runID, target
}

func (m *conversationModel) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *conversationModel) factory() server.ModelFactory {
	return func(context.Context, server.ModelRequest) (provider.LanguageModel, error) {
		return m, nil
	}
}

func (m *conversationModel) ModelID() string                     { return "test-model" }
func (m *conversationModel) ProviderName() string                { return "stub" }
func (m *conversationModel) Capabilities() provider.Capabilities { return provider.Capabilities{} }

func (m *conversationModel) Generate(
	_ context.Context, call provider.Call,
) (*provider.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++

	answered := map[string]bool{}
	for _, msg := range call.Messages {
		for _, part := range msg.Content {
			if result, ok := part.(provider.ToolResultPart); ok {
				answered[result.Name] = true
			}
		}
	}
	switch {
	case !answered[agent.ActionReadRun]:
		return agentToolCall("c1", agent.ActionReadRun, `{"run_id":"`+m.runID+`"}`), nil
	case !answered[agent.ActionStartRun]:
		return agentToolCall("c2", agent.ActionStartRun, `{"pipeline_id":"`+m.target+`"}`), nil
	default:
		return agentText("the deploy is running"), nil
	}
}

func (m *conversationModel) Stream(
	context.Context, provider.Call,
) (provider.StreamResponse, error) {
	panic("the agent step does not stream")
}
