// `builtin:agent`, asked of the SERVER.
//
// ADR 0025: an agent step acts only through Dhole's own public API, as a
// principal of its tenant, and its action space is the contract — start a run,
// read a run, decide an approval gate, apply an operation to a pipeline.
// Nothing else.
//
// Every test here starts a plane with server.New/Start and supplies no wiring
// of its own. internal/steps/agent was a library with tests and no caller for
// the whole of Tasks 20-52 precisely because nothing gave it an invoker, and a
// test that handed it one itself would recreate the defect it is meant to
// close.
package server_test

import (
	"context"
	"encoding/json"
	nethttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/dhole/internal/steps/agent"
	"github.com/azrtydxb/go-ai-sdk/provider"
)

// TestAnAgentStepActsThroughThePlanesOwnContractUnderItsOwnSubject is the
// whole of ADR 0025 in one run: the agent takes a real action, it takes it as
// an ordinary authenticated client of the plane's own API, and the run log
// says which agent took it.
//
// The action is `read_run`, which is the only one of the four that is PURE —
// the other three are gated, which is the subject of the tests below.
func TestAnAgentStepActsThroughThePlanesOwnContractUnderItsOwnSubject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	model := &agentModel{}
	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.Models = model.factory()
	})

	pipeline := agentPipeline("agent-reads", agent.ActionReadRun)
	runID, err := srv.Submit(ctx, tenantID, pipeline)
	require.NoError(t, err)

	// The model asks to read the run it is itself a step of, then stops.
	model.script(
		agentToolCall("c1", agent.ActionReadRun,
			`{"run_id":"`+runID+`"}`),
		agentText("the run is in flight"),
	)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "triage")

	record := requireAgentAction(t, events)
	require.Equal(t, agent.ActionReadRun, record.Action)
	require.True(t, record.Allowed)
	require.Contains(t, record.Subject, pipeline.GetId(),
		"the action is not attributed to an agent of this pipeline")
	require.Contains(t, record.Subject, "triage",
		"the action is not attributed to the agent step that took it")
}

// TestAnAgentAskedForSomethingOutsideTheContractIsRefusedByName. ADR 0025's
// action space is a CLOSED set, and this is the boundary rather than the happy
// path: the model asks for the one thing the ADR names as the deliberate
// limit, and the refusal has to name what it tried.
//
// "The agent cannot run a command, and that is the deliberate limit. A
// pipeline that wants an agent to run something expresses it as a pipeline the
// agent STARTS."
func TestAnAgentAskedForSomethingOutsideTheContractIsRefusedByName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	model := &agentModel{}
	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.Models = model.factory()
	})
	model.script(agentToolCall("c1", "run_command", `{"args":["/bin/sh","-c","id"]}`))

	pipeline := agentPipeline("agent-overreaches", agent.ActionReadRun)
	runID, err := srv.Submit(ctx, tenantID, pipeline)
	require.NoError(t, err)

	awaitStepEvent(ctx, t, srv, runID, "triage", runstore.StepFailed)
	events, err := srv.Events(ctx, tenantID, runID)
	require.NoError(t, err)
	failed := requireEvent(t, events, "triage", runstore.StepFailed)
	var status dholev1.JobStatus
	require.NoError(t, proto.Unmarshal(failed.Payload, &status))
	require.Contains(t, status.GetError(), "run_command",
		"the refusal does not name what the agent tried")
	require.Contains(t, status.GetError(), agent.ActionReadRun,
		"the refusal does not name what the agent actually holds")
}

// TestAnAgentAskingToStartARunIsRoutedToTheSameGateAPersonWouldBe. ADR 0025:
// "The per-action approval gate becomes meaningful rather than theoretical: it
// gates real calls."
//
// Starting a run is the ONLY way an agent reaches anything effectful, so it is
// AT_MOST_ONCE and it stops at the gate a person's approval queue reads. The
// negative half is the half that matters: no run was started.
func TestAnAgentAskingToStartARunIsRoutedToTheSameGateAPersonWouldBe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	model := &agentModel{}
	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.Models = model.factory()
	})
	model.script(agentToolCall("c1", agent.ActionStartRun, `{"pipeline_id":"deploy"}`))

	pipeline := agentPipeline("agent-starts", agent.ActionStartRun)
	runID, err := srv.Submit(ctx, tenantID, pipeline)
	require.NoError(t, err)

	// The gate is armed against the AGENT's own step, which is what puts an
	// agent asking to deploy in the same queue as a person asking to deploy.
	//
	// What happens AFTER a person decides is deliberately not asserted, and
	// deliberately not built: resuming a parked agent means re-entering the
	// model's loop at the call it was stopped on, which this build cannot do,
	// so the step fails with the gate's own reason and the run stops. A run
	// that stops with a readable explanation is the right failure to leave
	// behind; a run that hangs on a gate nothing will ever release is not.
	awaitStepEvent(ctx, t, srv, runID, "triage", scheduler.StepAwaitingApproval)

	events, err := srv.Events(ctx, tenantID, runID)
	require.NoError(t, err)
	record := requireAgentAction(t, events)
	require.Equal(t, agent.ActionStartRun, record.Action)
	require.False(t, record.Allowed, "an at-most-once action was taken before anyone approved it")

	for _, e := range events {
		require.NotEqual(t, runstore.StepSucceeded, e.Type,
			"the agent step reported success on an action nobody approved")
	}
}

// TestAnAgentsCallIsRefusedWhenItsTokenWouldBe. ADR 0025: the agent "holds no
// capability a person with the same token would not have", and ADR 0013 has no
// privileged path.
//
// The invoker below is the SAME type the plane gives an agent step, pointed at
// the SAME listener, and it is refused — which is the proof that the agent
// reaches the API as an ordinary client rather than through a side door. A
// direct in-process call into api.Server would have passed this test by never
// having been authenticated at all.
func TestAnAgentsCallIsRefusedWhenItsTokenWouldBe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)

	invoker, err := agent.NewContractInvoker(agent.ContractInvokerConfig{
		HTTPClient:  &nethttp.Client{Timeout: 30 * time.Second},
		BaseURL:     "http://" + srv.APIAddr(),
		Credential:  "dht_" + tenantID + "_" + strings.Repeat("0", 64),
		ReadTimeout: 5 * time.Second,
	})
	require.NoError(t, err)

	_, err = invoker.Invoke(ctx, agent.Invocation{
		RunID: "run-1", StepID: "triage", Action: agent.ActionStartRun,
		Args: json.RawMessage(`{"pipeline_id":"deploy"}`),
	})
	require.Error(t, err)
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err),
		"an agent's call was served on a credential the same API refuses a person")
}

// --- what these tests build ------------------------------------------------

// agentPipeline is one `builtin:agent` step granted exactly the actions named.
func agentPipeline(id string, grants ...string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     id,
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:        "triage",
			Name:      "triage the failure",
			PluginRef: server.BuiltinAgent,
			// AT_MOST_ONCE for the same reason a loop is: an agent that spent
			// its ceiling will spend it again, and re-running it is tokens
			// burnt for the same ending.
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
			Config: map[string]string{
				"provider":     "stub",
				"model":        "test-model",
				"max_steps":    "3",
				"grants":       strings.Join(grants, ","),
				"instructions": "you triage failing builds",
				"prompt":       "the build failed; find out why",
			},
			Outputs: []*dholev1.Port{blobPort("summary")},
		}},
	}
}

// requireAgentAction is the run log's answer to "what did it do".
func requireAgentAction(t *testing.T, events []runstore.Event) agent.ActionRecord {
	t.Helper()
	for _, e := range events {
		if e.Type != agent.EventAction {
			continue
		}
		var record agent.ActionRecord
		require.NoError(t, json.Unmarshal(e.Payload, &record))
		return record
	}
	t.Fatalf("the run log holds no %s: an agent's actions are unaccounted for", agent.EventAction)
	return agent.ActionRecord{}
}

// --- the model -------------------------------------------------------------

// agentModel is a scripted model that can emit TOOL CALLS, which the
// text-only scriptedModel beside it cannot. It is deliberately able to ask for
// a tool it was never given: a stub that only ever names granted actions could
// not show that the action space is enforced at invocation.
type agentModel struct {
	mu    sync.Mutex
	turns []*provider.Response
}

func (m *agentModel) script(turns ...*provider.Response) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.turns = turns
}

func (m *agentModel) factory() server.ModelFactory {
	return func(context.Context, string, string) (provider.LanguageModel, error) {
		return m, nil
	}
}

func (m *agentModel) ModelID() string                     { return "test-model" }
func (m *agentModel) ProviderName() string                { return "stub" }
func (m *agentModel) Capabilities() provider.Capabilities { return provider.Capabilities{} }

func (m *agentModel) Generate(context.Context, provider.Call) (*provider.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.turns) > 0 {
		next := m.turns[0]
		m.turns = m.turns[1:]
		return next, nil
	}
	return agentText("done"), nil
}

func (m *agentModel) Stream(context.Context, provider.Call) (provider.StreamResponse, error) {
	panic("the agent step does not stream")
}

func agentToolCall(id, name, args string) *provider.Response {
	return &provider.Response{
		Content: []provider.ContentPart{
			provider.ToolCallPart{ID: id, Name: name, Args: json.RawMessage(args)},
		},
		FinishReason: provider.FinishToolCalls,
	}
}

func agentText(text string) *provider.Response {
	return &provider.Response{
		Content:      []provider.ContentPart{provider.TextPart{Text: text}},
		FinishReason: provider.FinishStop,
	}
}
