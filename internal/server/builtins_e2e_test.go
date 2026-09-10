// The step types, the triggers and the timer poll, asked of the SERVER.
//
// Every one of these subsystems was built and tested before this file existed,
// and none of them ran in the shipped binary: `dhole serve` imported no step
// type, no trigger and no timer poll, and the acceptance harness was the
// missing dispatcher — it drove all of them itself, against the same store and
// the same tenant, which is why its tests passed while the product did
// nothing. So nothing here supplies wiring: each test starts a plane with
// server.New/Start, submits a pipeline, and reads the run's event log, which
// is the only place a run's state lives (ADR 0003).
package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/identity"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/dhole/internal/steps/approval"
	"github.com/azrtydxb/dhole/internal/steps/loop"
	"github.com/azrtydxb/dhole/internal/wait"
	"github.com/azrtydxb/go-ai-sdk/provider"
)

// findingSchema is the shape a model answer has to have in these tests. It is
// on the step's OUTPUT PORT because that is the only place a schema can live:
// there is no second declaration for an answer to be judged against.
const findingSchema = `{
  "type": "object",
  "additionalProperties": false,
  "required": ["severity", "done"],
  "properties": {
    "severity": {"type": "string", "enum": ["low", "medium", "high"]},
    "done": {"type": "boolean"}
  }
}`

// TestATimerStepMakesTheRunWaitAndThePlanesOwnPollEndsTheWait is the whole
// point of a durable timer: nothing sleeps, and nothing in this test fires it.
//
// The mutation this exists to catch is the plane that runs no poll. `dhole
// serve` ran none for four tasks, so every wait a run entered was a wait it
// never left; the acceptance suite passed because its harness started
// wait.Runner itself.
func TestATimerStepMakesTheRunWaitAndThePlanesOwnPollEndsTheWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)

	pipeline := &dholev1.Pipeline{
		Id:     "builtin-timer",
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{
			commandStep("work", `printf %s done > outputs/out`),
			{
				Id:          "hold",
				Name:        "wait for the timer",
				PluginRef:   server.BuiltinTimer,
				EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
				Config:      map[string]string{"duration": "3s"},
			},
		},
	}

	runID, err := srv.Submit(ctx, tenantID, pipeline)
	require.NoError(t, err)

	// The gate is armed before anything ends it. Without this the assertions
	// below would hold just as well for a plane that never gated the step.
	awaitStepEvent(ctx, t, srv, runID, "hold", scheduler.StepAwaitingTimer)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "hold")
	requireStepSucceeded(t, events, "work")

	// The wait ENDED because a timer fired, not because anything skipped it.
	fired := requireEvent(t, events, "hold", wait.StepTimerFired)
	var record wait.Fired
	require.NoError(t, json.Unmarshal(fired.Payload, &record))
	require.False(t, record.FiredAt.Before(record.DueAt),
		"a timer that fired before it was due did not wait for anything")

	// And a gate is not work: nobody was asked to run it.
	requireNeverDispatched(t, events, "hold")
}

// TestAnApprovalStepHoldsTheRunUntilTheServerIsToldSomebodyDecided is the
// human gate, decided through the plane that armed it.
//
// A run holding one is neither running nor finished and costs a row. The
// negative half is the half that matters: the run must still be open before
// anyone decides, or "the gate held" is a claim about a step that was never
// gated at all.
func TestAnApprovalStepHoldsTheRunUntilTheServerIsToldSomebodyDecided(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	dir := t.TempDir()
	srv := startEmbeddedOn(ctx, t, dir)
	approver := registerApprover(ctx, t, dir, "release-manager")

	pipeline := &dholev1.Pipeline{
		Id:     "builtin-approval",
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:          "approve",
			Name:        "approve the release",
			PluginRef:   server.BuiltinApproval,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
			Config:      map[string]string{"prompt": "Approve applying release 1.4.0?"},
		}},
	}

	runID, err := srv.Submit(ctx, tenantID, pipeline)
	require.NoError(t, err)
	awaitStepEvent(ctx, t, srv, runID, "approve", scheduler.StepAwaitingApproval)

	// Two seconds of a plane advancing this run every 250ms. A gate that did
	// not hold would have completed the run many times over by now.
	time.Sleep(2 * time.Second)
	events, err := srv.Events(ctx, tenantID, runID)
	require.NoError(t, err)
	require.False(t, hasEvent(events, "", runstore.RunCompleted),
		"the run finished with nobody having decided its approval; log: %s", describe(events))

	decidedAt := time.Now().UTC()
	require.NoError(t, srv.Approve(ctx, tenantID, runID, "approve", approver, true))

	events = awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "approve")
	requireNeverDispatched(t, events, "approve")

	var decision approval.Decision
	require.NoError(t, json.Unmarshal(
		requireEvent(t, events, "approve", approval.StepApprovalDecided).Payload, &decision))
	require.Equal(t, approver, decision.Approver)
	require.True(t, decision.Approved)
	require.False(t, decision.At.Before(decidedAt),
		"the decision was recorded before it was made")
}

// TestAnApprovalStepRefusesAnApproverTheTenantDoesNotKnow keeps the gate a
// gate. An approval whose approver is not a principal of the tenant is not an
// approval, and a log that recorded it would say something untrue about a
// person.
func TestAnApprovalStepRefusesAnApproverTheTenantDoesNotKnow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)
	pipeline := &dholev1.Pipeline{
		Id:     "builtin-approval-stranger",
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:          "approve",
			PluginRef:   server.BuiltinApproval,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
			Config:      map[string]string{"prompt": "ship it?"},
		}},
	}
	runID, err := srv.Submit(ctx, tenantID, pipeline)
	require.NoError(t, err)
	awaitStepEvent(ctx, t, srv, runID, "approve", scheduler.StepAwaitingApproval)

	err = srv.Approve(ctx, tenantID, runID, "approve", "somebody-else", true)
	require.ErrorIs(t, err, approval.ErrUnknownApprover)
}

// TestAnLLMStepCallsTheModelThePlaneWasGivenAndRefusesAnAnswerOffItsSchema is
// the model call as the plane runs it: the answer is validated against the
// schema the step's own output port declares, and reaches the next step
// through the content-addressed store like any other artifact.
func TestAnLLMStepCallsTheModelThePlaneWasGivenAndRefusesAnAnswerOffItsSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	model := &scriptedModel{id: "test-model"}
	model.always(`{"severity":"high","done":true}`)
	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.Models = model.factory()
	})

	runID, err := srv.Submit(ctx, tenantID, llmPipeline("builtin-llm", "classify this release"))
	require.NoError(t, err)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "classify")
	require.JSONEq(t, `{"severity":"high","done":true}`,
		string(outputBytes(ctx, t, srv, events, "classify", "finding")),
		"the answer has to reach the store, or no later step could read it")
	require.Positive(t, model.calls(), "the plane called no model at all")

	// And the schema is a refusal, not a decoration.
	model.always(`{"severity":"catastrophic","done":true}`)
	offSchema, err := srv.Submit(ctx, tenantID, llmPipeline("builtin-llm-off-schema", "classify this release"))
	require.NoError(t, err)
	failed := awaitRunEnded(ctx, t, srv, offSchema, scheduler.RunFailed)
	require.Contains(t, describe(failed), "STEP_FAILED",
		"an answer of the wrong shape must fail the step; log: %s", describe(failed))
}

// TestABoundedLoopStepStopsWhenItsExitConditionHolds is the good ending: the
// loop ends because the condition held, and every pass is in the log under its
// own unrolled step id.
func TestABoundedLoopStepStopsWhenItsExitConditionHolds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	model := &scriptedModel{id: "test-model"}
	// Not done, then done: the loop must run twice and stop, which is neither
	// its first pass nor its ceiling.
	model.script(
		`{"severity":"medium","done":false}`,
		`{"severity":"low","done":true}`,
	)
	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.Models = model.factory()
	})

	runID, err := srv.Submit(ctx, tenantID, loopPipeline("builtin-loop", 3))
	require.NoError(t, err)

	events := awaitRunCompleted(ctx, t, srv, runID)
	requireStepSucceeded(t, events, "refine")
	requireEvent(t, events, "refine", loop.EventExited)
	require.Equal(t, 2, countEvents(events, loop.EventIterationStarted),
		"the loop ran a different number of passes than its condition asked for; log: %s",
		describe(events))
}

// TestABoundedLoopStepStopsAtItsCeilingWhateverTheBodySays is the property
// ADR 0015 is about: a loop whose condition never holds is stopped by its
// bound, and says in the log that that is what happened.
func TestABoundedLoopStepStopsAtItsCeilingWhateverTheBodySays(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	model := &scriptedModel{id: "test-model"}
	model.always(`{"severity":"medium","done":false}`)
	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.Models = model.factory()
	})

	runID, err := srv.Submit(ctx, tenantID, loopPipeline("builtin-loop-ceiling", 3))
	require.NoError(t, err)

	awaitStepEvent(ctx, t, srv, runID, "refine", runstore.StepFailed)
	events, err := srv.Events(ctx, tenantID, runID)
	require.NoError(t, err)

	requireEvent(t, events, "refine", loop.EventCeilingReached)
	require.Equal(t, 3, countEvents(events, loop.EventIterationStarted),
		"the ceiling is three, so three passes and no more; log: %s", describe(events))

	// And the run stops rather than spending the allowance again: the step is
	// AT_MOST_ONCE, so the plane records that a person has to authorise a
	// replay instead of retrying on its own (ADR 0002).
	awaitStepEvent(ctx, t, srv, runID, "refine", scheduler.StepAwaitingReplay)
	events, err = srv.Events(ctx, tenantID, runID)
	require.NoError(t, err)
	require.False(t, hasEvent(events, "", runstore.RunCompleted),
		"a loop stopped by its ceiling completed the run; log: %s", describe(events))
}

// TestAConfiguredScheduleAndWebhookEachStartARunThroughTheServer is the
// triggers, run by the plane rather than by a test that started them.
//
// Both are declared the way a deployment declares them — on server.Config —
// and neither is touched afterwards: the cron runs on the plane's own poll and
// the webhook is served on the plane's own listener, under TriggerPrefix.
func TestAConfiguredScheduleAndWebhookEachStartARunThroughTheServer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	dir := t.TempDir()
	pipeline := &dholev1.Pipeline{
		Id:     "triggered",
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:          "work",
			PluginRef:   `command:{"args":["/bin/sh","-c","printf %s ok > outputs/out"]}`,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			// A free input port IS the pipeline's declared input, and what a
			// trigger binds to (ADR 0007). There is no separate inputs list.
			Inputs: []*dholev1.Port{{
				Name: "event",
				Type: &dholev1.PortType{Kind: &dholev1.PortType_Structured{
					Structured: &dholev1.StructType{
						SchemaId: "dhole:test/event",
						Schema:   `{"type":"string"}`,
					},
				}},
			}},
			Outputs: []*dholev1.Port{blobPort("out")},
		}},
	}

	// A trigger drives a pipeline's ACTIVE revision, so the definition has to
	// exist and be approved before a plane can be configured to trigger it.
	// The first plane is what puts it there, through the contract.
	seed := startEmbeddedOn(ctx, t, dir)
	approveThroughTheContract(ctx, t, seed, dir, pipeline)
	stopPlane(t, seed)

	triggered := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.StoreDSN = filepath.Join(dir, "dhole.db")
		cfg.BlobRoot = filepath.Join(dir, "state")
		cfg.Triggers = []server.TriggerSpec{
			{
				ID:         "every-second",
				Kind:       "schedule",
				PipelineID: pipeline.GetId(),
				// Six fields: seconds are optional in the parser, and a test
				// that waited for a minute boundary is a test nobody runs.
				Expression:   "* * * * * *",
				InputMapping: map[string]string{"event": "scheduled_for"},
			},
			{
				ID:           "webhook",
				Kind:         "http",
				PipelineID:   pipeline.GetId(),
				InputMapping: map[string]string{"event": "ref"},
			},
		}
	})

	// The cron fires on the plane's own poll: nothing here ticks it.
	scheduled := awaitAnyRun(ctx, t, triggered)

	// And the webhook is really on the plane's listener, at the path the
	// server mounts it on.
	endpoint := "http://" + triggered.APIAddr() + server.TriggerPrefix + "webhook"
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, endpoint,
		strings.NewReader(`{"ref":"refs/heads/main"}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, nethttp.StatusAccepted, resp.StatusCode,
		"the webhook trigger is not served on the plane's own listener")

	// Two triggers, two runs — and a second run is the proof the webhook did
	// something the cron had not already done.
	deadline := time.Now().Add(60 * time.Second)
	for {
		runs, err := triggered.OpenRuns(ctx, tenantID)
		require.NoError(t, err)
		if len(runs) > 1 || sawTwoRuns(ctx, t, triggered, scheduled) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the webhook fired and no second run appeared; open runs: %v", runs)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestAWebhookTriggerIsRefusedForAPipelineInputItDoesNotDeclare keeps the
// binding check at CONFIGURATION time, which is the whole reason it exists: a
// binding nobody checked fails deep inside a step, at 3am, in a run that
// should never have started (ADR 0007).
func TestAWebhookTriggerIsRefusedForAPipelineInputItDoesNotDeclare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	dir := t.TempDir()
	pipeline := &dholev1.Pipeline{
		Id:     "no-inputs",
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps:  []*dholev1.Step{commandStep("work", `printf %s ok > outputs/out`)},
	}
	seed := startEmbeddedOn(ctx, t, dir)
	approveThroughTheContract(ctx, t, seed, dir, pipeline)
	stopPlane(t, seed)

	srv, err := server.New(server.Config{
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
		Triggers: []server.TriggerSpec{{
			ID:           "webhook",
			Kind:         "http",
			PipelineID:   pipeline.GetId(),
			InputMapping: map[string]string{"nothing-declares-this": "ref"},
		}},
	})
	require.NoError(t, err)
	err = srv.Start(ctx)
	require.Error(t, err, "a plane started with a trigger it could not configure")
	require.Contains(t, err.Error(), "webhook")
	// A refused start leaves nothing running, so there is nothing to stop.
	require.Empty(t, srv.APIAddr())
}

// --- what these tests build ------------------------------------------------

func commandStep(id, script string) *dholev1.Step {
	return &dholev1.Step{
		Id:          id,
		PluginRef:   fmt.Sprintf(`command:{"args":["/bin/sh","-c",%q]}`, script),
		EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
		Outputs:     []*dholev1.Port{blobPort("out")},
	}
}

func blobPort(name string) *dholev1.Port {
	return &dholev1.Port{
		Name: name,
		Type: &dholev1.PortType{Kind: &dholev1.PortType_Blob{
			Blob: &dholev1.BlobType{MediaType: "text/plain"},
		}},
	}
}

func findingPort(name string) *dholev1.Port {
	return &dholev1.Port{
		Name: name,
		Type: &dholev1.PortType{Kind: &dholev1.PortType_Structured{
			Structured: &dholev1.StructType{
				SchemaId: "dhole:test/finding",
				Schema:   findingSchema,
			},
		}},
	}
}

func llmPipeline(id, prompt string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     id,
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:          "classify",
			PluginRef:   server.BuiltinLLM,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_IDEMPOTENT,
			Config: map[string]string{
				"provider":   "stub",
				"model":      "test-model",
				"max_tokens": "4096",
				"prompt":     prompt,
			},
			Outputs: []*dholev1.Port{findingPort("finding")},
		}},
	}
}

func loopPipeline(id string, ceiling int) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     id,
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:        "refine",
			PluginRef: server.BuiltinLoop,
			// AT_MOST_ONCE, because a loop that spent its whole allowance
			// without its condition holding will spend it again: repeating it
			// automatically is tokens burnt for the same ending.
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
			Config: map[string]string{
				"body":           server.BuiltinLLM,
				"max_iterations": fmt.Sprint(ceiling),
				"exit_condition": "state.done",
				"provider":       "stub",
				"model":          "test-model",
				"prompt":         "refine the finding",
			},
			Outputs: []*dholev1.Port{findingPort("plan")},
		}},
	}
}

// --- what these tests need from a plane ------------------------------------

// startEmbeddedWith is startEmbedded with the configuration a test needs
// changed. It is a mutator rather than a whole Config so that a test states
// only the thing under test, and so a field added to Config later reaches
// every test here.
func startEmbeddedWith(ctx context.Context, t *testing.T, with func(*server.Config)) *server.Server {
	t.Helper()
	dir := t.TempDir()
	cfg := server.Config{
		// Port zero: these tests run beside each other, and a plane bound to
		// the well-known port would fight for a socket.
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

// startEmbeddedOn is a plane over a caller-owned directory, so a second plane
// can be started over the same store.
func startEmbeddedOn(ctx context.Context, t *testing.T, dir string) *server.Server {
	t.Helper()
	srv, err := server.New(server.Config{
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	// Stopping twice is not an error, so a test that stops this one itself —
	// to start a second plane over the same store — still gets a cleanup.
	t.Cleanup(func() { stopPlane(t, srv) })
	return srv
}

func stopPlane(t *testing.T, srv *server.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	require.NoError(t, srv.Stop(ctx))
}

// principals opens the plane's own credential database, beside the run log it
// shares. It is opened directly rather than through the server because the
// contract has no identity RPC — the same compromise `dhole token issue` makes
// and for the same reason.
func principals(t *testing.T, dir string) *identity.Local {
	t.Helper()
	db, err := runstore.OpenSQLite(filepath.Join(dir, "dhole.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return identity.NewLocal(identity.NewSQLStoreWithDialect(db, runstore.DialectSQLite))
}

// registerApprover creates a principal the tenant knows, which is what an
// approval is verified against.
func registerApprover(ctx context.Context, t *testing.T, dir, subject string) string {
	t.Helper()
	require.NoError(t, principals(t, dir).CreateUser(ctx, tenantID, subject, "test-secret"))
	return subject
}

// approveThroughTheContract creates and approves a pipeline over the served
// API, which is the one contract the GUI, the CLI and agents all use
// (ADR 0013). A trigger drives the ACTIVE revision, and only an approval makes
// one active.
func approveThroughTheContract(
	ctx context.Context, t *testing.T, srv *server.Server, dir string, pipeline *dholev1.Pipeline,
) {
	t.Helper()
	client := dholev1connect.NewPipelineServiceClient(
		&nethttp.Client{Timeout: 60 * time.Second}, "http://"+srv.APIAddr())

	create := connect.NewRequest(&dholev1.CreatePipelineRequest{
		PipelineId: pipeline.GetId(),
		Pipeline:   pipeline,
	})
	create.Header().Set("Authorization", "Bearer "+srv.BootstrapToken())
	created, err := client.CreatePipeline(ctx, create)
	require.NoError(t, err)

	// The author may not approve their own definition, so the approval needs
	// a second principal with a credential of its own.
	local := principals(t, dir)
	require.NoError(t, local.CreateUser(ctx, tenantID, "reviewer", "test-secret"))
	token, err := local.IssueToken(ctx, identity.Principal{
		TenantID: tenantID, Subject: "reviewer", Kind: identity.PrincipalService,
	}, time.Hour)
	require.NoError(t, err)

	approve := connect.NewRequest(&dholev1.ApproveRevisionRequest{
		RevisionId: created.Msg.GetRevision().GetId(),
	})
	approve.Header().Set("Authorization", "Bearer "+token)
	_, err = client.ApproveRevision(ctx, approve)
	require.NoError(t, err)
}

// --- reading a run ---------------------------------------------------------

func awaitStepEvent(
	ctx context.Context, t *testing.T, srv *server.Server,
	runID, stepID string, kind runstore.EventType,
) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		events, err := srv.Events(ctx, tenantID, runID)
		require.NoError(t, err)
		if hasEvent(events, stepID, kind) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s step %s never recorded %s; log: %s",
				runID, stepID, kind, describe(events))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("run %s step %s never recorded %s before the deadline; log: %s",
				runID, stepID, kind, describe(events))
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func awaitRunEnded(
	ctx context.Context, t *testing.T, srv *server.Server, runID string, kind runstore.EventType,
) []runstore.Event {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		events, err := srv.Events(ctx, tenantID, runID)
		require.NoError(t, err)
		if hasEvent(events, "", kind) {
			return events
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never recorded %s; log: %s", runID, kind, describe(events))
		}
		select {
		case <-ctx.Done():
			t.Fatalf("run %s never recorded %s before the deadline; log: %s",
				runID, kind, describe(events))
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// awaitAnyRun waits until this plane has a run it was never handed the id of,
// which is what a trigger produces.
func awaitAnyRun(ctx context.Context, t *testing.T, srv *server.Server) string {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		runs, err := srv.OpenRuns(ctx, tenantID)
		require.NoError(t, err)
		if len(runs) > 0 {
			return runs[0]
		}
		if time.Now().After(deadline) {
			t.Fatal("no trigger ever started a run")
		}
		select {
		case <-ctx.Done():
			t.Fatal("no trigger started a run before the deadline")
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// sawTwoRuns reports whether a run other than the one named exists. Open runs
// close as they finish, so counting the index alone would be a race against
// the plane finishing the first one.
func sawTwoRuns(ctx context.Context, t *testing.T, srv *server.Server, first string) bool {
	t.Helper()
	runs, err := srv.OpenRuns(ctx, tenantID)
	require.NoError(t, err)
	for _, id := range runs {
		if id != first {
			return true
		}
	}
	return false
}

func hasEvent(events []runstore.Event, stepID string, kind runstore.EventType) bool {
	for _, e := range events {
		if e.Type == kind && (stepID == "" || e.StepID == stepID) {
			return true
		}
	}
	return false
}

func requireEvent(
	t *testing.T, events []runstore.Event, stepID string, kind runstore.EventType,
) runstore.Event {
	t.Helper()
	for _, e := range events {
		if e.StepID == stepID && e.Type == kind {
			return e
		}
	}
	t.Fatalf("step %q never recorded %s; log: %s", stepID, kind, describe(events))
	return runstore.Event{}
}

func requireNeverDispatched(t *testing.T, events []runstore.Event, stepID string) {
	t.Helper()
	for _, e := range events {
		require.False(t, e.StepID == stepID && e.Type == runstore.StepDispatched,
			"step %q was dispatched to an engine; no engine can run a gate. log: %s",
			stepID, describe(events))
	}
}

func countEvents(events []runstore.Event, kind runstore.EventType) int {
	n := 0
	for _, e := range events {
		if e.Type == kind {
			n++
		}
	}
	return n
}

// --- the model these tests call --------------------------------------------

// scriptedModel is the language model the plane calls. It NEVER makes a
// network call: no test may spend somebody's money or depend on a provider
// being up, and the point of the step is the schema, the record and the fact
// that the PLANE made the call at all.
//
// Its response names a resolved model in the raw body, the way every real
// provider does, because that is what the step fingerprints.
type scriptedModel struct {
	id string

	mu      sync.Mutex
	answers []string
	last    string
	n       int
}

// always answers the same thing however many times it is asked.
func (m *scriptedModel) always(text string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.answers, m.last = nil, text
}

// script answers each of these in turn, then repeats the final one.
func (m *scriptedModel) script(texts ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.answers, m.last = texts, texts[len(texts)-1]
}

func (m *scriptedModel) calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.n
}

func (m *scriptedModel) next() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n++
	if len(m.answers) > 0 {
		text := m.answers[0]
		m.answers = m.answers[1:]
		return text
	}
	return m.last
}

// factory is what a deployment gives server.Config.Models.
func (m *scriptedModel) factory() server.ModelFactory {
	return func(context.Context, server.ModelRequest) (provider.LanguageModel, error) {
		return m, nil
	}
}

func (m *scriptedModel) ModelID() string      { return m.id }
func (m *scriptedModel) ProviderName() string { return "stub" }

// Capabilities advertises native JSON, so the step asks for an object through
// the response format rather than through a forced tool call.
func (m *scriptedModel) Capabilities() provider.Capabilities {
	return provider.Capabilities{NativeJSON: true}
}

func (m *scriptedModel) Generate(context.Context, provider.Call) (*provider.Response, error) {
	text := m.next()
	return &provider.Response{
		Content:      []provider.ContentPart{provider.TextPart{Text: text}},
		FinishReason: provider.FinishStop,
		Usage:        provider.Usage{InputTokens: 60, OutputTokens: 20, TotalTokens: 80},
		// A RESOLVED model id in the raw body, the way every real provider
		// answers, because that is what the step fingerprints.
		Raw: json.RawMessage(fmt.Sprintf(`{"model":%q}`, m.id+"-20260101")),
	}, nil
}

func (m *scriptedModel) Stream(context.Context, provider.Call) (provider.StreamResponse, error) {
	return nil, fmt.Errorf("scripted model %q does not stream", m.id)
}
