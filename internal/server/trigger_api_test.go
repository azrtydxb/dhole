// Triggers through the contract, asked of the shipping binary.
//
// A trigger used to be declarable and nothing else: `--triggers` read a YAML
// file into server.Config at start-up, so creating one meant a shell on the
// control plane's host and a restart. The GUI has neither and an agent has
// neither, which is the capability ADR 0013 refuses to leave off the contract.
//
// Every test here goes through a real generated Connect client over HTTP to a
// plane started by server.New/Start. A handler constructed in-process would
// prove the handler works and say nothing about whether `dhole serve` mounts
// it, which is a mistake this repository has made more than once.
package server_test

import (
	"context"
	nethttp "net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/server"
)

// webhookPipeline declares one free structured input, which IS the pipeline's
// declared input and what a trigger binds to (ADR 0007).
func webhookPipeline(id string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     id,
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:          "work",
			PluginRef:   `command:{"args":["/bin/sh","-c","printf %s ok > outputs/out"]}`,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
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
}

// contractClient is a real Connect client against a plane's own listener.
func contractClient(srv *server.Server) dholev1connect.PipelineServiceClient {
	return dholev1connect.NewPipelineServiceClient(
		&nethttp.Client{Timeout: 60 * time.Second}, "http://"+srv.APIAddr())
}

// createTrigger is one CreateTrigger with the plane's bootstrap credential on
// it, which is the credential an operator has.
func createTrigger(
	ctx context.Context, srv *server.Server, t *dholev1.Trigger,
) (*dholev1.Trigger, error) {
	req := connect.NewRequest(&dholev1.CreateTriggerRequest{Trigger: t})
	req.Header().Set("Authorization", "Bearer "+srv.BootstrapToken())
	res, err := contractClient(srv).CreateTrigger(ctx, req)
	if err != nil {
		return nil, err
	}
	return res.Msg.GetTrigger(), nil
}

func listTriggers(ctx context.Context, t *testing.T, srv *server.Server) []*dholev1.Trigger {
	t.Helper()
	req := connect.NewRequest(&dholev1.ListTriggersRequest{})
	req.Header().Set("Authorization", "Bearer "+srv.BootstrapToken())
	res, err := contractClient(srv).ListTriggers(ctx, req)
	require.NoError(t, err)
	return res.Msg.GetTriggers()
}

// postWebhook delivers one event to a trigger's endpoint on the plane's own
// listener and returns the status code.
func postWebhook(ctx context.Context, t *testing.T, srv *server.Server, id, body string) int {
	t.Helper()
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost,
		"http://"+srv.APIAddr()+server.TriggerPrefix+id, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := nethttp.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	return resp.StatusCode
}

// awaitWebhookServed waits until the plane serves a trigger's endpoint. The
// reconciler picks a stored trigger up on its own tick, so a create is live
// shortly after it returns rather than instantly, and a test that posted once
// would be timing that tick.
func awaitWebhookServed(ctx context.Context, t *testing.T, srv *server.Server, id, body string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if code := postWebhook(ctx, t, srv, id, body); code == nethttp.StatusAccepted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("trigger %q was created through the contract and is served by nothing", id)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestATriggerCreatedThroughTheContractIsServedByThePlaneWithoutARestart is
// the whole of clause (d): an operator with a credential and no shell creates
// an event source, and the plane that accepted it starts serving it.
//
// The mutation this catches is a CreateTrigger that writes a row nothing ever
// reads — which is what a trigger table without a reconciler would be, and
// would look exactly like this test's first half passing.
func TestATriggerCreatedThroughTheContractIsServedByThePlaneWithoutARestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	dir := t.TempDir()
	pipeline := webhookPipeline("contract-triggered")
	srv := startEmbeddedOn(ctx, t, dir)
	approveThroughTheContract(ctx, t, srv, dir, pipeline)

	// Before: nothing is served under that name.
	require.Equal(t, nethttp.StatusNotFound, postWebhook(ctx, t, srv, "hook", `{"ref":"main"}`),
		"a trigger nobody created is being served")

	created, err := createTrigger(ctx, srv, &dholev1.Trigger{
		Id:           "hook",
		Kind:         "http",
		PipelineId:   pipeline.GetId(),
		InputMapping: map[string]string{"event": "ref"},
		Untrusted:    true,
	})
	require.NoError(t, err)
	require.Equal(t, "hook", created.GetId())
	require.False(t, created.GetDeclared(), "a trigger created through the contract is not declared")

	awaitWebhookServed(ctx, t, srv, "hook", `{"ref":"refs/heads/main"}`)

	// And the delivery started a run, which is the only thing a trigger is
	// for. A 202 from an endpoint that fires nothing would satisfy every
	// assertion above.
	runID := awaitAnyRun(ctx, t, srv)
	require.NotEmpty(t, runID)
}

// TestAStoredTriggerIsRunByAPlaneThatDidNotCreateIt: the row is the
// configuration now, so a trigger has to outlive the process that accepted it
// — which is the difference between a table and a field on a struct.
func TestAStoredTriggerIsRunByAPlaneThatDidNotCreateIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	dir := t.TempDir()
	pipeline := webhookPipeline("outlives-its-plane")
	first := startEmbeddedOn(ctx, t, dir)
	approveThroughTheContract(ctx, t, first, dir, pipeline)
	_, err := createTrigger(ctx, first, &dholev1.Trigger{
		Id:           "survivor",
		Kind:         "http",
		PipelineId:   pipeline.GetId(),
		InputMapping: map[string]string{"event": "ref"},
	})
	require.NoError(t, err)
	stopPlane(t, first)

	second := startEmbeddedOn(ctx, t, dir)
	awaitWebhookServed(ctx, t, second, "survivor", `{"ref":"refs/heads/main"}`)
	require.NotEmpty(t, awaitAnyRun(ctx, t, second))
}

// TestADeletedTriggerStopsBeingServed. A delete that removed the row and left
// the endpoint up would be a trigger an operator believes is gone and that
// still starts runs.
func TestADeletedTriggerStopsBeingServed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	dir := t.TempDir()
	pipeline := webhookPipeline("deletable")
	srv := startEmbeddedOn(ctx, t, dir)
	approveThroughTheContract(ctx, t, srv, dir, pipeline)
	_, err := createTrigger(ctx, srv, &dholev1.Trigger{
		Id:           "temporary",
		Kind:         "http",
		PipelineId:   pipeline.GetId(),
		InputMapping: map[string]string{"event": "ref"},
	})
	require.NoError(t, err)
	awaitWebhookServed(ctx, t, srv, "temporary", `{"ref":"refs/heads/main"}`)

	del := connect.NewRequest(&dholev1.DeleteTriggerRequest{TriggerId: "temporary"})
	del.Header().Set("Authorization", "Bearer "+srv.BootstrapToken())
	_, err = contractClient(srv).DeleteTrigger(ctx, del)
	require.NoError(t, err)
	require.Empty(t, listTriggers(ctx, t, srv))

	deadline := time.Now().Add(30 * time.Second)
	for {
		if postWebhook(ctx, t, srv, "temporary", `{"ref":"main"}`) == nethttp.StatusNotFound {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("a deleted trigger is still served, so it still starts runs")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestATriggerDeclaredInTheConfigurationFileWinsOverAStoredOne states the
// precedence, and states it as a refusal rather than as a silent override: the
// `--triggers` file is what the next restart reads, so a stored row of the
// same id would fire or not depending on a file the caller cannot see.
func TestATriggerDeclaredInTheConfigurationFileWinsOverAStoredOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	dir := t.TempDir()
	pipeline := webhookPipeline("declared-wins")
	seed := startEmbeddedOn(ctx, t, dir)
	approveThroughTheContract(ctx, t, seed, dir, pipeline)
	stopPlane(t, seed)

	srv := startEmbeddedWith(ctx, t, func(cfg *server.Config) {
		cfg.StoreDSN = filepath.Join(dir, "dhole.db")
		cfg.BlobRoot = filepath.Join(dir, "state")
		cfg.Triggers = []server.TriggerSpec{{
			ID:           "hook",
			Kind:         "http",
			PipelineID:   pipeline.GetId(),
			InputMapping: map[string]string{"event": "ref"},
		}}
	})

	_, err := createTrigger(ctx, srv, &dholev1.Trigger{
		Id:           "hook",
		Kind:         "http",
		PipelineId:   pipeline.GetId(),
		InputMapping: map[string]string{"event": "ref"},
	})
	require.Error(t, err)
	require.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))
	require.Contains(t, err.Error(), "--triggers")

	// A declared trigger is not deletable through the contract either: the
	// row is not where it lives.
	del := connect.NewRequest(&dholev1.DeleteTriggerRequest{TriggerId: "hook"})
	del.Header().Set("Authorization", "Bearer "+srv.BootstrapToken())
	_, err = contractClient(srv).DeleteTrigger(ctx, del)
	require.Error(t, err)
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// And it is LISTED, marked as declared. An operator mid-migration has
	// some of each, and a listing that showed only the stored half would make
	// the declared half invisible.
	listed := listTriggers(ctx, t, srv)
	require.Len(t, listed, 1)
	require.Equal(t, "hook", listed[0].GetId())
	require.True(t, listed[0].GetDeclared())
}

// TestListTriggersNeverReturnsTheSecretItWasGiven: a contract that read back
// the shared secret a forge signs with would make every listing client a way
// to exfiltrate it.
func TestListTriggersNeverReturnsTheSecretItWasGiven(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	dir := t.TempDir()
	pipeline := webhookPipeline("forge-driven")
	srv := startEmbeddedOn(ctx, t, dir)
	approveThroughTheContract(ctx, t, srv, dir, pipeline)

	created, err := createTrigger(ctx, srv, &dholev1.Trigger{
		Id:           "forge",
		Kind:         "git",
		PipelineId:   pipeline.GetId(),
		InputMapping: map[string]string{"event": "ref"},
		Secret:       "the-shared-secret",
	})
	require.NoError(t, err)
	require.Empty(t, created.GetSecret(), "the create echoed the secret back")
	require.True(t, created.GetHasSecret())

	listed := listTriggers(ctx, t, srv)
	require.Len(t, listed, 1)
	require.Empty(t, listed[0].GetSecret(), "ListTriggers returned a trigger's secret")
	require.True(t, listed[0].GetHasSecret(),
		"a caller cannot tell a trigger with a secret from one without")
}

// TestAGitTriggerWithNoSecretIsRefused keeps the endpoint an endpoint that
// verifies something. One that can verify nothing is an unauthenticated way to
// start somebody's pipeline.
func TestAGitTriggerWithNoSecretIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	dir := t.TempDir()
	pipeline := webhookPipeline("unsigned")
	srv := startEmbeddedOn(ctx, t, dir)
	approveThroughTheContract(ctx, t, srv, dir, pipeline)

	_, err := createTrigger(ctx, srv, &dholev1.Trigger{
		Id: "forge", Kind: "git", PipelineId: pipeline.GetId(),
	})
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.Contains(t, err.Error(), "secret")
}

// TestACreatedTriggerIsRefusedForAnInputThePipelineDoesNotDeclare moves the
// binding check to CREATION time, which is where ADR 0007 puts it: a binding
// nobody checked fails deep inside a step, at 3am, in a run that should never
// have started.
func TestACreatedTriggerIsRefusedForAnInputThePipelineDoesNotDeclare(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	dir := t.TempDir()
	pipeline := webhookPipeline("strict-binding")
	srv := startEmbeddedOn(ctx, t, dir)
	approveThroughTheContract(ctx, t, srv, dir, pipeline)

	_, err := createTrigger(ctx, srv, &dholev1.Trigger{
		Id:           "hook",
		Kind:         "http",
		PipelineId:   pipeline.GetId(),
		InputMapping: map[string]string{"nothing-declares-this": "ref"},
	})
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.Contains(t, err.Error(), "nothing-declares-this")

	// Nothing was stored, so the refusal is not merely cosmetic.
	require.Empty(t, listTriggers(ctx, t, srv))
}
