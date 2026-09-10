package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/gen/dhole/v1/dholev1connect"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/server"
)

// sleeper is a one-step pipeline whose step outlives the test. A run has to be
// genuinely in flight for cancellation to have anything to reach, and a step
// that finishes on its own would make "the run stopped" prove nothing.
func sleeper(id string) *dholev1.Pipeline {
	return &dholev1.Pipeline{
		Id:     id,
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:          "slow",
			Name:        "slow",
			PluginRef:   `command:{"args":["/bin/sh","-c","sleep 600 > outputs/out"]}`,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_AT_MOST_ONCE,
			Outputs: []*dholev1.Port{{
				Name: "out",
				Type: &dholev1.PortType{Kind: &dholev1.PortType_Blob{
					Blob: &dholev1.BlobType{MediaType: "text/plain"},
				}},
			}},
		}},
	}
}

// engineClient is a real client of EngineService on the served plane.
func engineClient(t *testing.T, srv *server.Server) dholev1connect.EngineServiceClient {
	t.Helper()
	addr := srv.APIAddr()
	require.NotEmpty(t, addr, "the plane is not listening for API calls")
	return dholev1connect.NewEngineServiceClient(httpClient(), "http://"+addr)
}

// TestTheFleetIsReadableAndDrainableThroughTheContract is what the CLI's
// `engine list` and `engine drain` refused to do behind the API's back.
//
// Both commands existed and refused, deliberately: they could each have read
// internal/registry directly — the binary contains it — and a CLI able to list
// engines while the GUI and an agent cannot is the same failure as a GUI-only
// endpoint seen from the other side (ADR 0013).
func TestTheFleetIsReadableAndDrainableThroughTheContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)
	engines := engineClient(t, srv)
	token := srv.BootstrapToken()

	// No credential: refused, like every other RPC in this system.
	_, err := engines.ListEngines(ctx, connect.NewRequest(&dholev1.ListEnginesRequest{}))
	require.Error(t, err)
	require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))

	fleet := awaitFleet(ctx, t, engines, token, func(e []*dholev1.Engine) bool { return len(e) > 0 })
	require.NotEmpty(t, fleet[0].GetId())
	require.NotEmpty(t, fleet[0].GetOs())
	require.NotEmpty(t, fleet[0].GetArch())

	drain := connect.NewRequest(&dholev1.DrainEngineRequest{EngineId: fleet[0].GetId()})
	drain.Header().Set("Authorization", "Bearer "+token)
	drained, err := engines.DrainEngine(ctx, drain)
	require.NoError(t, err)
	require.Equal(t, "draining", drained.Msg.GetEngine().GetState(),
		"draining an engine did not reach the registry")

	after := awaitFleet(ctx, t, engines, token, func(e []*dholev1.Engine) bool {
		return len(e) > 0 && e[0].GetState() == "draining"
	})
	require.Equal(t, "draining", after[0].GetState(),
		"the drain did not survive the next heartbeat")
}

// TestCancelRunReachesTheEngineHoldingTheStep is cancellation's engine-side
// half. Closing the run's log alone would leave the sandbox running and the
// slot occupied: the capacity a cancellation is asked for would never come
// back, and the operator would have been told the run had stopped.
//
// The assertion is on the wire, not on a mock: the test subscribes to the
// engine's own control subject and requires the Cancel to arrive there, with
// the step, the attempt and the fence of the attempt actually in flight.
func TestCancelRunReachesTheEngineHoldingTheStep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)
	pipelines, engines := apiClient(t, srv), engineClient(t, srv)
	token := srv.BootstrapToken()

	runID, err := srv.Submit(ctx, tenantID, sleeper("cancel-me"))
	require.NoError(t, err)

	// Wait until an engine's own heartbeat says it is holding this run's
	// step. Nothing else knows which engine has it: a dispatch goes to a
	// subject, and which member of the fleet picked it up is only ever
	// reported by the engine.
	fleet := awaitFleet(ctx, t, engines, token, func(all []*dholev1.Engine) bool {
		for _, e := range all {
			for _, held := range e.GetInFlight() {
				if held.GetRunId() == runID {
					return true
				}
			}
		}
		return false
	})

	var (
		engineID string
		held     *dholev1.InFlight
	)
	for _, e := range fleet {
		for _, job := range e.GetInFlight() {
			if job.GetRunId() == runID {
				engineID, held = e.GetId(), job
			}
		}
	}
	require.NotEmpty(t, engineID)

	// Listening on the engine's own inbound subject, before the cancel.
	conn, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	var (
		mu       sync.Mutex
		received []*dholev1.EngineControl
	)
	stop, err := conn.SubscribeEphemeral(ctx, bus.SubjectEngineControl(engineID), func(raw []byte) {
		msg := &dholev1.EngineControl{}
		if proto.Unmarshal(raw, msg) != nil {
			return
		}
		mu.Lock()
		received = append(received, msg)
		mu.Unlock()
	})
	require.NoError(t, err)
	t.Cleanup(stop)

	req := connect.NewRequest(&dholev1.CancelRunRequest{RunId: runID, Reason: "the operator asked"})
	req.Header().Set("Authorization", "Bearer "+token)
	res, err := pipelines.CancelRun(ctx, req)
	require.NoError(t, err)
	require.Equal(t, runID, res.Msg.GetRunId())
	require.Len(t, res.Msg.GetSteps(), 1)
	require.Equal(t, "slow", res.Msg.GetSteps()[0].GetStepId())
	require.Equal(t, engineID, res.Msg.GetSteps()[0].GetEngineId())

	// The message on the engine's subject, with the attempt's own fence: a
	// cancel carrying the wrong fence is one the engine must ignore.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, msg := range received {
			c := msg.GetCancel()
			if c.GetRunId() == runID && c.GetStepId() == "slow" &&
				c.GetAttempt() == held.GetAttempt() && c.GetFenceToken() == held.GetFenceToken() {
				return true
			}
		}
		return false
	}, 30*time.Second, 100*time.Millisecond,
		"no Cancel reached the engine holding the step")

	// And the run is closed in its own log, which is the run's only position.
	require.Eventually(t, func() bool {
		events, err := srv.Events(ctx, tenantID, runID)
		if err != nil {
			return false
		}
		for _, e := range events {
			if e.Type == runstore.RunCancelled {
				return true
			}
		}
		return false
	}, 30*time.Second, 200*time.Millisecond,
		"the run's log never recorded the cancellation")
}

// TestCancelRunRefusesARunItCannotSee keeps cancellation inside the tenant
// boundary: a run of another tenant is indistinguishable from one that does
// not exist.
func TestCancelRunRefusesARunItCannotSee(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startWithAPI(ctx, t)
	client := apiClient(t, srv)

	req := connect.NewRequest(&dholev1.CancelRunRequest{RunId: "run_nonesuch"})
	req.Header().Set("Authorization", "Bearer "+srv.BootstrapToken())
	_, err := client.CancelRun(ctx, req)
	require.Error(t, err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

// awaitFleet polls ListEngines until `want` is satisfied, and fails with what
// it last saw rather than with a bare timeout.
func awaitFleet(
	ctx context.Context, t *testing.T,
	engines dholev1connect.EngineServiceClient, token string,
	want func([]*dholev1.Engine) bool,
) []*dholev1.Engine {
	t.Helper()
	var last []*dholev1.Engine
	deadline := time.Now().Add(150 * time.Second)
	for {
		req := connect.NewRequest(&dholev1.ListEnginesRequest{})
		req.Header().Set("Authorization", "Bearer "+token)
		res, err := engines.ListEngines(ctx, req)
		require.NoError(t, err)
		last = res.Msg.GetEngines()
		if want(last) {
			return last
		}
		if time.Now().After(deadline) {
			t.Fatalf("the fleet never reached the state this test needs; last saw %v", last)
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}
