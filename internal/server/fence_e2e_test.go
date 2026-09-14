package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/engine"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/wire"
)

// TestThePlaneAnswersAnAcceptanceRequestByTheLease is the plane's half of
// ADR 0029, through the real server. An engine asks, before it starts a
// dispatch, whether the dispatch's fence is still the step's lease: a
// superseded fence is answered FENCED, the current one CURRENT — and CURRENT
// is the engine accepting the lease, exactly as its ACCEPTED status is, so the
// offer stops waiting.
func TestThePlaneAnswersAnAcceptanceRequestByTheLease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)
	conn, err := nats.Connect(srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	lost, err := leases.Offer(ctx, tenantID, "run-accept", "build", 1, time.Minute)
	require.NoError(t, err)
	current, err := leases.Offer(ctx, tenantID, "run-accept", "build", 2, time.Minute)
	require.NoError(t, err)

	engineBus, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(engineBus.Close)

	ask := func(attempt uint32, token lease.Token) *dholev1.AcceptReply {
		t.Helper()
		reqCtx, done := context.WithTimeout(ctx, 5*time.Second)
		defer done()
		reply := &dholev1.AcceptReply{}
		require.NoError(t, engineBus.Request(reqCtx, bus.SubjectAccept("run-accept", "build"),
			&dholev1.JobStatus{
				RunId:      "run-accept",
				StepId:     "build",
				Attempt:    attempt,
				FenceToken: scheduler.EncodeFence(tenantID, token),
				Phase:      dholev1.Phase_PHASE_ACCEPTED,
			}, reply), "nothing on the plane answers an engine asking to accept a dispatch")
		return reply
	}

	require.Equal(t, dholev1.Acceptance_ACCEPTANCE_FENCED, ask(1, lost).GetAcceptance(),
		"the plane confirmed a dispatch whose attempt was superseded")
	require.Equal(t, dholev1.Acceptance_ACCEPTANCE_CURRENT, ask(2, current).GetAcceptance(),
		"the plane refused the current dispatch of the step")

	waiting, err := leases.Unaccepted(ctx)
	require.NoError(t, err)
	for _, w := range waiting {
		require.False(t, w.RunID == "run-accept" && w.Fence == current.Fence,
			"a CURRENT answer did not accept the lease: the engine is running a step whose offer never expires")
	}
}

// TestAPlaneThatAnswersAcceptanceSaysSoInItsDispatches: the flag is what lets a
// new engine ask only a plane that answers, so a plane that serves
// job.accept.* must set it on what it dispatches — or no engine ever asks and
// the superseded dispatch runs exactly as before.
func TestAPlaneThatAnswersAcceptanceSaysSoInItsDispatches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)
	observer, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(observer.Close)

	seen := make(chan *dholev1.JobDispatch, 16)
	stop, err := observer.SubscribeEphemeral(ctx, "job.dispatch.>", func(data []byte) {
		d := &dholev1.JobDispatch{}
		if proto.Unmarshal(data, d) != nil {
			return
		}
		select {
		case seen <- d:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(stop)

	runID, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	require.True(t, awaitDispatch(ctx, t, seen, runID, "a").GetConfirmAcceptance(),
		"a plane that answers acceptance requests dispatched without confirm_acceptance, so no engine asks")
	awaitRunCompleted(ctx, t, srv, runID)
}

// TestAHeartbeatNamingASupersededFenceIsAnsweredWithACancel is the running
// half. An engine that confirmed a dispatch and started it can still be
// overtaken — its lease expired while it was partitioned, say — and the only
// thing it says afterwards is its heartbeat. A heartbeat naming a fence the
// lease refuses is answered on the engine's control subject with the Cancel an
// operator's cancel sends, under the fence the engine holds, which every engine
// already obeys. A heartbeat naming the CURRENT fence is not.
func TestAHeartbeatNamingASupersededFenceIsAnsweredWithACancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)
	conn, err := nats.Connect(srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	lost, err := leases.Offer(ctx, tenantID, "run-overtaken", "build", 1, time.Minute)
	require.NoError(t, err)
	current, err := leases.Offer(ctx, tenantID, "run-overtaken", "build", 2, time.Minute)
	require.NoError(t, err)

	observer, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(observer.Close)
	var (
		mu       sync.Mutex
		controls = map[string][]*dholev1.Cancel{}
	)
	for _, id := range []string{"engine-overtaken", "engine-current"} {
		stop, err := observer.SubscribeEphemeral(ctx, bus.SubjectEngineControl(id), func(raw []byte) {
			msg := &dholev1.EngineControl{}
			if proto.Unmarshal(raw, msg) != nil || msg.GetCancel() == nil {
				return
			}
			mu.Lock()
			controls[id] = append(controls[id], msg.GetCancel())
			mu.Unlock()
		})
		require.NoError(t, err)
		t.Cleanup(stop)
	}
	cancels := func(id string) []*dholev1.Cancel {
		mu.Lock()
		defer mu.Unlock()
		return append([]*dholev1.Cancel(nil), controls[id]...)
	}

	engineBus, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(engineBus.Close)
	beat := func(engineID string, attempt uint32, token lease.Token) {
		require.NoError(t, engineBus.Publish(ctx, bus.SubjectEngineHeartbeat(engineID),
			wire.FrameHeartbeat(&dholev1.EngineHeartbeat{
				EngineId: engineID,
				InFlight: []*dholev1.InFlight{{
					RunId:      "run-overtaken",
					StepId:     "build",
					Attempt:    attempt,
					FenceToken: scheduler.EncodeFence(tenantID, token),
				}},
			})))
	}
	beat("engine-current", 2, current)
	beat("engine-overtaken", 1, lost)

	require.Eventually(t, func() bool { return len(cancels("engine-overtaken")) > 0 },
		engine.HeartbeatInterval*2, 50*time.Millisecond,
		"a heartbeat naming a superseded fence was not answered with a Cancel: "+
			"the overtaken attempt runs on for nobody")
	got := cancels("engine-overtaken")[0]
	require.Equal(t, "run-overtaken", got.GetRunId())
	require.Equal(t, "build", got.GetStepId())
	require.Equal(t, uint32(1), got.GetAttempt())
	require.Equal(t, scheduler.EncodeFence(tenantID, lost), got.GetFenceToken(),
		"the Cancel must carry the fence the engine holds, or the engine refuses it")

	time.Sleep(time.Second)
	require.Empty(t, cancels("engine-current"),
		"the plane cancelled the attempt that holds the current lease")
}

// TestARunningAttemptWhoseLeaseIsSupersededIsCancelledWithinAHeartbeat runs the
// whole path against the plane's own engine: a real step is running, its lease
// is superseded underneath it, and the engine's next heartbeat has to end in
// that attempt reporting CANCELLED under its own fence — not in the step
// running to completion for nobody.
func TestARunningAttemptWhoseLeaseIsSupersededIsCancelledWithinAHeartbeat(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)
	conn, err := nats.Connect(srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	leases, err := lease.New(ctx, conn)
	require.NoError(t, err)

	observer, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(observer.Close)
	statuses := make(chan *dholev1.JobStatus, 64)
	stop, err := observer.SubscribeEphemeral(ctx, "job.status.>", func(raw []byte) {
		st := &dholev1.JobStatus{}
		if proto.Unmarshal(raw, st) != nil {
			return
		}
		select {
		case statuses <- st:
		default:
		}
	})
	require.NoError(t, err)
	t.Cleanup(stop)

	runID, err := srv.Submit(ctx, tenantID, sleeper("overtaken-while-running"))
	require.NoError(t, err)

	var accepted *dholev1.JobStatus
	for accepted == nil {
		select {
		case st := <-statuses:
			if st.GetRunId() == runID && st.GetPhase() == dholev1.Phase_PHASE_ACCEPTED {
				accepted = st
			}
		case <-ctx.Done():
			t.Fatal("the plane's engine never accepted the sleeping step")
		}
	}

	// The attempt is running. Supersede it, as a loss and re-dispatch does.
	_, err = leases.Offer(ctx, tenantID, runID, accepted.GetStepId(), accepted.GetAttempt()+1, time.Minute)
	require.NoError(t, err)
	superseded := time.Now()

	// Two heartbeat intervals: the next beat, plus the time to act on it.
	deadline := time.After(2*engine.HeartbeatInterval + 2*time.Second)
	for {
		select {
		case st := <-statuses:
			if st.GetRunId() != runID || st.GetFenceToken() != accepted.GetFenceToken() {
				continue
			}
			switch st.GetPhase() {
			case dholev1.Phase_PHASE_CANCELLED:
				t.Logf("superseded attempt cancelled %s after its lease was", time.Since(superseded).Round(time.Millisecond))
				return
			case dholev1.Phase_PHASE_SUCCEEDED, dholev1.Phase_PHASE_FAILED:
				t.Fatalf("the superseded attempt ended %s rather than being cancelled", st.GetPhase())
			case dholev1.Phase_PHASE_UNSPECIFIED, dholev1.Phase_PHASE_ACCEPTED, dholev1.Phase_PHASE_RUNNING:
			}
		case <-deadline:
			t.Fatal("a running attempt whose lease was superseded was still running two heartbeats later: " +
				"its sandbox runs on for nobody")
		}
	}
}
