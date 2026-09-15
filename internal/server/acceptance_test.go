package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/lease"
	"github.com/azrtydxb/dhole/internal/scheduler"
)

// One plane answers each acceptance request (ADR 0033).
//
// An engine asks on job.accept.<run>.<step> before it starts a dispatch, and
// the answer is a lease Renew. Served with a plain subscription, every plane on
// the bus received every request and renewed the lease once each: three planes,
// three writes for one question, and three replies of which the engine reads
// the first.

func acceptanceBus(ctx context.Context, t *testing.T) (*bus.Embedded, func() *bus.NATS) {
	t.Helper()
	srv, err := bus.StartEmbedded(filepath.Join(t.TempDir(), "nats"))
	require.NoError(t, err)
	t.Cleanup(srv.Close)
	return srv, func() *bus.NATS {
		conn, err := bus.Connect(ctx, srv.URL())
		require.NoError(t, err)
		t.Cleanup(conn.Close)
		return conn
	}
}

func askAcceptance(ctx context.Context, t *testing.T, engine *bus.NATS, fence string) dholev1.Acceptance {
	t.Helper()
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	reply := &dholev1.AcceptReply{}
	require.NoError(t, engine.Request(reqCtx, bus.SubjectAccept("run-1", "build"), &dholev1.JobStatus{
		RunId: "run-1", StepId: "build", Attempt: 1, FenceToken: fence, Phase: dholev1.Phase_PHASE_ACCEPTED,
	}, reply))
	require.Empty(t, reply.GetError())
	return reply.GetAcceptance()
}

func TestEachAcceptanceRequestIsAnsweredByOnePlane(t *testing.T) {
	ctx := testCtx(t)
	_, connect := acceptanceBus(ctx, t)

	var renewals atomic.Int64
	accept := func(context.Context, *dholev1.JobStatus) (dholev1.Acceptance, error) {
		renewals.Add(1)
		return dholev1.Acceptance_ACCEPTANCE_CURRENT, nil
	}
	const planes = 3
	for range planes {
		stop, err := serveAcceptanceOn(ctx, ctx, connect(), accept, slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		t.Cleanup(stop)
	}

	engine := connect()
	const asked = 20
	for i := range asked {
		require.Equal(t, dholev1.Acceptance_ACCEPTANCE_CURRENT, askAcceptance(ctx, t, engine, fmt.Sprint(i)))
	}
	// Every plane that received a request has answered it by the time a later
	// request is answered, since each connection handles its requests in order;
	// a flush of every plane's connection is not needed to count them.
	require.Eventually(t, func() bool { return renewals.Load() >= asked }, 5*time.Second, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(asked), renewals.Load(),
		"every plane answered every acceptance request: one lease renewal per plane for each question an engine asked")
}

// TestAnOlderPlaneBesideANewerOneStillAnswersEachFenceByItsLease is the rolling
// upgrade of the change above. A plane from before it still subscribes plainly
// and receives every request as well as the queue member that does; the engine
// reads whichever reply arrives first. That is only safe because both answers
// are the same compare against the same lease, and this holds them to it: a
// current fence is CURRENT and a superseded one FENCED, however the two planes
// split the work.
func TestAnOlderPlaneBesideANewerOneStillAnswersEachFenceByItsLease(t *testing.T) {
	ctx := testCtx(t)
	srv, connect := acceptanceBus(ctx, t)

	kvConn, err := nats.Connect(srv.URL())
	require.NoError(t, err)
	t.Cleanup(kvConn.Close)
	kv, err := lease.New(ctx, kvConn)
	require.NoError(t, err)

	superseded, err := kv.Offer(ctx, claimTestTenant, "run-1", "build", 1, time.Minute)
	require.NoError(t, err)
	current, err := kv.Offer(ctx, claimTestTenant, "run-1", "build", 2, time.Minute)
	require.NoError(t, err)

	var byOld, byNew atomic.Int64
	byLease := func(count *atomic.Int64) acceptFunc {
		return func(ctx context.Context, st *dholev1.JobStatus) (dholev1.Acceptance, error) {
			count.Add(1)
			_, token, err := scheduler.DecodeFence(st.GetFenceToken())
			if err != nil {
				return dholev1.Acceptance_ACCEPTANCE_UNSPECIFIED, err
			}
			switch err := kv.Renew(ctx, token); {
			case errors.Is(err, lease.ErrFenced):
				return dholev1.Acceptance_ACCEPTANCE_FENCED, nil
			case err != nil:
				return dholev1.Acceptance_ACCEPTANCE_UNSPECIFIED, err
			}
			return dholev1.Acceptance_ACCEPTANCE_CURRENT, nil
		}
	}

	// The older plane: a plain subscription, as ADR 0029 shipped it.
	older := connect()
	stopOld, err := older.Respond(ctx, bus.SubjectAcceptWildcard(), func(raw []byte) (proto.Message, error) {
		st := &dholev1.JobStatus{}
		if err := proto.Unmarshal(raw, st); err != nil {
			return nil, err
		}
		verdict, err := byLease(&byOld)(ctx, st)
		if err != nil {
			return &dholev1.AcceptReply{Error: err.Error()}, nil
		}
		return &dholev1.AcceptReply{Acceptance: verdict}, nil
	})
	require.NoError(t, err)
	t.Cleanup(stopOld)
	for range 2 {
		stop, err := serveAcceptanceOn(ctx, ctx, connect(), byLease(&byNew), slog.New(slog.DiscardHandler))
		require.NoError(t, err)
		t.Cleanup(stop)
	}

	engine := connect()
	for range 10 {
		require.Equal(t, dholev1.Acceptance_ACCEPTANCE_CURRENT,
			askAcceptance(ctx, t, engine, scheduler.EncodeFence(claimTestTenant, current)),
			"a mixed fleet of planes refused the dispatch that holds the lease")
		require.Equal(t, dholev1.Acceptance_ACCEPTANCE_FENCED,
			askAcceptance(ctx, t, engine, scheduler.EncodeFence(claimTestTenant, superseded)),
			"a mixed fleet of planes confirmed a superseded dispatch")
	}
	require.Eventually(t, func() bool { return byOld.Load() == 20 && byNew.Load() == 20 },
		5*time.Second, 10*time.Millisecond,
		"the older plane answers every request and exactly one newer plane answers each")
	require.NoError(t, kv.Validate(ctx, current), "answering renewed nothing but the current lease")
}
