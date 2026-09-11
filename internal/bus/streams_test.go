package bus_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// A stream name is not a subject. `local/dev` is a perfectly legal subject
// token and an illegal JetStream stream name (nats-server refuses '.', '*',
// '>', '\\' and '/'), so now that the tier is spelled into a stream name the
// token rule alone is not enough. The refusal has to happen where the tier is
// CONFIGURED and name the tier: found at the first dispatch instead, it is a
// stream-creation error raised from inside the scheduler, with the tier
// nowhere in the message.
func TestATierThatCannotBeAStreamNameIsRefusedWhereItIsConfigured(t *testing.T) {
	for _, tier := range []string{"local/dev", "back\\slash", "two.tokens", "*", ">", "with space", ""} {
		t.Run(tier, func(t *testing.T) {
			_, err := bus.DispatchStreamName(tier)
			require.Error(t, err, "a tier that cannot be a stream name was accepted")

			_, err = bus.TierPermissions(tier)
			require.Error(t, err, "a tier that cannot be a stream name got permissions")
			if tier != "" {
				// %q escapes, so compare against the quoted spelling: what
				// matters is that the operator can see WHICH tier is bad.
				quoted := strconv.Quote(tier)
				require.Contains(t, err.Error(), quoted[1:len(quoted)-1],
					"the refusal must name the tier")
			}

			_, err = bus.StartEmbeddedWithTiers(t.TempDir(), []string{tier})
			require.Error(t, err, "a server started with a tier it cannot name a stream for")
		})
	}
}

// The name itself, pinned: an engine written in another language builds this
// string from its own tier, and a change here is a change to that contract.
func TestTheDispatchStreamNameCarriesTheTier(t *testing.T) {
	name, err := bus.DispatchStreamName("untrusted")
	require.NoError(t, err)
	require.Equal(t, "DISPATCH_untrusted", name)
}

// A pre-split `DISPATCH` covers `job.dispatch.>`, which overlaps every
// per-tier stream, and JetStream refuses two streams over overlapping
// subjects — so an upgrade has to do something about it rather than step
// around it. Empty, it is only an obstacle and is removed.
func TestAnEmptyLegacyDispatchStreamIsRemovedSoTheTiersStreamsCanExist(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	conn, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	require.NoError(t, conn.EnsureWorkQueue(ctx, bus.LegacyDispatchStream, []string{"job.dispatch.>"}))
	require.NoError(t, conn.EnsureDispatchStreams(ctx, []string{"trusted", "untrusted"}))

	js, err := jetstream.New(rawConn(t, srv.URL()))
	require.NoError(t, err)
	_, err = js.Stream(ctx, bus.LegacyDispatchStream)
	require.ErrorIs(t, err, jetstream.ErrStreamNotFound,
		"the empty pre-split stream was left behind, and its subjects overlap every tier's")
	for _, tier := range []string{"trusted", "untrusted"} {
		name, nameErr := bus.DispatchStreamName(tier)
		require.NoError(t, nameErr)
		_, streamErr := js.Stream(ctx, name)
		require.NoError(t, streamErr, "the tier's stream was not created")
	}
}

// Non-empty, it is the opposite call. Those messages are dispatches that steps
// of live runs are waiting on; deleting them to make room would strand every
// one of those runs, silently, and the plane would come up looking healthy.
// So the plane refuses to start and says what is in the way.
func TestALegacyDispatchStreamHoldingWorkStopsTheUpgradeRatherThanLosingIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	conn, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	require.NoError(t, conn.EnsureWorkQueue(ctx, bus.LegacyDispatchStream, []string{"job.dispatch.>"}))
	require.NoError(t, conn.Publish(ctx, bus.SubjectDispatch("trusted", "abc"),
		&dholev1.JobDispatch{RunId: "run-1", StepId: "in-flight"}))

	err = conn.EnsureDispatchStreams(ctx, []string{"trusted"})
	require.Error(t, err, "the upgrade discarded work that was already queued")
	require.Contains(t, err.Error(), bus.LegacyDispatchStream)
	require.Contains(t, err.Error(), "1 dispatch")

	// And the work is still there to be drained.
	js, err := jetstream.New(rawConn(t, srv.URL()))
	require.NoError(t, err)
	stream, err := js.Stream(ctx, bus.LegacyDispatchStream)
	require.NoError(t, err)
	info, err := stream.Info(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), info.State.Msgs)
}

// rawConn dials a bus for an assertion that has to look at JetStream itself
// rather than through the Bus interface, which deliberately says nothing about
// streams.
func rawConn(t *testing.T, url string) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}
