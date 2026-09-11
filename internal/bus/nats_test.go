package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"google.golang.org/protobuf/proto"
)

// TestEmbeddedBusRoundTripsRequestReply is the smallest proof the embedded bus
// is a real bus: a responder on engine.control.e1 answers a request from
// another connection, end to end, in-process.
func TestEmbeddedBusRoundTripsRequestReply(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	engine, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(engine.Close)

	stop, err := engine.Respond(ctx, bus.SubjectEngineControl("e1"), func([]byte) (proto.Message, error) {
		return &dholev1.EngineHeartbeat{EngineId: "e1"}, nil
	})
	require.NoError(t, err)
	t.Cleanup(stop)

	plane, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)

	var reply dholev1.EngineHeartbeat
	require.NoError(t, plane.Request(ctx, bus.SubjectEngineControl("e1"),
		&dholev1.EngineControl{}, &reply))
	require.Equal(t, "e1", reply.GetEngineId())
}

// TestPullConsumerRedeliversUnackedMessage is the property the whole engine
// protocol leans on. An engine that dies mid-step never acknowledges its
// dispatch, and the work must come back to somebody else. Closing the
// subscription without acking stands in for the engine dying.
func TestPullConsumerRedeliversUnackedMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	conn, err := bus.Connect(ctx, srv.URL(), bus.WithAckWait(500*time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	subject := bus.SubjectDispatch("untrusted", "abc")
	require.NoError(t, conn.EnsureWorkQueue(ctx, "DISPATCH", []string{"job.dispatch.>"}))

	dispatch := &dholev1.JobDispatch{RunId: "run-1", StepId: "build", FenceToken: "f1"}
	require.NoError(t, conn.Publish(ctx, subject, dispatch))

	// First delivery: received and deliberately NOT acknowledged.
	first, err := conn.SubscribePull(ctx, "DISPATCH", "engines", subject)
	require.NoError(t, err)
	msg, err := first.Next(ctx)
	require.NoError(t, err)
	var got dholev1.JobDispatch
	require.NoError(t, proto.Unmarshal(msg.Data(), &got))
	require.Equal(t, "build", got.GetStepId())
	require.NoError(t, first.Close())

	// The engine is gone. The same message must come back to the next one.
	second, err := conn.SubscribePull(ctx, "DISPATCH", "engines", subject)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	redelivered, err := second.Next(ctx)
	require.NoError(t, err)
	var again dholev1.JobDispatch
	require.NoError(t, proto.Unmarshal(redelivered.Data(), &again))
	require.Equal(t, "run-1", again.GetRunId())
	require.Equal(t, "build", again.GetStepId())
	require.Equal(t, "f1", again.GetFenceToken())
	require.NoError(t, redelivered.Ack())
}

// TestEngineCannotSubscribeToForeignTier holds the tier boundary where the wire
// contract puts it: in the bus. An engine whose credentials are scoped to the
// untrusted tier is refused by the SERVER when it reaches for trusted work — a
// Go-side filter would only be the control plane's good manners, and an engine
// written in another language, or a hostile one, would not have them.
func TestEngineCannotSubscribeToForeignTier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	srv, err := bus.StartEmbeddedWithTiers(t.TempDir(), []string{"trusted", "untrusted"})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	engine, err := bus.Connect(ctx, srv.TierURL("untrusted"))
	require.NoError(t, err)
	t.Cleanup(engine.Close)

	// Its own tier is allowed — otherwise this test would pass on a server that
	// simply refuses everything.
	stop, err := engine.SubscribeEphemeral(ctx, bus.SubjectDispatchWildcard("untrusted"), func([]byte) {})
	require.NoError(t, err)
	t.Cleanup(stop)

	// Its own tier's KIND subjects too: those are one token longer, and under
	// the old `*` wildcard the server refused an engine the very consumer that
	// routes work to its backend kind.
	stopKind, err := engine.SubscribeEphemeral(ctx,
		bus.SubjectDispatchKind("untrusted", "abc", "vm"), func([]byte) {})
	require.NoError(t, err)
	t.Cleanup(stopKind)

	_, err = engine.SubscribeEphemeral(ctx, bus.SubjectDispatchWildcard("trusted"), func([]byte) {})
	require.Error(t, err)
	require.ErrorIs(t, err, bus.ErrPermissionDenied)

	// The boundary holds for the longer subject as well: a kind token is not a
	// way around the tier.
	_, err = engine.SubscribeEphemeral(ctx,
		bus.SubjectDispatchKind("trusted", "abc", "vm"), func([]byte) {})
	require.Error(t, err)
	require.ErrorIs(t, err, bus.ErrPermissionDenied,
		"an engine must not reach another tier's work by naming a kind")
}

// TestAnEngineCanBindItsOwnKindsWorkQueueButNotAnotherTiers holds the
// permission half of kind routing on the real server. A kind-targeted subject carries one
// token more than the tier wildcard used to allow — `*` matches exactly one
// token — so an engine was refused the very consumer that routes work to its
// backend. Widening the wildcard to `>` must not widen the TIER boundary,
// which is what the second half asserts.
func TestAnEngineCanBindItsOwnKindsWorkQueueButNotAnotherTiers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbeddedWithTiers(t.TempDir(), []string{"trusted", "untrusted"})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.PlaneURL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)
	require.NoError(t, plane.EnsureWorkQueue(ctx, "DISPATCH", []string{"job.dispatch.>"}))

	engine, err := bus.Connect(ctx, srv.TierURL("untrusted"))
	require.NoError(t, err)
	t.Cleanup(engine.Close)

	own, err := engine.SubscribePull(ctx, "DISPATCH", "engines-untrusted-abc-vm",
		bus.SubjectDispatchKind("untrusted", "abc", "vm"))
	require.NoError(t, err, "an engine must be able to bind the queue for its own kind")
	t.Cleanup(func() { _ = own.Close() })

	// The tier boundary, on the subject shape that is one token longer. It is
	// asserted on a core subscription because that is where the boundary is
	// actually enforced: see the note in embedded.go on what a pull consumer's
	// filter subject is NOT checked against.
	_, err = engine.SubscribeEphemeral(ctx,
		bus.SubjectDispatchKind("trusted", "abc", "vm"), func([]byte) {})
	require.ErrorIs(t, err, bus.ErrPermissionDenied,
		"a kind token must not be a way into another tier's work")
}

// TestASubscriberOnTheUnrestrictedDispatchSubjectDoesNotReceiveKindTargetedWork
// is the assumption the whole routing fix rests on, verified rather than
// trusted: NATS subjects are token-exact unless wildcarded, so an engine bound
// to job.dispatch.<tier>.<caps> — which is every engine built before the kind
// token existed — never sees job.dispatch.<tier>.<caps>.<kind>. That is what
// makes the change backward compatible AND correct: an old engine keeps taking
// unrestricted work and is never handed work it could not honour.
func TestASubscriberOnTheUnrestrictedDispatchSubjectDoesNotReceiveKindTargetedWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	conn, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	unrestricted := make(chan string, 4)
	stop, err := conn.SubscribeEphemeral(ctx, bus.SubjectDispatch("untrusted", "abc"),
		func([]byte) { unrestricted <- "unrestricted" })
	require.NoError(t, err)
	t.Cleanup(stop)

	kinded := make(chan string, 4)
	stopKind, err := conn.SubscribeEphemeral(ctx, bus.SubjectDispatchKind("untrusted", "abc", "vm"),
		func([]byte) { kinded <- "vm" })
	require.NoError(t, err)
	t.Cleanup(stopKind)

	require.NoError(t, conn.Publish(ctx, bus.SubjectDispatchKind("untrusted", "abc", "vm"),
		&dholev1.JobDispatch{RunId: "run-1", StepId: "build"}))

	select {
	case got := <-kinded:
		require.Equal(t, "vm", got)
	case <-ctx.Done():
		t.Fatal("the kind subscriber received nothing")
	}
	require.Empty(t, unrestricted,
		"a three-token subscriber must not match a four-token subject")
}

// TestPublishToDurableSubjectReportsRefusal closes the other half of the tier
// boundary: an engine may not inject work. It also pins that a publish to a
// subject a stream covers goes through JetStream and waits for the server to
// say it stored the message. Fire-and-forget there would let Publish return
// nil for a dispatch that was never stored, and the outbox would tick the row
// off as sent.
func TestPublishToDurableSubjectReportsRefusal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbeddedWithTiers(t.TempDir(), []string{"untrusted"})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.PlaneURL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)
	require.NoError(t, plane.EnsureWorkQueue(ctx, "DISPATCH", []string{"job.dispatch.>"}))

	engine, err := bus.Connect(ctx, srv.TierURL("untrusted"))
	require.NoError(t, err)
	t.Cleanup(engine.Close)

	attempt, cancelAttempt := context.WithTimeout(ctx, 3*time.Second)
	defer cancelAttempt()
	err = engine.Publish(attempt, bus.SubjectDispatch("untrusted", "abc"),
		&dholev1.JobDispatch{RunId: "run-1", StepId: "smuggled"})
	require.Error(t, err, "an engine must not be able to enqueue its own dispatch")
}

// A subscription that outlives any deadline is the normal case, not an edge
// one: the run view tails a step's log for as long as a person watches it, and
// the request context behind that has no deadline at all. Confirming the
// subscription used to flush on the CALLER's context, and the NATS client
// refuses a deadline-free context outright — so every live log subscription in
// the product failed with "context requires a deadline" and the run view
// silently fell back to no log.
func TestSubscribingWithADeadlinelessContextWorks(t *testing.T) {
	// Deliberately cancel-only: no deadline, which is what an HTTP request
	// context is.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer dialCancel()
	conn, err := bus.Connect(dialCtx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	got := make(chan []byte, 1)
	stop, err := conn.SubscribeEphemeral(ctx, bus.SubjectLogs("run-1", "step-1"), func(b []byte) {
		select {
		case got <- b:
		default:
		}
	})
	require.NoError(t, err, "a subscription with no deadline was refused")
	t.Cleanup(stop)

	publisher, err := bus.Connect(dialCtx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(publisher.Close)
	require.NoError(t, publisher.Publish(ctx, bus.SubjectLogs("run-1", "step-1"),
		&dholev1.LogChunk{RunId: "run-1", StepId: "step-1"}))

	select {
	case <-got:
	case <-time.After(15 * time.Second):
		t.Fatal("the subscription reported success but delivered nothing")
	}
}

// TestATierEngineMayRedeemASecretButNotAnswerOne is the permission the
// redemption subject needs and the one it must not have. An engine requests on
// secret.redeem; the control plane answers. An engine allowed to SUBSCRIBE
// there could answer a sibling's redemption with a value of its own choosing,
// which is a credential-substitution attack inside the tier the bus exists to
// contain.
func TestATierEngineMayRedeemASecretButNotAnswerOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbeddedWithTiers(t.TempDir(), []string{"untrusted"})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.PlaneURL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)

	stop, err := plane.RespondRaw(ctx, bus.SubjectSecretRedeem(), func(req []byte) []byte {
		return []byte("value-for-" + string(req))
	})
	require.NoError(t, err)
	t.Cleanup(stop)

	engine, err := bus.Connect(ctx, srv.TierURL("untrusted"))
	require.NoError(t, err)
	t.Cleanup(engine.Close)

	reply, err := engine.RequestRaw(ctx, bus.SubjectSecretRedeem(), []byte("handle-1"))
	require.NoError(t, err)
	require.Equal(t, "value-for-handle-1", string(reply))

	_, err = engine.SubscribeEphemeral(ctx, bus.SubjectSecretRedeem(), func([]byte) {})
	require.Error(t, err, "an engine that could answer redemptions could substitute a value")
	require.ErrorIs(t, err, bus.ErrPermissionDenied)
}
