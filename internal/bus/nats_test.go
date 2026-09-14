package bus_test

import (
	"context"
	"errors"
	"sync"
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
	require.NoError(t, conn.EnsureDispatchStreams(ctx, []string{"untrusted"}))

	dispatch := &dholev1.JobDispatch{RunId: "run-1", StepId: "build", FenceToken: "f1"}
	require.NoError(t, conn.Publish(ctx, subject, dispatch))

	// First delivery: received and deliberately NOT acknowledged.
	first, err := conn.SubscribePull(ctx, dispatchStream(t, "untrusted"), "engines", subject)
	require.NoError(t, err)
	msg, err := first.Next(ctx)
	require.NoError(t, err)
	var got dholev1.JobDispatch
	require.NoError(t, proto.Unmarshal(msg.Data(), &got))
	require.Equal(t, "build", got.GetStepId())
	require.NoError(t, first.Close())

	// The engine is gone. The same message must come back to the next one.
	second, err := conn.SubscribePull(ctx, dispatchStream(t, "untrusted"), "engines", subject)
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

	srv, err := bus.StartEmbeddedWithTiers(t.TempDir(), "acme", []string{"trusted", "untrusted"})
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

	srv, err := bus.StartEmbeddedWithTiers(t.TempDir(), "acme", []string{"trusted", "untrusted"})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.PlaneURL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)
	require.NoError(t, plane.EnsureDispatchStreams(ctx, []string{"trusted", "untrusted"}))

	engine, err := bus.Connect(ctx, srv.TierURL("untrusted"))
	require.NoError(t, err)
	t.Cleanup(engine.Close)

	own, err := engine.SubscribePull(ctx, dispatchStream(t, "untrusted"), "engines-untrusted-abc-vm",
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

	srv, err := bus.StartEmbeddedWithTiers(t.TempDir(), "acme", []string{"untrusted"})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.PlaneURL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)
	require.NoError(t, plane.EnsureDispatchStreams(ctx, []string{"untrusted"}))

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
// its tenant's secret.redeem.<tenant>; the control plane answers. An engine
// allowed to SUBSCRIBE there could answer a sibling's redemption with a value
// of its own choosing, which is a credential-substitution attack inside the
// tier the bus exists to contain.
func TestATierEngineMayRedeemASecretButNotAnswerOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbeddedWithTiers(t.TempDir(), "acme", []string{"untrusted"})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.PlaneURL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)

	stop, err := plane.RespondRawSubject(ctx, bus.SubjectSecretRedeemAny(), func(subject string, req []byte) []byte {
		return []byte("value-for-" + string(req) + "-on-" + subject)
	})
	require.NoError(t, err)
	t.Cleanup(stop)

	engine, err := bus.Connect(ctx, srv.TierURL("untrusted"))
	require.NoError(t, err)
	t.Cleanup(engine.Close)

	reply, err := engine.RequestRaw(ctx, bus.SubjectSecretRedeemFor("acme"), []byte("handle-1"))
	require.NoError(t, err)
	require.Equal(t, "value-for-handle-1-on-secret.redeem.acme", string(reply),
		"the responder must learn the tenant from the subject the request arrived on")

	for _, subject := range []string{bus.SubjectSecretRedeemAny(), bus.SubjectSecretRedeemFor("acme")} {
		_, err = engine.SubscribeEphemeral(ctx, subject, func([]byte) {})
		require.Error(t, err, "an engine that could answer redemptions on %s could substitute a value", subject)
		require.ErrorIs(t, err, bus.ErrPermissionDenied)
	}
}

// TestATierEngineMayRedeemOnlyOnItsOwnTenantsSubject is ADR 0028's bus half. A
// tier credential belongs to one tenant, so the one redemption subject it may
// request on is that tenant's: an engine presenting a handle it obtained from
// another tenant is refused by the SERVER before any responder sees it.
func TestATierEngineMayRedeemOnlyOnItsOwnTenantsSubject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbeddedWithTiers(t.TempDir(), "acme", []string{"untrusted"})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.PlaneURL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)

	var (
		mu   sync.Mutex
		seen []string
	)
	stop, err := plane.RespondRawSubject(ctx, bus.SubjectSecretRedeemAny(), func(subject string, _ []byte) []byte {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, subject)
		return []byte("value")
	})
	require.NoError(t, err)
	t.Cleanup(stop)

	engine, err := bus.Connect(ctx, srv.TierURL("untrusted"))
	require.NoError(t, err)
	t.Cleanup(engine.Close)

	_, err = engine.RequestRaw(ctx, bus.SubjectSecretRedeemFor("acme"), []byte("mine"))
	require.NoError(t, err, "an engine was refused redemption on its own tenant's subject")

	short, stopShort := context.WithTimeout(ctx, 2*time.Second)
	defer stopShort()
	_, err = engine.RequestRaw(short, bus.SubjectSecretRedeemFor("globex"), []byte("theirs"))
	require.Error(t, err, "an engine of tenant acme redeemed on tenant globex's subject")

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{bus.SubjectSecretRedeemFor("acme")}, seen,
		"a request on another tenant's redemption subject reached the responder")
}

// dispatchStream names one tier's work queue, failing the test rather than
// returning an error for a tier a test spelled wrong.
func dispatchStream(t *testing.T, tier string) string {
	t.Helper()
	name, err := bus.DispatchStreamName(tier)
	require.NoError(t, err)
	return name
}

// TestAFetchItsCallerAbandonedGivesALateMessageBackRatherThanSittingOnIt. A
// pull request outlives the Next that made it: the server holds it open for the
// fetch wait, and a message published in that window is delivered to it
// whether or not anybody is still listening. An engine abandons fetches on
// purpose — it stops fetching the moment its last slot is taken, so it does
// not hold work another engine could run — and a message that lands on an
// abandoned fetch must go back to the queue at once.
//
// Two failures, both real. Ignoring the context returns the message to a caller
// that has already decided it cannot run it. Honouring the context and simply
// walking away leaves the message delivered to nobody until the ack wait
// expires — thirty seconds by default, which is the lease the plane declares
// the dispatch lost at.
func TestAFetchItsCallerAbandonedGivesALateMessageBackRatherThanSittingOnIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	// The DEFAULT ack wait: a message stranded on an abandoned fetch would
	// not come back within this test's patience.
	conn, err := bus.Connect(ctx, srv.URL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	subject := bus.SubjectDispatch("untrusted", "abc")
	require.NoError(t, conn.EnsureDispatchStreams(ctx, []string{"untrusted"}))

	first, err := conn.SubscribePull(ctx, dispatchStream(t, "untrusted"), "engines", subject)
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })

	abandoned := make(chan error, 1)
	returned := make(chan time.Duration, 1)
	go func() {
		waitCtx, stop := context.WithTimeout(ctx, 200*time.Millisecond)
		defer stop()
		began := time.Now()
		msg, err := first.Next(waitCtx)
		returned <- time.Since(began)
		if err == nil {
			_ = msg.Nak()
			err = errors.New("Next handed a message to a caller whose context had already ended")
		}
		abandoned <- err
	}()

	// Published after the caller gave up, while its pull request is still open
	// on the server.
	time.Sleep(600 * time.Millisecond)
	require.NoError(t, conn.Publish(ctx, subject, &dholev1.JobDispatch{RunId: "run-late", StepId: "build"}))

	require.ErrorIs(t, <-abandoned, context.DeadlineExceeded)
	require.Less(t, <-returned, time.Second, "Next must return when its caller's context ends, not at the fetch wait")

	second, err := conn.SubscribePull(ctx, dispatchStream(t, "untrusted"), "engines", subject)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })

	waitCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	msg, err := second.Next(waitCtx)
	require.NoError(t, err, "the message that landed on the abandoned fetch was not given back")
	var got dholev1.JobDispatch
	require.NoError(t, proto.Unmarshal(msg.Data(), &got))
	require.Equal(t, "run-late", got.GetRunId())
	require.NoError(t, msg.Ack())
}
