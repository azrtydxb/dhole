package bus_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
)

// Subscribe permissions are enforced on a core SUB, and a JetStream pull
// consumer does not use one: its delivery goes through the JS API, so the
// allow-list that refuses `job.dispatch.trusted.>` on a SUB never sees the
// consumer's FILTER SUBJECT.
//
// That makes tier isolation application-level rather than bus-level, which is
// the opposite of what [S-5] claims: "refused at the bus subject level rather
// than by application code". An untrusted engine binding a durable filtered to
// the trusted tier receives trusted work.
func TestAnEngineCannotBindAWorkQueueFilteredToAnotherTier(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, err := bus.StartEmbeddedWithTiers(t.TempDir(), []string{"trusted", "untrusted"})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	// The stream is the control plane's to declare, so stand in for it.
	plane, err := bus.Connect(ctx, srv.PlaneURL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)
	require.NoError(t, plane.EnsureDispatchStreams(ctx, []string{"trusted", "untrusted"}))

	intruder, err := bus.Connect(ctx, srv.TierURL("untrusted"))
	require.NoError(t, err)
	t.Cleanup(intruder.Close)

	// Its own tier first, so a server that refused everything could not pass
	// this test by accident.
	own, err := intruder.SubscribePull(ctx, dispatchStream(t, "untrusted"), "own-tier-probe",
		bus.SubjectDispatchWildcard("untrusted"))
	require.NoError(t, err, "an engine was refused its own tier's work queue")
	t.Cleanup(func() { _ = own.Close() })

	// And now the boundary itself.
	foreign, err := intruder.SubscribePull(ctx, dispatchStream(t, "trusted"), "foreign-tier-probe",
		bus.SubjectDispatchWildcard("trusted"))
	if err == nil {
		_ = foreign.Close()
		t.Fatal("an untrusted engine bound a work queue filtered to the trusted tier: " +
			"tier isolation is enforced on core subscriptions but not on a JetStream consumer, " +
			"so it is application-level rather than bus-level")
	}
	require.ErrorIs(t, err, bus.ErrPermissionDenied)
}

// The permission that closes the hole constrains ONE shape of consumer create:
// the one nats.go uses for a single filter subject, where the filter is a
// suffix of the JS API subject and the server refuses a body that disagrees
// with it. Three other create endpoints carry the filter in the JSON body
// ALONE, where no subject permission can see it, and any of them left granted
// would keep the hole open while the test above passed:
//
//   - `$JS.API.CONSUMER.CREATE.<stream>.<consumer>` — also the only form a
//     multi-filter (FilterSubjects) create may use,
//   - `$JS.API.CONSUMER.DURABLE.CREATE.<stream>.<consumer>` — legacy durable,
//   - `$JS.API.CONSUMER.CREATE.<stream>` — legacy ephemeral.
//
// So the boundary is asserted against the API subjects themselves, not through
// the client, which would only ever exercise the one form it happens to send.
func TestAnEngineCannotReachTheConsumerCreateEndpointsThatHideTheFilterSubject(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srv, err := bus.StartEmbeddedWithTiers(t.TempDir(), []string{"trusted", "untrusted"})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.PlaneURL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)
	require.NoError(t, plane.EnsureDispatchStreams(ctx, []string{"trusted", "untrusted"}))

	// The server answers a forbidden PUBLISH with an -ERR on the connection
	// and never replies to the request, so the refusal is read from the async
	// handler. LastError would not do: it is sticky, and the FIRST violation
	// would stand in for every case after it.
	violations := make(chan error, 16)
	raw, err := nats.Connect(srv.TierURL("untrusted"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, handlerErr error) {
			select {
			case violations <- handlerErr:
			default:
			}
		}))
	require.NoError(t, err)
	t.Cleanup(raw.Close)

	// The target is the TRUSTED tier's stream: an untrusted connection reaching
	// these endpoints on its own stream would prove nothing about the tier.
	target := dispatchStream(t, "trusted")

	body := func(name string, cfg map[string]any) []byte {
		cfg["durable_name"] = name
		cfg["name"] = name
		cfg["ack_policy"] = "explicit"
		encoded, marshalErr := json.Marshal(map[string]any{
			"stream_name": target,
			"config":      cfg,
		})
		require.NoError(t, marshalErr)
		return encoded
	}

	for _, tc := range []struct {
		name    string
		subject string
		payload []byte
	}{
		{
			name:    "the new endpoint with no filter token",
			subject: "$JS.API.CONSUMER.CREATE." + target + ".body-filter",
			payload: body("body-filter", map[string]any{
				"filter_subject": bus.SubjectDispatchWildcard("trusted"),
			}),
		},
		{
			name:    "a multi-filter create, which cannot name its filters in the subject",
			subject: "$JS.API.CONSUMER.CREATE." + target + ".multi-filter",
			payload: body("multi-filter", map[string]any{
				"filter_subjects": []string{bus.SubjectDispatchWildcard("trusted")},
			}),
		},
		{
			name:    "the legacy durable endpoint",
			subject: "$JS.API.CONSUMER.DURABLE.CREATE." + target + ".legacy-durable",
			payload: body("legacy-durable", map[string]any{
				"filter_subject": bus.SubjectDispatchWildcard("trusted"),
			}),
		},
		{
			name:    "the legacy ephemeral endpoint",
			subject: "$JS.API.CONSUMER.CREATE." + target,
			payload: body("", map[string]any{
				"filter_subject": bus.SubjectDispatchWildcard("trusted"),
			}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, reqErr := raw.Request(tc.subject, tc.payload, requestWait)
			require.Error(t, reqErr,
				"an untrusted engine reached %s, where the filter subject is invisible to a permission", tc.subject)

			// The refusal has to be the server's, not a stream that happens
			// not to exist: a create that got through and failed for its own
			// reasons would leave the boundary open to a well-formed one.
			select {
			case violation := <-violations:
				require.ErrorIs(t, violation, nats.ErrPermissionViolation)
				require.Contains(t, violation.Error(), tc.subject)
			case <-time.After(requestWait):
				t.Fatalf("the server did not refuse %s on permissions", tc.subject)
			}
		})
	}

	// Nothing was created on the way past. The stream is the plane's, so ask
	// it rather than the connection that was refused.
	listCtx, cancelList := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelList()
	planeJS, err := jetstream.New(planeConn(t, srv))
	require.NoError(t, err)
	stream, err := planeJS.Stream(listCtx, target)
	require.NoError(t, err)
	names := stream.ConsumerNames(listCtx)
	for name := range names.Name() {
		t.Fatalf("an untrusted engine created the consumer %q on the trusted tier's stream", name)
	}
	require.NoError(t, names.Err())
}

// requestWait bounds one refused JS API request. A refused publish is never
// answered, so every case here waits out this timeout rather than returning:
// long enough for a real reply, short enough that four of them are not a test
// run's worth of waiting.
const requestWait = 2 * time.Second

// planeConn dials the control plane's credentials for an assertion that has to
// look at the server's own view rather than the refused connection's.
func planeConn(t *testing.T, srv *bus.Embedded) *nats.Conn {
	t.Helper()
	conn, err := nats.Connect(srv.PlaneURL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	return conn
}

// TestAnEngineCannotPullFromAConsumerAnotherTiersEngineCreated is the attack
// the consumer-create permission does not touch: the intruder CREATES nothing.
// It waits for a trusted engine to bind its queue and then publishes to
// `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>` with that consumer's name,
// which is guessable — every engine in a tier binds `engines-<tier>-<caps>`.
//
// No permission can narrow MSG.NEXT by consumer NAME: a NATS wildcard matches
// a whole token, so `engines-untrusted-*` is not expressible. The tier has to
// be in the STREAM token instead, which is why the dispatch stream is one per
// tier rather than one shared `DISPATCH`.
func TestAnEngineCannotPullFromAConsumerAnotherTiersEngineCreated(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srv, err := bus.StartEmbeddedWithTiers(t.TempDir(), []string{"trusted", "untrusted"})
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	plane, err := bus.Connect(ctx, srv.PlaneURL())
	require.NoError(t, err)
	t.Cleanup(plane.Close)
	require.NoError(t, plane.EnsureDispatchStreams(ctx, []string{"trusted", "untrusted"}))

	// A trusted engine binds its queue, exactly as the agent does.
	trusted, err := bus.Connect(ctx, srv.TierURL("trusted"))
	require.NoError(t, err)
	t.Cleanup(trusted.Close)
	victim, err := trusted.SubscribePull(ctx, dispatchStream(t, "trusted"), "engines-trusted-abc",
		bus.SubjectDispatch("trusted", "abc"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = victim.Close() })

	require.NoError(t, plane.Publish(ctx, bus.SubjectDispatch("trusted", "abc"),
		&dholev1.JobDispatch{RunId: "run-1", StepId: "secret-work"}))

	violations := make(chan error, 4)
	raw, err := nats.Connect(srv.TierURL("untrusted"),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, handlerErr error) {
			select {
			case violations <- handlerErr:
			default:
			}
		}))
	require.NoError(t, err)
	t.Cleanup(raw.Close)

	next := "$JS.API.CONSUMER.MSG.NEXT." + dispatchStream(t, "trusted") + ".engines-trusted-abc"
	stolen, err := raw.Request(next, []byte(`{"batch":1,"expires":1000000000}`), requestWait)
	if err == nil {
		t.Fatalf("an untrusted connection pulled %q off the trusted tier's consumer by naming it",
			string(stolen.Data))
	}

	select {
	case violation := <-violations:
		require.ErrorIs(t, violation, nats.ErrPermissionViolation)
		require.Contains(t, violation.Error(), next)
	case <-time.After(requestWait):
		t.Fatalf("the server did not refuse %s on permissions", next)
	}

	// The work is still there for the engine it was dispatched to.
	victimCtx, cancelVictim := context.WithTimeout(ctx, 20*time.Second)
	defer cancelVictim()
	msg, err := victim.Next(victimCtx)
	require.NoError(t, err, "the dispatch did not survive the attempt to steal it")
	require.NoError(t, msg.Ack())
}
