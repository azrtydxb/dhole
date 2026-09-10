package server_test

import (
	"context"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/nats-io/nats.go"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/executor/process"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/server"
	"github.com/azrtydxb/dhole/internal/wire"
)

// containerLikeEnv is the process executor with one honest change: it names
// the environment its steps run in, as a container or Kubernetes backend does
// by resolving its sandbox image to a digest.
//
// It is a stand-in for a backend this suite cannot start, and it is NOT a way
// of injecting an identity into the control plane. The plane no longer has
// anywhere to inject one: this executor belongs to the ENGINE, the engine
// announces the identity on its registration, and the scheduler reads it back
// off the tier's fleet (ADR 0021). Everything between those two points is the
// production path, including the bus.
//
// That distinction is the whole reason this file was rewritten. The identity
// used to be read off an executor the control plane held, which no deployment
// ever had — a distributed plane has no executor at all and the single binary
// has a host process one, which correctly reports it has nothing stable to
// name — so nothing was ever cached anywhere while this suite passed.
//
// The identity is a constant because this test IS the reproducible
// environment: both runs are the same process, the same binaries and the same
// /bin/sh. That is exactly the promise a container digest or a VM snapshot id
// makes for a real backend.
type containerLikeEnv struct{ *process.Executor }

func (containerLikeEnv) EnvironmentIdentity() (string, error) {
	return "sha256:test-environment-v1", nil
}

// startCachingServer is startEmbedded over a caller-owned directory and an
// engine backend that names its environment, so several runs share ONE store
// and one cache.
func startCachingServer(ctx context.Context, t *testing.T, dir string) *server.Server {
	t.Helper()
	srv, err := server.New(server.Config{
		// Port zero: these tests run beside each other, and a plane
		// bound to the well-known port would fight for a socket.
		APIAddr:  "127.0.0.1:0",
		Mode:     server.ModeEmbedded,
		StoreDSN: filepath.Join(dir, "dhole.db"),
		BlobRoot: filepath.Join(dir, "state"),
		Executor: containerLikeEnv{process.New()},
	})
	require.NoError(t, err)
	require.NoError(t, srv.Start(ctx))
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		require.NoError(t, srv.Stop(stopCtx))
	})
	return srv
}

// dispatchWatcher records every JobDispatch that reaches the bus. It is a
// plain core-NATS observer, so watching costs the work queue nothing: it
// cannot take a dispatch from the engine.
type dispatchWatcher struct {
	mu   sync.Mutex
	seen []*dholev1.JobDispatch
}

func watchDispatches(ctx context.Context, t *testing.T, srv *server.Server) *dispatchWatcher {
	t.Helper()
	observer, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(observer.Close)

	w := &dispatchWatcher{}
	stop, err := observer.SubscribeEphemeral(ctx, "job.dispatch.>", func(data []byte) {
		d := &dholev1.JobDispatch{}
		if err := proto.Unmarshal(data, d); err != nil {
			return
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		w.seen = append(w.seen, d)
	})
	require.NoError(t, err)
	t.Cleanup(stop)
	return w
}

// stepsDispatchedFor is which steps of one run were sent to an engine.
func (w *dispatchWatcher) stepsDispatchedFor(runID string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	var ids []string
	for _, d := range w.seen {
		if d.GetRunId() == runID {
			ids = append(ids, d.GetStepId())
		}
	}
	return ids
}

// warmTheFleet runs a throwaway pipeline to completion so the engine has
// registered before the runs under test are submitted.
//
// It is not ceremony. A run submitted before any engine has announced itself
// records STEP_UNSCHEDULABLE and is placed on the next tick, which would put
// an extra event in the first run's log and none in the second — and the
// claim being tested is that the two logs are the SAME.
func warmTheFleet(ctx context.Context, t *testing.T, srv *server.Server) {
	t.Helper()
	warmup := &dholev1.Pipeline{
		Id:     "warmup",
		Tenant: &dholev1.Tenant{Id: tenantID},
		Steps: []*dholev1.Step{{
			Id:          "w",
			Name:        "warm",
			PluginRef:   `command:{"args":["/bin/sh","-c","printf warm > out"]}`,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
			Outputs: []*dholev1.Port{{
				Name: "out",
				Type: &dholev1.PortType{Kind: &dholev1.PortType_Blob{
					Blob: &dholev1.BlobType{MediaType: "text/plain"},
				}},
			}},
		}},
	}
	runID, err := srv.Submit(ctx, tenantID, warmup)
	require.NoError(t, err)
	awaitRunCompleted(ctx, t, srv, runID)
}

// runTwice submits the two-step pipeline twice against one store and returns
// both logs plus everything that reached the bus.
func runTwice(ctx context.Context, t *testing.T) (*server.Server, string, []runstore.Event, string, []runstore.Event, *dispatchWatcher) {
	t.Helper()
	srv := startCachingServer(ctx, t, t.TempDir())
	warmTheFleet(ctx, t, srv)
	watcher := watchDispatches(ctx, t, srv)

	first, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	firstEvents := awaitRunCompleted(ctx, t, srv, first)

	second, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	secondEvents := awaitRunCompleted(ctx, t, srv, second)

	return srv, first, firstEvents, second, secondEvents, watcher
}

// TestSecondRunIsServedFromTheCache is the claim ADR 0009 makes and the one
// the scheduler did not honour: a pure step whose inputs, environment and
// lockfile have all been seen before is SKIPPED, and its recorded outputs are
// reused.
//
// The assertion that matters is the negative one. A run that merely completed
// again proves nothing — it would complete just as happily by executing every
// step for a second time, which is precisely the defect. So this requires that
// nothing at all was sent to an engine for the second run.
func TestSecondRunIsServedFromTheCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srv, first, firstEvents, second, secondEvents, watcher := runTwice(ctx, t)

	// The first run really ran, and really was cacheable — a run whose steps
	// were all refused by cache.Eligible would make the rest of this test
	// pass for the wrong reason.
	require.Equal(t, []string{"a", "b"}, watcher.stepsDispatchedFor(first),
		"the first run has nothing recorded to reuse, so both steps go to an engine")
	require.True(t, dispatchedPayload(t, firstEvents, "a").Cacheable,
		"step a must be cacheable, or nothing below tests the cache")
	require.False(t, dispatchedPayload(t, firstEvents, "a").CacheHit,
		"the first run cannot have hit a cache that was empty")

	require.Empty(t, watcher.stepsDispatchedFor(second),
		"the second run repeats work already recorded: no step may reach an engine")

	requireStepSucceeded(t, secondEvents, "a")
	requireStepSucceeded(t, secondEvents, "b")
	require.True(t, dispatchedPayload(t, secondEvents, "a").CacheHit,
		"step a was served from the cache and the log has to say so")

	// The reused outputs are the first run's, byte for byte: the same digests,
	// resolving to the same bytes in the content-addressed store.
	require.Equal(t, outputDigest(t, firstEvents, "b", "out"), outputDigest(t, secondEvents, "b", "out"))
	require.Equal(t,
		string(outputBytes(ctx, t, srv, firstEvents, "b", "out")),
		string(outputBytes(ctx, t, srv, secondEvents, "b", "out")))
}

// TestCachedRunProducesTheSameEventsAsARealOne is what makes a cache hit safe
// to build on. Every view of a run — the run page, the DAG, anything replaying
// the log — reads the event log and nothing else, so a step that was skipped
// has to be indistinguishable there from one that ran, apart from the recorded
// hit itself. A hit that wrote a different shape of history would make the
// cache visible in every surface that consumes it.
func TestCachedRunProducesTheSameEventsAsARealOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	_, _, firstEvents, _, secondEvents, _ := runTwice(ctx, t)

	require.Equal(t, eventShape(firstEvents), eventShape(secondEvents),
		"a cached run's log has the same events, for the same steps, in the same order")

	// And the terminal statuses carry the same outputs, so anything reading a
	// step's result finds it in the usual place.
	for _, stepID := range []string{"a", "b"} {
		require.Equal(t, succeededStatus(t, firstEvents, stepID).GetOutputs(),
			succeededStatus(t, secondEvents, stepID).GetOutputs(),
			"step %q reports the outputs it was recorded with", stepID)
	}

	// The ONE permitted difference.
	require.False(t, dispatchedPayload(t, firstEvents, "a").CacheHit)
	require.True(t, dispatchedPayload(t, secondEvents, "a").CacheHit)
}

// eventShape reduces a log to what a reader replaying it sees happen: the
// event types, in order, against the steps they belong to.
func eventShape(events []runstore.Event) []string {
	shape := make([]string, 0, len(events))
	for _, e := range events {
		shape = append(shape, string(e.Type)+"/"+e.StepID)
	}
	return shape
}

func dispatchedPayload(t *testing.T, events []runstore.Event, stepID string) scheduler.Dispatched {
	t.Helper()
	for _, e := range events {
		if e.Type != runstore.StepDispatched || e.StepID != stepID {
			continue
		}
		payload, err := scheduler.UnmarshalDispatched(e.Payload)
		require.NoError(t, err)
		return payload
	}
	t.Fatalf("step %q has no %s event; log: %s", stepID, runstore.StepDispatched, describe(events))
	return scheduler.Dispatched{}
}

func succeededStatus(t *testing.T, events []runstore.Event, stepID string) *dholev1.JobStatus {
	t.Helper()
	for _, e := range events {
		if e.Type != runstore.StepSucceeded || e.StepID != stepID {
			continue
		}
		status := &dholev1.JobStatus{}
		require.NoError(t, proto.Unmarshal(e.Payload, status))
		return status
	}
	t.Fatalf("step %q never succeeded; log: %s", stepID, describe(events))
	return nil
}

func outputDigest(t *testing.T, events []runstore.Event, stepID, port string) string {
	t.Helper()
	for _, out := range succeededStatus(t, events, stepID).GetOutputs() {
		if out.GetPort() == port {
			return out.GetDigest().GetAlgo() + ":" + out.GetDigest().GetHex()
		}
	}
	t.Fatalf("step %q reported no output on port %q", stepID, port)
	return ""
}

// TestTheDefaultDeploymentCachesNothingAndSaysWhy is the configuration this
// binary ships with: no executor named, so the hosted engine runs steps as
// host processes, and a host process runs against whatever the host happens to
// carry.
//
// It is here because the previous version of this file could not have been
// written. Every test of the cache injected an identity into the plane, so
// there was no case at all for the configuration every deployment actually
// had — the one where nothing is cacheable — and no case that would have
// noticed if the identity stopped arriving. This is the negative half of
// TestSecondRunIsServedFromTheCache, and it must keep failing to cache for the
// reason it states.
func TestTheDefaultDeploymentCachesNothingAndSaysWhy(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srv := startEmbedded(ctx, t)
	warmTheFleet(ctx, t, srv)
	watcher := watchDispatches(ctx, t, srv)

	first, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	firstEvents := awaitRunCompleted(ctx, t, srv, first)
	require.Equal(t, "no stable environment identity to hash the step against",
		dispatchedPayload(t, firstEvents, "a").CacheIneligibleReason,
		"a host process engine names no environment, and the run log says so")

	second, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	awaitRunCompleted(ctx, t, srv, second)
	require.Equal(t, []string{"a", "b"}, watcher.stepsDispatchedFor(second),
		"nothing was recorded to reuse, so the second run does the work again")
}

// TestATierWhoseEnginesDisagreeAboutTheirEnvironmentStopsCaching is the
// misconfiguration ADR 0021 chose to make visible rather than tolerate: a tier
// is a set of interchangeable workers, and two of them advertising two
// different environments mean the plane cannot say what a result was produced
// in. Half a rollout turns the tier's cache off until it finishes.
//
// The second engine is a registration published on the bus, exactly as an
// engine announces itself — no type in this file pretends to be one.
func TestATierWhoseEnginesDisagreeAboutTheirEnvironmentStopsCaching(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	srv := startCachingServer(ctx, t, t.TempDir())
	warmTheFleet(ctx, t, srv)
	watcher := watchDispatches(ctx, t, srv)

	first, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	awaitRunCompleted(ctx, t, srv, first)
	require.Equal(t, []string{"a", "b"}, watcher.stepsDispatchedFor(first))

	// A second engine joins the tier, running a different image. It never runs
	// anything — it does not have to. The plane cannot know which of the two
	// the queue would hand the next step to.
	announceEngine(ctx, t, srv, "rolled-out-engine", "sha256:test-environment-v2")

	second, err := srv.Submit(ctx, tenantID, loadPipeline(t))
	require.NoError(t, err)
	secondEvents := awaitRunCompleted(ctx, t, srv, second)

	require.Equal(t, []string{"a", "b"}, watcher.stepsDispatchedFor(second),
		"the tier cannot say what its steps run in, so nothing may be served from the cache")
	require.False(t, dispatchedPayload(t, secondEvents, "a").Cacheable)
	require.Equal(t, "no stable environment identity to hash the step against",
		dispatchedPayload(t, secondEvents, "a").CacheIneligibleReason)
}

// announceEngine publishes one engine registration and waits until the plane
// has it, which is what makes the assertion after it about a fleet the plane
// can see rather than about a message in flight.
//
// It sends what an engine sends: a framed EngineMessage on engine.registration
// (docs/wire-contract.md). Nothing here reaches into the registry.
func announceEngine(ctx context.Context, t *testing.T, srv *server.Server, engineID, identity string) {
	t.Helper()
	conn, err := bus.Connect(ctx, srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	reg := &dholev1.EngineRegistration{
		EngineId:            engineID,
		ProtocolVersions:    []uint32{wire.ProtocolVersion},
		Os:                  runtime.GOOS,
		Arch:                runtime.GOARCH,
		Slots:               1,
		EngineTypes:         []string{"process"},
		Tier:                server.DefaultTier,
		EnvironmentIdentity: identity,
	}
	require.NoError(t, conn.Publish(ctx, bus.SubjectEngineRegistration(), wire.FrameRegistration(reg)))

	// Read back through the registry the plane itself writes, over the same
	// bucket. The TTL matches the server's on purpose: binding a KV bucket
	// with a different one would reconfigure the bucket the plane is using.
	raw, err := nats.Connect(srv.BusURL())
	require.NoError(t, err)
	t.Cleanup(raw.Close)
	fleet, err := registry.New(ctx, raw, tenantID, 30*time.Second)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		engines, err := fleet.Instances(ctx, tenantID)
		if err != nil {
			return false
		}
		for _, e := range engines {
			if e.ID == engineID && e.EnvironmentIdentity == identity {
				return true
			}
		}
		return false
	}, 30*time.Second, 50*time.Millisecond, "the plane never saw the second engine register")
}
