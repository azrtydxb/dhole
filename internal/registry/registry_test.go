package registry_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/registry"
	"github.com/azrtydxb/dhole/internal/scheduler"
	"github.com/azrtydxb/dhole/internal/wire"
)

// tenantA and tenantB scope every instance in this file. There is no unscoped
// registration and no unscoped list, even with one tenant.
const (
	tenantA = "tenant-a"
	tenantB = "tenant-b"
)

// liveTTL is long enough that nothing ages out while a test is asserting on
// lifecycle, and shortTTL is what the expiry tests use. shortTTL is
// deliberately small: an expiry test that waits seconds proves only that the
// entry went away eventually, and would pass against a server sweeping on a
// timer nobody set.
const (
	liveTTL  = 10 * time.Second
	shortTTL = 200 * time.Millisecond
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// newRegistry brings up an embedded NATS with JetStream and a registry over it,
// scoped to tenantID. The URL is returned so a second tenant's registry can be
// attached to the same bucket, which is where cross-tenant leakage would show.
func newRegistry(ctx context.Context, t *testing.T, tenantID string, ttl time.Duration) (*registry.KV, string) {
	t.Helper()

	srv, err := bus.StartEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(srv.Close)

	return registryOn(ctx, t, srv.URL(), tenantID, ttl), srv.URL()
}

func registryOn(ctx context.Context, t *testing.T, url, tenantID string, ttl time.Duration) *registry.KV {
	t.Helper()

	conn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)

	reg, err := registry.New(ctx, conn, tenantID, ttl)
	require.NoError(t, err)
	return reg
}

// registration is a well-formed announcement from an engine speaking the
// version this build speaks.
func registration(engineID string, caps ...dholev1.Capability) *dholev1.EngineRegistration {
	return &dholev1.EngineRegistration{
		EngineId:         engineID,
		ProtocolVersions: []uint32{wire.ProtocolVersion},
		Capabilities:     caps,
		Os:               "linux",
		Arch:             "amd64",
		Slots:            4,
		EngineTypes:      []string{"process"},
		Tier:             testTier,
		// What the tier's cache keys are hashed against (ADR 0021). Written
		// here rather than in the one test about it, because it has to survive
		// every round trip through the bucket, not just that one.
		EnvironmentIdentity: testEnvIdentity,
	}
}

// testTier and testEnvIdentity are the tier every registration in this file is
// in and the environment it names.
const (
	testTier        = "trusted"
	testEnvIdentity = "sha256:sandbox-v1"
)

func heartbeat(engineID string, inFlight ...*dholev1.InFlight) *dholev1.EngineHeartbeat {
	return &dholev1.EngineHeartbeat{EngineId: engineID, InFlight: inFlight}
}

func job(runID, stepID string) *dholev1.InFlight {
	return &dholev1.InFlight{RunId: runID, StepId: stepID, Attempt: 1, FenceToken: "7"}
}

// instanceByID finds one instance in a fleet listing.
func instanceByID(t *testing.T, instances []registry.Instance, engineID string) (registry.Instance, bool) {
	t.Helper()
	for _, i := range instances {
		if i.ID == engineID {
			return i, true
		}
	}
	return registry.Instance{}, false
}

func idsOf(instances []registry.Instance) []string {
	out := make([]string, 0, len(instances))
	for _, i := range instances {
		out = append(out, i.ID)
	}
	return out
}

// TestInstanceAgesOutWithoutHeartbeat is the property the whole runtime
// registry exists for: liveness is self-healing. An engine that stops
// heartbeating disappears on its own, with nothing anywhere having to remember
// to delete it — which is also how an orphaned step is detected.
func TestInstanceAgesOutWithoutHeartbeat(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, shortTTL)

	require.NoError(t, reg.Register(ctx, registration("engine-1")))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1")))

	// Present first: an assertion that it is gone later proves nothing if it
	// was never there.
	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.Equal(t, []string{"engine-1"}, idsOf(instances))
	require.Equal(t, registry.StateReady, instances[0].State)

	// Then stop heartbeating. The window is bounded: if the TTL never expires
	// anything, this fails rather than waiting the test out.
	deadline := time.Now().Add(4 * time.Second)
	for {
		instances, err = reg.Instances(ctx, tenantA)
		require.NoError(t, err)
		if len(instances) == 0 {
			break
		}
		require.False(t, time.Now().After(deadline),
			"engine-1 is still registered %s after its last heartbeat with a %s TTL: %v",
			time.Since(deadline.Add(-4*time.Second)), shortTTL, idsOf(instances))
		time.Sleep(10 * time.Millisecond)
	}
}

// TestDrainStopsNewWorkButFinishesInFlight is what makes a rolling upgrade not
// an outage. Draining removes an engine from dispatch immediately, and leaves
// it alive — heartbeating, holding, and finishing — until its last job is done.
func TestDrainStopsNewWorkButFinishesInFlight(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	require.NoError(t, reg.Register(ctx, registration("engine-1", dholev1.Capability_CAPABILITY_NETWORK)))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1", job("run-1", "step-1"))))

	req := executor.Requirements{OS: "linux", Arch: "amd64"}
	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.Equal(t, []string{"engine-1"}, idsOf(scheduler.Match(req, instances)),
		"a ready engine holding a job still takes new work")

	require.NoError(t, reg.Drain(ctx, "engine-1"))

	instances, err = reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	drained, ok := instanceByID(t, instances, "engine-1")
	require.True(t, ok, "a draining engine is still part of the fleet: it is alive and holding work")
	require.Equal(t, registry.StateDraining, drained.State)
	require.Empty(t, scheduler.Match(req, instances), "a draining engine must be handed nothing new")

	// The in-flight step keeps running: its heartbeats are still accepted, and
	// the instance neither ages out nor is forced back into ready.
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1", job("run-1", "step-1"))),
		"a draining engine's heartbeat must keep it alive while it finishes")
	instances, err = reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	drained, ok = instanceByID(t, instances, "engine-1")
	require.True(t, ok)
	require.Equal(t, registry.StateDraining, drained.State,
		"a heartbeat must not undo a drain")
	require.Empty(t, scheduler.Match(req, instances))

	// The step finishes. The first heartbeat holding nothing is the engine
	// reporting the drain complete, and it leaves the fleet.
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1")))
	instances, err = reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.Empty(t, instances, "a drained engine that holds nothing is gone")
}

// TestRegistrationWithUnsupportedProtocolIsRefused keeps an engine the control
// plane cannot talk to out of the fleet entirely, rather than discovering the
// mismatch at dispatch time when a step is already committed to it.
func TestRegistrationWithUnsupportedProtocolIsRefused(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	old := registration("engine-old")
	old.ProtocolVersions = []uint32{0}

	err := reg.Register(ctx, old)
	require.Error(t, err)
	require.ErrorContains(t, err, "unsupported protocol")

	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.Empty(t, instances, "a refused registration must leave nothing behind")

	// And it cannot sneak in through the heartbeat path either.
	require.Error(t, reg.Heartbeat(ctx, heartbeat("engine-old")))
	instances, err = reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.Empty(t, instances)
}

// TestSchedulerRoutesOnlyToInstancesSatisfyingResolvedRequirements is version
// skew as a first-class condition (ADR 0010): the fleet is routinely on several
// builds, and work goes only where both the protocol and the capabilities fit.
func TestSchedulerRoutesOnlyToInstancesSatisfyingResolvedRequirements(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	// Too old: below the compatibility window.
	tooOld := registration("engine-v0", dholev1.Capability_CAPABILITY_NETWORK)
	tooOld.ProtocolVersions = []uint32{0}
	require.ErrorContains(t, reg.Register(ctx, tooOld), "unsupported protocol")

	// Too new: an engine from a future build the plane cannot be talked down
	// to. It is refused rather than deferred to.
	tooNew := registration("engine-ahead", dholev1.Capability_CAPABILITY_NETWORK)
	tooNew.ProtocolVersions = []uint32{wire.ProtocolVersion + 1}
	require.ErrorContains(t, reg.Register(ctx, tooNew), "unsupported protocol")

	// Right version, wrong capabilities.
	require.NoError(t, reg.Register(ctx, registration("engine-no-net")))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-no-net")))

	// Right version, wrong platform.
	windows := registration("engine-windows", dholev1.Capability_CAPABILITY_NETWORK)
	windows.Os = "windows"
	require.NoError(t, reg.Register(ctx, windows))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-windows")))

	// Everything satisfied, on a build one version behind and on the current
	// one: both speak a version the plane accepts.
	behind := registration("engine-behind", dholev1.Capability_CAPABILITY_NETWORK)
	behind.ProtocolVersions = []uint32{wire.ProtocolVersion - 1}
	require.NoError(t, reg.Register(ctx, behind))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-behind")))

	current := registration("engine-current", dholev1.Capability_CAPABILITY_NETWORK, dholev1.Capability_CAPABILITY_SECRETS)
	require.NoError(t, reg.Register(ctx, current))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-current")))

	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.ElementsMatch(t,
		[]string{"engine-no-net", "engine-windows", "engine-behind", "engine-current"},
		idsOf(instances),
		"only engines speaking an accepted protocol version are in the fleet")

	matched := scheduler.Match(executor.Requirements{
		OS:           "linux",
		Arch:         "amd64",
		Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK},
	}, instances)
	require.ElementsMatch(t, []string{"engine-behind", "engine-current"}, idsOf(matched))

	// The advertised versions survive the round trip, because deciding what an
	// instance may be sent is downstream of knowing what it speaks.
	behindInstance, ok := instanceByID(t, instances, "engine-behind")
	require.True(t, ok)
	require.Equal(t, []uint32{wire.ProtocolVersion - 1}, behindInstance.ProtocolVersions)
	require.Equal(t, 4, behindInstance.Slots)
}

// TestInstancesRefusesAnUnscopedTenant: every query in Dhole is tenant-scoped,
// and an empty tenant is a bug in the caller rather than a wildcard that
// happens to list the whole world.
func TestInstancesRefusesAnUnscopedTenant(t *testing.T) {
	ctx := testContext(t)
	reg, url := newRegistry(ctx, t, tenantA, liveTTL)

	require.NoError(t, reg.Register(ctx, registration("engine-1")))

	_, err := reg.Instances(ctx, "")
	require.Error(t, err)
	require.ErrorContains(t, err, "tenant scope required")

	conn, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	_, err = registry.New(ctx, conn, "", liveTTL)
	require.ErrorContains(t, err, "tenant scope required")
}

// TestInstancesAreTenantScoped: one tenant's fleet is never another's, even
// when both name an engine identically and both live in one bucket.
func TestInstancesAreTenantScoped(t *testing.T) {
	ctx := testContext(t)
	regA, url := newRegistry(ctx, t, tenantA, liveTTL)
	regB := registryOn(ctx, t, url, tenantB, liveTTL)

	require.NoError(t, regA.Register(ctx, registration("shared-name", dholev1.Capability_CAPABILITY_NETWORK)))
	require.NoError(t, regA.Heartbeat(ctx, heartbeat("shared-name")))
	require.NoError(t, regA.Register(ctx, registration("only-in-a")))
	require.NoError(t, regB.Register(ctx, registration("shared-name")))
	require.NoError(t, regB.Register(ctx, registration("only-in-b")))

	a, err := regA.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"shared-name", "only-in-a"}, idsOf(a))

	b, err := regB.Instances(ctx, tenantB)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"shared-name", "only-in-b"}, idsOf(b))

	// The two "shared-name" instances are independent records, not one entry
	// seen twice: tenant A's has been heartbeaten into ready and B's has not.
	sharedA, ok := instanceByID(t, a, "shared-name")
	require.True(t, ok)
	require.Equal(t, registry.StateReady, sharedA.State)
	sharedB, ok := instanceByID(t, b, "shared-name")
	require.True(t, ok)
	require.Equal(t, registry.StateRegistering, sharedB.State)

	// Draining in one tenant leaves the other tenant's engine of the same name
	// untouched.
	require.NoError(t, regA.Drain(ctx, "shared-name"))
	b, err = regB.Instances(ctx, tenantB)
	require.NoError(t, err)
	sharedB, ok = instanceByID(t, b, "shared-name")
	require.True(t, ok)
	require.Equal(t, registry.StateRegistering, sharedB.State)
}

// TestHeartbeatAfterAgeOutIsRefused: a heartbeat carries only an engine id and
// what it holds, so honouring one for an instance that has aged out would
// recreate it with no capabilities, no platform and no slots — an engine that
// is in the fleet and can never be matched. The engine is told to register
// again instead, which is the one message that carries what the plane needs.
func TestHeartbeatAfterAgeOutIsRefused(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, shortTTL)

	require.NoError(t, reg.Register(ctx, registration("engine-1", dholev1.Capability_CAPABILITY_NETWORK)))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1")))

	deadline := time.Now().Add(4 * time.Second)
	for {
		instances, err := reg.Instances(ctx, tenantA)
		require.NoError(t, err)
		if len(instances) == 0 {
			break
		}
		require.False(t, time.Now().After(deadline), "engine-1 never aged out")
		time.Sleep(10 * time.Millisecond)
	}

	err := reg.Heartbeat(ctx, heartbeat("engine-1", job("run-1", "step-1")))
	require.Error(t, err)
	require.ErrorIs(t, err, registry.ErrNotRegistered)

	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.Empty(t, instances, "a heartbeat must not resurrect an aged-out engine")

	// Registering again is the way back, and it restores the full advertisement.
	require.NoError(t, reg.Register(ctx, registration("engine-1", dholev1.Capability_CAPABILITY_NETWORK)))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1")))
	instances, err = reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.Len(t, instances, 1)
	require.Equal(t, registry.StateReady, instances[0].State)
	require.Equal(t, []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK}, instances[0].Capabilities)
}

// TestDrainIsIdempotentAndUnknownEngineIsNotACrash: drain is what a rolling
// upgrade calls, from a script, twice, on a list that may be out of date.
func TestDrainIsIdempotentAndUnknownEngineIsNotACrash(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	require.NoError(t, reg.Register(ctx, registration("engine-1")))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1", job("run-1", "step-1"))))

	require.NoError(t, reg.Drain(ctx, "engine-1"))
	require.NoError(t, reg.Drain(ctx, "engine-1"))

	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	drained, ok := instanceByID(t, instances, "engine-1")
	require.True(t, ok)
	require.Equal(t, registry.StateDraining, drained.State)

	require.NoError(t, reg.Drain(ctx, "no-such-engine"),
		"draining an engine that is already gone is the normal case, not an error")
	require.NoError(t, reg.Drain(ctx, ""))

	instances, err = reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.Equal(t, []string{"engine-1"}, idsOf(instances))
}

// TestInstancesNeverReturnsGoneEngines: a gone instance stays in the bucket as
// a tombstone until it ages out, so a late heartbeat cannot walk back into the
// fleet. It must never be listed — a scheduler that saw it would dispatch to a
// process that has exited.
func TestInstancesNeverReturnsGoneEngines(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	require.NoError(t, reg.Register(ctx, registration("engine-1")))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1", job("run-1", "step-1"))))
	require.NoError(t, reg.Drain(ctx, "engine-1"))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1")))

	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.Empty(t, instances, "a gone engine must never be listed")

	// The record is still there — this is a filter, not a deletion — and it
	// refuses the late heartbeat that would otherwise resurrect it.
	err = reg.Heartbeat(ctx, heartbeat("engine-1", job("run-2", "step-2")))
	require.ErrorIs(t, err, registry.ErrNotRegistered)

	instances, err = reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.Empty(t, instances)
}

// TestRegisterRequiresAnEngineID: an anonymous registration would take the same
// key as every other anonymous one.
func TestRegisterRequiresAnEngineID(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	require.Error(t, reg.Register(ctx, registration("")))
	require.Error(t, reg.Register(ctx, nil))
	require.Error(t, reg.Heartbeat(ctx, heartbeat("")))
	require.Error(t, reg.Heartbeat(ctx, nil))
}

// TestRegistryKeepsTheEngineTypesAnEngineAdvertises: an EngineRegistration
// says which executor backends the engine offers, and that is the only place
// the fact exists. A registry that drops it leaves every consumer — the
// planner above all — to answer "which kind of engine takes this step" from
// its own local configuration, which is a different answer on any fleet whose
// engines are not all alike.
func TestRegistryKeepsTheEngineTypesAnEngineAdvertises(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	announcement := registration("engine-mixed")
	announcement.EngineTypes = []string{"container", "process"}
	require.NoError(t, reg.Register(ctx, announcement))
	require.NoError(t, reg.Heartbeat(ctx, &dholev1.EngineHeartbeat{EngineId: "engine-mixed"}))

	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	require.Len(t, instances, 1)
	require.Equal(t, []string{"container", "process"}, instances[0].EngineTypes,
		"the engine types survive the write and the read, or nothing downstream can "+
			"tell a container engine from a process one")
}

// TestAnEngineThatReannouncesItselfStaysDispatchable is a real outage, in one
// test.
//
// The wire contract obliges every engine to publish its registration again
// every fifteen seconds, and Register used to write StateRegistering over
// whatever was there. scheduler.Match dispatches to ready instances only, so
// every healthy engine dropped out of the dispatchable fleet three times a
// minute and stayed out until its next heartbeat, up to five seconds later —
// which showed up on a live cluster as the first step of every run being
// recorded STEP_UNSCHEDULABLE, "no registered engine is ready", against a warm
// idle fleet of two engines.
//
// Saying again what you are does not un-prove that you are alive.
func TestAnEngineThatReannouncesItselfStaysDispatchable(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	require.NoError(t, reg.Register(ctx, registration("engine-1")))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1")))

	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	ready, ok := instanceByID(t, instances, "engine-1")
	require.True(t, ok)
	require.Equal(t, registry.StateReady, ready.State, "a heartbeat is what proves readiness")

	// The obligatory re-announcement, unchanged, exactly as the agent sends it.
	require.NoError(t, reg.Register(ctx, registration("engine-1")))

	instances, err = reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	after, ok := instanceByID(t, instances, "engine-1")
	require.True(t, ok)
	require.Equal(t, registry.StateReady, after.State,
		"an engine that says again what it is has not stopped being alive")
	require.NotEmpty(t, scheduler.Match(executor.Requirements{OS: "linux", Arch: "amd64"}, instances),
		"and it is still somewhere a step can be dispatched")
}

// TestAReannouncementReplacesWhatAnEngineAdvertises is the other half of the
// same rule: the STATE survives a re-announcement and the DESCRIPTION does
// not. Preserving the description would make a re-announcement pointless —
// changing what an engine advertises is the whole reason the wire contract
// obliges it to send one.
func TestAReannouncementReplacesWhatAnEngineAdvertises(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	require.NoError(t, reg.Register(ctx, registration("engine-1")))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1")))

	upgraded := registration("engine-1", dholev1.Capability_CAPABILITY_NETWORK)
	upgraded.Slots = 9
	upgraded.EnvironmentIdentity = "sha256:sandbox-v2"
	require.NoError(t, reg.Register(ctx, upgraded))

	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	after, ok := instanceByID(t, instances, "engine-1")
	require.True(t, ok)
	require.Equal(t, registry.StateReady, after.State)
	require.Equal(t, 9, after.Slots)
	require.Equal(t, []dholev1.Capability{dholev1.Capability_CAPABILITY_NETWORK}, after.Capabilities)
	require.Equal(t, "sha256:sandbox-v2", after.EnvironmentIdentity)
}

// TestADrainingEngineThatReannouncesItselfDoesNotBecomeDispatchableAgain: a
// drain is a decision the plane made about an engine, and the engine repeating
// what it is cannot overturn it. If it could, every rolling upgrade would hand
// new work to the engine it was trying to empty, fifteen seconds after
// draining it.
func TestADrainingEngineThatReannouncesItselfDoesNotBecomeDispatchableAgain(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	require.NoError(t, reg.Register(ctx, registration("engine-1")))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1", job("run-1", "step-1"))))
	require.NoError(t, reg.Drain(ctx, "engine-1"))

	require.NoError(t, reg.Register(ctx, registration("engine-1")))

	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	after, ok := instanceByID(t, instances, "engine-1")
	require.True(t, ok)
	require.Equal(t, registry.StateDraining, after.State)
	require.Empty(t, scheduler.Match(executor.Requirements{OS: "linux", Arch: "amd64"}, instances),
		"a draining engine takes no new work, however often it announces itself")
}

// TestAnEngineThatWasGoneMustProveItsLivenessAgainBeforeItIsDispatchedTo: the
// tombstone exists so a dead engine cannot walk back into the fleet, and a
// registration is a claim rather than proof. Re-announcing therefore starts it
// at registering, and only a heartbeat — under the engine's own hand — makes
// it dispatchable.
func TestAnEngineThatWasGoneMustProveItsLivenessAgainBeforeItIsDispatchedTo(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	// Registered, ready, drained, and then the heartbeat holding nothing that
	// completes the drain: gone.
	require.NoError(t, reg.Register(ctx, registration("engine-1")))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1", job("run-1", "step-1"))))
	require.NoError(t, reg.Drain(ctx, "engine-1"))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1")))

	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	_, ok := instanceByID(t, instances, "engine-1")
	require.False(t, ok, "a gone instance is not in the fleet")

	require.NoError(t, reg.Register(ctx, registration("engine-1")))
	instances, err = reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	back, ok := instanceByID(t, instances, "engine-1")
	require.True(t, ok)
	require.Equal(t, registry.StateRegistering, back.State,
		"a tombstone may not resurrect straight into ready")
	require.Empty(t, scheduler.Match(executor.Requirements{OS: "linux", Arch: "amd64"}, instances))

	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1")))
	instances, err = reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	proven, ok := instanceByID(t, instances, "engine-1")
	require.True(t, ok)
	require.Equal(t, registry.StateReady, proven.State)
}

// TestAReannouncementKeepsWhatTheLastHeartbeatSaidTheEngineHolds: a
// registration cannot describe in-flight work, so writing zero would be
// inventing a fact the message does not carry. The in-flight list is the only
// answer to "which engine holds this step", and blanking it every fifteen
// seconds left a window in which a cancellation had nowhere to go.
func TestAReannouncementKeepsWhatTheLastHeartbeatSaidTheEngineHolds(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	require.NoError(t, reg.Register(ctx, registration("engine-1")))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1", job("run-1", "step-1"))))

	require.NoError(t, reg.Register(ctx, registration("engine-1")))

	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	after, ok := instanceByID(t, instances, "engine-1")
	require.True(t, ok)
	require.Len(t, after.InFlight, 1)
	require.Equal(t, "step-1", after.InFlight[0].StepID)
}

// TestAnEnginesEnvironmentIdentityReachesTheFleetItAnnouncedItTo is the whole
// point of the new field: the plane cannot see the environment an engine runs
// steps in, so it has to arrive on the registration and survive the round trip
// through the bucket (ADR 0021).
func TestAnEnginesEnvironmentIdentityReachesTheFleetItAnnouncedItTo(t *testing.T) {
	ctx := testContext(t)
	reg, _ := newRegistry(ctx, t, tenantA, liveTTL)

	require.NoError(t, reg.Register(ctx, registration("engine-1")))
	require.NoError(t, reg.Heartbeat(ctx, heartbeat("engine-1")))

	instances, err := reg.Instances(ctx, tenantA)
	require.NoError(t, err)
	one, ok := instanceByID(t, instances, "engine-1")
	require.True(t, ok)
	require.Equal(t, testEnvIdentity, one.EnvironmentIdentity)
	require.Equal(t, testTier, one.Tier)
}

// TestATierTakesTheIdentityItsEnginesAgreeOn covers the four answers
// TierEnvironmentIdentity can give, because three of them are refusals and a
// refusal that came back as an identity would key the cache on a lie.
func TestATierTakesTheIdentityItsEnginesAgreeOn(t *testing.T) {
	engine := func(id, tier, identity string) registry.Instance {
		return registry.Instance{
			ID: id, State: registry.StateReady, Tier: tier, EnvironmentIdentity: identity,
		}
	}

	t.Run("engines that agree name the tier's environment", func(t *testing.T) {
		identity, conflict := registry.TierEnvironmentIdentity([]registry.Instance{
			engine("e1", "trusted", "sha256:a"),
			engine("e2", "trusted", "sha256:a"),
			// Another tier's disagreement is not this tier's problem.
			engine("e3", "untrusted", "sha256:b"),
		}, "trusted")
		require.Equal(t, "sha256:a", identity)
		require.Empty(t, conflict)
	})

	t.Run("engines that disagree leave the tier with no identity", func(t *testing.T) {
		identity, conflict := registry.TierEnvironmentIdentity([]registry.Instance{
			engine("e1", "trusted", "sha256:a"),
			engine("e2", "trusted", "sha256:b"),
		}, "trusted")
		require.Empty(t, identity,
			"a half-finished rollout of two images caches nothing rather than caching wrongly")
		require.Equal(t, []string{"sha256:a", "sha256:b"}, conflict,
			"and the conflicting identities are reported, so the plane can say what is wrong")
	})

	t.Run("one engine naming no environment leaves the tier with none", func(t *testing.T) {
		identity, conflict := registry.TierEnvironmentIdentity([]registry.Instance{
			engine("e1", "trusted", "sha256:a"),
			engine("e2", "trusted", ""),
		}, "trusted")
		require.Empty(t, identity,
			"the queue could hand the step to the engine that cannot name where it ran it")
		require.Equal(t, []string{"", "sha256:a"}, conflict)
	})

	t.Run("a tier of host processes has no identity and no conflict", func(t *testing.T) {
		identity, conflict := registry.TierEnvironmentIdentity([]registry.Instance{
			engine("e1", "trusted", ""),
			engine("e2", "trusted", ""),
		}, "trusted")
		require.Empty(t, identity)
		require.Empty(t, conflict, "agreeing that there is nothing to name is not a disagreement")
	})

	t.Run("a tier nothing has registered in has no identity", func(t *testing.T) {
		identity, conflict := registry.TierEnvironmentIdentity(nil, "trusted")
		require.Empty(t, identity, "a plane whose fleet has not checked in has a cold cache")
		require.Empty(t, conflict)
	})
}
