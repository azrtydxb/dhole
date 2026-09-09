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
		ProtocolVersions: []uint32{1},
		Capabilities:     caps,
		Os:               "linux",
		Arch:             "amd64",
		Slots:            4,
		EngineTypes:      []string{"process"},
		Tier:             "trusted",
	}
}

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
	tooNew := registration("engine-v2", dholev1.Capability_CAPABILITY_NETWORK)
	tooNew.ProtocolVersions = []uint32{2}
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
	behind.ProtocolVersions = []uint32{1}
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
	require.Equal(t, []uint32{1}, behindInstance.ProtocolVersions)
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
