// Package registry is the runtime engine registry: which engine instances are
// alive right now, what they can do, and what state they are in.
//
// It is deliberately NOT the catalog (ADR 0010). The catalog is durable — what
// step, plugin, engine and trigger types exist, with their schemas — and must
// survive everything. This is the opposite: ephemeral state that should
// evaporate when the processes it describes do. Conflating them gives you
// either a stale row claiming an engine that died last week, or a step type
// that vanishes on reboot.
//
// So nothing here deletes an instance on a shutdown path. Liveness is a lease
// the engine renews: registration and every heartbeat rewrite the instance's
// key in a NATS KV bucket whose TTL is the heartbeat deadline, and an engine
// that stops heartbeating disappears because the server drops the key. There is
// no reconciliation job to forget to run, and no code path that has to notice a
// process died — which is the same mechanism ADR 0004's orphan detection rests
// on.
//
// The Instance type lives here rather than in the scheduler because it is the
// registry's vocabulary, not the scheduler's: the scheduler is a consumer that
// filters a fleet it did not assemble (scheduler.Match).
package registry

import (
	"context"
	"errors"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// ErrTenantRequired refuses an unscoped registry. Every stored record in Dhole
// carries a tenant scope, and an instance is a stored record: an empty tenant
// is a bug in the caller, never a wildcard that lists every tenant's fleet.
var ErrTenantRequired = errors.New("tenant scope required")

// ErrNotRegistered is returned for a heartbeat from an engine the registry does
// not currently hold: one that aged out while the engine was partitioned, or
// one that already reported itself gone.
//
// The heartbeat is NOT allowed to recreate it. An EngineHeartbeat carries an
// engine id and what it holds, and nothing else — no capabilities, no platform,
// no slots — so an instance resurrected from one would be a member of the fleet
// that advertises nothing and can never be matched, or worse, one carrying
// whatever attributes it had before it vanished. Refusing sends the engine back
// through Register, which is the one message that says what it can do, and the
// plane's view is rebuilt from a fresh advertisement rather than a memory.
var ErrNotRegistered = errors.New("engine is not registered; it must register again")

// ErrEngineRequired refuses an anonymous registration or heartbeat: without an
// engine id there is no identity to key liveness on.
var ErrEngineRequired = errors.New("engine id required")

// State is where an instance stands in its lifecycle. It is deliberately not a
// boolean "alive": an instance that is draining is alive, answering
// heartbeats, and finishing work, yet must be handed nothing new — a fact no
// liveness flag can express.
type State string

// The lifecycle an engine instance moves through.
const (
	// StateRegistering has announced itself but is not yet accepting work.
	StateRegistering State = "registering"
	// StateReady is accepting work. The only state the scheduler dispatches to.
	StateReady State = "ready"
	// StateDraining is finishing what it holds and taking nothing new.
	StateDraining State = "draining"
	// StateGone has stopped heartbeating or has exited. Whatever it held is
	// orphaned and re-dispatched under a new fence.
	StateGone State = "gone"
)

// Instance is one engine as the control plane currently sees it. Every field
// is something a scheduling decision reads: the capabilities and platform it
// advertises, how much it will run at once, and the protocol versions it
// speaks.
type Instance struct {
	// ID is the engine's stable identity on the bus.
	ID string
	// State is where it stands in its lifecycle.
	State State
	// Capabilities it advertises. A capability it cannot honestly enforce is
	// one it must not advertise, so this list is what a step is matched
	// against.
	Capabilities []dholev1.Capability
	// OS and Arch are the platform it runs on, in Go's GOOS/GOARCH vocabulary.
	OS   string
	Arch string
	// Slots is how many jobs it will run concurrently.
	Slots int
	// ProtocolVersions is every wire version it speaks, so the control plane
	// can pick the highest both sides support.
	ProtocolVersions []uint32
}

// Registry is the live fleet, as the control plane sees it.
//
// The lifecycle it implements is registering -> ready -> draining -> gone, and
// each transition has exactly one trigger:
//
//   - Register puts an instance in registering. It is a claim, not proof: the
//     protocol version is negotiated here, and an engine the plane cannot talk
//     to never enters the fleet at all.
//   - The first Heartbeat promotes registering to ready. Readiness is proof of
//     liveness under the engine's own hand, which is also the moment the TTL
//     clock starts being renewed rather than merely set.
//   - Drain moves ready or registering to draining. It stops new dispatch
//     immediately and stops nothing else: the instance stays in the fleet,
//     alive and heartbeating, until its last job is done. A drain that killed
//     running work would make every rolling upgrade an outage.
//   - A draining engine's first heartbeat holding nothing is the drain
//     completing, and moves it to gone. Gone is a tombstone kept until it ages
//     out, so a late heartbeat cannot walk back into the fleet.
//
// Every method on an implementation is scoped to one tenant, and Instances
// refuses an empty one.
type Registry interface {
	// Register admits an engine to the fleet in state registering, replacing
	// any earlier registration of the same id — a restarted engine is the
	// normal case. It refuses an engine whose protocol version this control
	// plane does not speak.
	Register(ctx context.Context, r *dholev1.EngineRegistration) error
	// Heartbeat renews an instance's liveness and records what it holds. It
	// returns ErrNotRegistered for an engine that has aged out or is gone,
	// rather than recreating one from a message that cannot describe it.
	Heartbeat(ctx context.Context, h *dholev1.EngineHeartbeat) error
	// Instances is the fleet for one tenant, never including gone instances.
	// An empty tenant is refused with ErrTenantRequired.
	Instances(ctx context.Context, tenantID string) ([]Instance, error)
	// Drain stops new work reaching an engine while it finishes what it holds.
	// It is idempotent, and draining an engine the registry does not hold is
	// the normal outcome of a stale operator list, not an error.
	Drain(ctx context.Context, engineID string) error
}
