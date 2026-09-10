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
	"slices"

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
	// EngineTypes are the executor backends this engine offers — "process",
	// "container" — as it advertised them. It is the only place that fact
	// exists: a consumer that cannot read it here answers "which kind of
	// engine takes this step" from its own local configuration, which is a
	// different answer on any fleet whose engines are not all alike.
	//
	// scheduler.Match filters on it: a step naming Step.engine_type may only
	// be placed on an engine that offered that kind. An engine that advertised
	// none satisfies no such step — unstated is unknown, not universal.
	EngineTypes []string
	// Slots is how many jobs it will run concurrently.
	Slots int
	// ProtocolVersions is every wire version it speaks, so the control plane
	// can pick the highest both sides support.
	ProtocolVersions []uint32
	// Tier is the trust tier it runs in, as it advertised it. Work reaches an
	// engine by being published to its tier's dispatch subject, so this is the
	// only place the plane can see which engines a dispatch could land on.
	Tier string
	// EnvironmentIdentity is the digest of the environment it runs steps in,
	// empty when it has none. Cache keys are hashed against the TIER's
	// identity, agreed by its members (ADR 0021) — see TierEnvironmentIdentity.
	EnvironmentIdentity string
	// InFlight is what its last heartbeat said it was holding.
	//
	// It is kept, and not merely counted, because it is the ONLY answer to
	// "which engine has this step?". A dispatch goes to a subject and which
	// member of the fleet picked it up is never decided by the plane, so a
	// cancellation with nothing but a count could not find the engine to
	// send it to.
	InFlight []Job
}

// Job is one attempt an engine says it is running, as its heartbeat reported
// it. The fence travels with it: a control message carrying a stale fence is
// one the engine must ignore, so cancelling means quoting back the fence of
// the attempt actually in flight.
type Job struct {
	RunID      string
	StepID     string
	Attempt    uint32
	FenceToken string
}

// Registry is the live fleet, as the control plane sees it.
//
// The lifecycle it implements is registering -> ready -> draining -> gone, and
// each transition has exactly one trigger:
//
//   - Register puts a NEW instance in registering. It is a claim, not proof: the
//     protocol version is negotiated here, and an engine the plane cannot talk
//     to never enters the fleet at all. An engine that is already in the fleet
//     keeps the state it is in — an engine re-announcing what it is, as the
//     wire contract obliges it to every fifteen seconds, has not stopped being
//     alive — while everything it advertises is replaced.
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
	// Register admits an engine to the fleet, replacing the advertisement
	// under any earlier registration of the same id — a restarted engine, and
	// the obligatory re-announcement, are both the normal case. A new or gone
	// engine starts in registering; one already in the fleet keeps its state.
	// It refuses an engine whose protocol version this control plane does not
	// speak.
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

// TierEnvironmentIdentity is the environment identity every cache key for tier
// is hashed against, and the identities that stopped it having one.
//
// The TIER, not the engine. A tier exists to be a set of interchangeable
// workers — the plane chooses a tier and the queue chooses which member picks
// the dispatch up — so an identity that varied per engine would key the cache
// on something the scheduler does not get to pick, and a step could be recorded
// under the environment of the engine that happened to run it and then served
// to a step that will run somewhere else (ADR 0021).
//
// Members are therefore expected to agree, and there are three ways they can
// fail to. All of them return "" and cache nothing:
//
//   - nobody is in the tier. A plane whose fleet has not checked in yet has a
//     cold cache, not a wrong one.
//   - a member reports no identity. It runs steps in an environment nothing can
//     name — a host process — and the rest of the tier cannot answer for it.
//   - members disagree. That is a misconfiguration, usually a half-finished
//     rollout of two different sandbox images, and the conflicting identities
//     are returned so the plane can say so rather than silently degrading.
//
// Every instance the registry still holds counts, including one that is
// draining or has not yet proven readiness. Only ready instances take new work,
// but a registering member is about to and a draining one is still finishing
// steps whose results get recorded — and an entry recorded under an identity
// the next dispatch will not run in is exactly the wrong answer this exists to
// avoid.
func TierEnvironmentIdentity(instances []Instance, tier string) (identity string, conflict []string) {
	var seen []string
	for _, e := range instances {
		if e.Tier != tier {
			continue
		}
		if !slices.Contains(seen, e.EnvironmentIdentity) {
			seen = append(seen, e.EnvironmentIdentity)
		}
	}
	switch len(seen) {
	case 0:
		return "", nil // nothing in this tier has announced itself
	case 1:
		return seen[0], nil // "" when the single answer is "I have none"
	default:
		slices.Sort(seen)
		return "", seen
	}
}
