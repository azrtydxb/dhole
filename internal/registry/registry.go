// Package registry is the runtime engine registry: which engine instances are
// alive right now, what they can do, and what state they are in.
//
// Only the Instance type exists so far. It is here rather than in the
// scheduler because it is the registry's vocabulary, not the scheduler's: the
// scheduler is a consumer that filters a fleet it did not assemble
// (scheduler.Match). The lifecycle over NATS KV — Register, Heartbeat,
// Instances, Drain, and the heartbeat TTL that makes a dead engine disappear —
// arrives with the task that owns this package.
package registry

import (
	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

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
