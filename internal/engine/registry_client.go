package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/bus"
	"github.com/azrtydxb/dhole/internal/wire"
)

// HeartbeatInterval is how often an engine proves it is alive and says what it
// is holding, per docs/wire-contract.md. A lease that stops being renewed
// expires and the control plane re-dispatches the step under a new fence, so
// this interval is a contract number and not a tuning knob.
const HeartbeatInterval = 5 * time.Second

// RegistrationInterval is how often an engine RE-ANNOUNCES itself.
//
// A registration is fire-and-forget on a core subject, and the plane's
// Heartbeat deliberately refuses to rebuild an instance from a beat that
// cannot describe one (registry.ErrNotRegistered): an instance resurrected
// from a heartbeat would advertise no platform and no capabilities, and every
// step matched against it would be unschedulable. Both of those are right, and
// together they made a registration published while no plane was listening
// permanent: the engine stayed invisible until somebody restarted it — an
// engine behind NAT in a customer's network, which is the engine nobody can
// restart.
//
// The alternative was a durable registration subject. It was rejected: a
// stream would make a plane that restarts replay the registrations of engines
// that died months ago, which is precisely the stale fleet ADR 0010 keeps the
// registry ephemeral to avoid — and it would then need retention tuning, an
// acknowledged consumer, and engine credentials to publish into a stream. A
// repeat costs one small message every fifteen seconds per engine and is
// self-healing: whatever was missed, the plane hears again shortly, and an
// engine that has genuinely died stops repeating and ages out on its own.
//
// Three heartbeats: long enough not to be chatter, short enough that a plane
// which has just come up has the fleet within one lease TTL.
const RegistrationInterval = 3 * HeartbeatInterval

// publishTimeout bounds one outbound publish. Nothing in an engine may wait
// forever: a bus call that hangs holds a slot and stops the heartbeats that
// prove the engine is alive.
const publishTimeout = 5 * time.Second

// CapsHash is the stable hash of a capability set that names a dispatch
// subject's last token. The control plane and every engine must compute it the
// same way or work is published where nobody is listening, so it is defined
// here in terms the wire understands — the sorted, de-duplicated enum numbers —
// rather than in terms of Go's spelling of them.
func CapsHash(caps []dholev1.Capability) string {
	sum := sha256.New()
	for _, c := range normaliseCaps(caps) {
		// The enum NUMBER, in decimal, is the hashed form: it is what the wire
		// carries, so an engine in another language hashes the same bytes
		// without knowing Go's name for the constant.
		_, _ = fmt.Fprintf(sum, "%d\n", int64(c))
	}
	// Half a SHA-256 is 64 bits of collision resistance over a set that has at
	// most a handful of members, and it keeps the subject token readable.
	return hex.EncodeToString(sum.Sum(nil))[:16]
}

// normaliseCaps sorts and de-duplicates a capability set and drops the
// unspecified member, so two spellings of the same set hash alike.
func normaliseCaps(caps []dholev1.Capability) []dholev1.Capability {
	out := make([]dholev1.Capability, 0, len(caps))
	for _, c := range caps {
		if c == dholev1.Capability_CAPABILITY_UNSPECIFIED {
			continue
		}
		out = append(out, c)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// maxAdvertisedCaps bounds how many capabilities an engine may advertise before
// subscribing to every satisfiable subset stops being sensible. The enum has
// four members today; the guard exists so a future one that grows it fails
// loudly here instead of quietly creating a thousand consumers.
const maxAdvertisedCaps = 8

// satisfiableCapsHashes is every capability set this engine could serve: each
// subset of what it advertises. Filtering happens at the bus rather than by
// receiving work and rejecting it, so an engine that cannot grant a capability
// is never handed a step that needs one.
func satisfiableCapsHashes(caps []dholev1.Capability) ([]string, error) {
	granted := normaliseCaps(caps)
	if len(granted) > maxAdvertisedCaps {
		return nil, fmt.Errorf("engine: %d capabilities advertised, at most %d supported",
			len(granted), maxAdvertisedCaps)
	}
	hashes := make([]string, 0, 1<<len(granted))
	for mask := 0; mask < 1<<len(granted); mask++ {
		subset := make([]dholev1.Capability, 0, len(granted))
		for i, c := range granted {
			if mask&(1<<i) != 0 {
				subset = append(subset, c)
			}
		}
		hashes = append(hashes, CapsHash(subset))
	}
	return hashes, nil
}

// protocolVersions is every version this build speaks, oldest first. The
// control plane picks the highest both sides support, so an engine that
// advertised only its newest would force a flag-day upgrade of the fleet.
func protocolVersions() []uint32 {
	oldest := uint32(1)
	if wire.ProtocolVersion > wire.SupportedWindow {
		oldest = wire.ProtocolVersion - wire.SupportedWindow
	}
	versions := make([]uint32, 0, wire.ProtocolVersion-oldest+1)
	for v := oldest; v <= wire.ProtocolVersion; v++ {
		versions = append(versions, v)
	}
	return versions
}

// registryClient is the engine's outbound half of the registry protocol: the
// registration it announces on start, the heartbeat it repeats, and the set of
// jobs those heartbeats declare. Nothing here ever dials in the other
// direction.
type registryClient struct {
	bus         bus.Bus
	engineID    string
	tier        string
	slots       uint32
	caps        []dholev1.Capability
	engineTypes []string

	// nudge asks for a heartbeat now rather than at the next tick. A job that
	// has just been accepted is already the engine's responsibility, and
	// leaving it invisible for up to five seconds widens the window in which
	// the plane believes nobody holds the step.
	nudge chan struct{}

	mu       sync.Mutex
	inFlight map[string]*dholev1.InFlight
}

func newRegistryClient(cfg Config) *registryClient {
	return &registryClient{
		bus:         cfg.Bus,
		engineID:    cfg.EngineID,
		tier:        cfg.Tier,
		slots:       uint32(cfg.Slots), // #nosec G115 -- New rejects a non-positive Slots.
		caps:        normaliseCaps(cfg.Executor.Capabilities()),
		engineTypes: []string{cfg.Executor.Kind()},
		nudge:       make(chan struct{}, 1),
		inFlight:    map[string]*dholev1.InFlight{},
	}
}

// register announces this engine. The scheduler matches a step's requirements
// against exactly these fields, so an engine that overstates them is handed
// work it cannot run.
func (r *registryClient) register(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	reg := &dholev1.EngineRegistration{
		EngineId:         r.engineID,
		ProtocolVersions: protocolVersions(),
		Capabilities:     r.caps,
		Os:               runtime.GOOS,
		Arch:             runtime.GOARCH,
		Slots:            r.slots,
		EngineTypes:      r.engineTypes,
		Tier:             r.tier,
	}
	// Framed, never bare: the plane must be able to tell a registration from a
	// heartbeat by its bytes alone (docs/wire-contract.md, "Message framing").
	if err := r.bus.Publish(ctx, bus.SubjectEngineRegistration(), wire.FrameRegistration(reg)); err != nil {
		return fmt.Errorf("engine: register %q: %w", r.engineID, err)
	}
	return nil
}

// hold records a job as this engine's responsibility, echoing the dispatch's
// fence token unchanged. An invented or omitted fence would make the plane
// unable to tell this attempt from a superseded one.
func (r *registryClient) hold(d *dholev1.JobDispatch) string {
	key := jobKey(d.GetRunId(), d.GetStepId(), d.GetAttempt())
	r.mu.Lock()
	r.inFlight[key] = &dholev1.InFlight{
		RunId:      d.GetRunId(),
		StepId:     d.GetStepId(),
		Attempt:    d.GetAttempt(),
		FenceToken: d.GetFenceToken(),
	}
	r.mu.Unlock()
	select {
	case r.nudge <- struct{}{}:
	default:
	}
	return key
}

// release forgets a job once its terminal status is on the wire.
func (r *registryClient) release(key string) {
	r.mu.Lock()
	delete(r.inFlight, key)
	r.mu.Unlock()
}

// snapshot is what a heartbeat declares. The entries are freshly built so no
// caller can mutate what a previous heartbeat reported.
func (r *registryClient) snapshot() []*dholev1.InFlight {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*dholev1.InFlight, 0, len(r.inFlight))
	for _, f := range r.inFlight {
		out = append(out, &dholev1.InFlight{
			RunId:      f.GetRunId(),
			StepId:     f.GetStepId(),
			Attempt:    f.GetAttempt(),
			FenceToken: f.GetFenceToken(),
		})
	}
	slices.SortFunc(out, func(a, b *dholev1.InFlight) int {
		return cmpString(jobKey(a.GetRunId(), a.GetStepId(), a.GetAttempt()),
			jobKey(b.GetRunId(), b.GetStepId(), b.GetAttempt()))
	})
	return out
}

// heartbeat publishes one proof of life. A failed heartbeat is not fatal: the
// plane's remedy is to expire the lease and re-dispatch, and an engine that
// exited on a transient publish error would abandon work it is still running.
func (r *registryClient) heartbeat(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	beat := &dholev1.EngineHeartbeat{EngineId: r.engineID, InFlight: r.snapshot()}
	return r.bus.Publish(ctx, bus.SubjectEngineHeartbeat(r.engineID), wire.FrameHeartbeat(beat))
}

// run heartbeats, and re-announces this engine, until ctx is done.
//
// A failed re-announcement is not fatal for the same reason a failed heartbeat
// is not: the next one is along shortly, and an engine that exited on a
// transient publish error would abandon work it is still running.
func (r *registryClient) run(ctx context.Context) {
	beats := time.NewTicker(HeartbeatInterval)
	defer beats.Stop()
	announcements := time.NewTicker(RegistrationInterval)
	defer announcements.Stop()
	// One immediately, so the plane does not wait a full interval to learn
	// this engine exists.
	_ = r.heartbeat(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-announcements.C:
			// Before the beat that follows it: the plane admits an engine on a
			// registration and promotes it on a heartbeat, so announcing
			// second would leave it registering for another five seconds.
			_ = r.register(ctx)
			continue
		case <-beats.C:
		case <-r.nudge:
		}
		_ = r.heartbeat(ctx)
	}
}

func jobKey(runID, stepID string, attempt uint32) string {
	return fmt.Sprintf("%s/%s/%d", runID, stepID, attempt)
}

func cmpString(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
