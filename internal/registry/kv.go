package registry

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/wire"
)

// Bucket is the KV bucket every live engine instance lives in.
const Bucket = "dhole-engines"

// instancePrefix keeps engine keys distinguishable from anything else that ever
// shares the bucket.
const instancePrefix = "engine."

// KV is the NATS KV implementation of Registry.
//
// The TTL is not stored per record and not swept by anything in this process:
// it is the bucket's own max age. Every Register and every Heartbeat rewrites
// the instance's key, which resets that key's age, so an engine survives
// exactly as long as it keeps proving it is alive. When it stops, the SERVER
// drops the key. That is the whole expiry mechanism, and it is on the server on
// purpose — a client-side sweep is a job that has to be scheduled, has to keep
// running, and is wrong the moment the control plane it runs in dies, which is
// precisely when engines are most likely to be orphaned.
type KV struct {
	kv jetstream.KeyValue
	// tenantID scopes every write this handle makes. It comes from the
	// connection the registrations arrive on rather than from the
	// EngineRegistration, because an engine does not get to name the tenant it
	// belongs to.
	tenantID string
	ttl      time.Duration
}

var _ Registry = (*KV)(nil)

// record is what one instance looks like at rest. It holds the full
// advertisement, because that is what the scheduler matches against and a
// heartbeat cannot restate it.
type record struct {
	TenantID         string   `json:"tenant_id"`
	EngineID         string   `json:"engine_id"`
	State            State    `json:"state"`
	Capabilities     []int32  `json:"capabilities"`
	OS               string   `json:"os"`
	Arch             string   `json:"arch"`
	Slots            int      `json:"slots"`
	ProtocolVersions []uint32 `json:"protocol_versions"`
	// Negotiated is the version the plane and this engine agreed on. It is
	// recorded rather than recomputed so a change to the compatibility window
	// cannot silently reinterpret a live registration.
	Negotiated  uint32   `json:"negotiated_version"`
	EngineTypes []string `json:"engine_types"`
	Tier        string   `json:"tier"`
	// InFlight is how many jobs the last heartbeat declared. A draining engine
	// is gone the moment this reaches zero.
	InFlight int `json:"in_flight"`
	// Jobs is WHICH jobs that heartbeat declared. The count above decides the
	// drain; this decides where a cancellation is sent, which nothing else in
	// the system can answer.
	Jobs []job `json:"jobs,omitempty"`
}

// job is one in-flight attempt at rest.
type job struct {
	RunID      string `json:"run_id"`
	StepID     string `json:"step_id"`
	Attempt    uint32 `json:"attempt"`
	FenceToken string `json:"fence_token"`
}

// New binds the engine bucket on conn for one tenant, creating it if it is not
// there yet, with ttl as the heartbeat deadline every instance is held to.
func New(ctx context.Context, conn *nats.Conn, tenantID string, ttl time.Duration) (*KV, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("registry: %w", ErrTenantRequired)
	}
	if ttl <= 0 {
		return nil, errors.New("registry: heartbeat ttl must be positive")
	}
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("registry: jetstream: %w", err)
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      Bucket,
		Description: "Live engine instances. A key that stops being rewritten ages out.",
		History:     1,
		TTL:         ttl,
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("registry: bucket %q: %w", Bucket, err)
	}
	return &KV{kv: kv, tenantID: tenantID, ttl: ttl}, nil
}

// TTL is the heartbeat deadline instances in this registry are held to.
func (k *KV) TTL() time.Duration { return k.ttl }

// Register admits an engine in state registering, replacing whatever was there
// under the same id: a restarted engine re-registering is the normal case, and
// its new advertisement is the true one.
//
// The protocol version is negotiated BEFORE anything is written. An engine the
// plane cannot talk to never becomes a fleet member, so the mismatch surfaces
// at registration instead of at dispatch, when a step is already committed to
// it. The window itself is wire's to define — duplicating it here is how the
// two would drift apart.
func (k *KV) Register(ctx context.Context, r *dholev1.EngineRegistration) error {
	if r.GetEngineId() == "" {
		return fmt.Errorf("registry: register: %w", ErrEngineRequired)
	}
	negotiated, err := wire.Negotiate(r.GetProtocolVersions())
	if err != nil {
		return fmt.Errorf("registry: register %q: %w", r.GetEngineId(), err)
	}
	if r.GetSlots() > math.MaxInt32 {
		return fmt.Errorf("registry: register %q: %d slots is not a plausible capacity",
			r.GetEngineId(), r.GetSlots())
	}

	rec := record{
		TenantID:         k.tenantID,
		EngineID:         r.GetEngineId(),
		State:            StateRegistering,
		Capabilities:     capabilityNumbers(r.GetCapabilities()),
		OS:               r.GetOs(),
		Arch:             r.GetArch(),
		Slots:            int(r.GetSlots()),
		ProtocolVersions: slices.Clone(r.GetProtocolVersions()),
		Negotiated:       negotiated,
		EngineTypes:      slices.Clone(r.GetEngineTypes()),
		Tier:             r.GetTier(),
	}
	return k.put(ctx, rec)
}

// Heartbeat renews an instance's lease on being alive and records what it
// holds. It is the only thing that resets the TTL after registration, so an
// engine that goes quiet ages out however healthy it believes itself to be.
//
// The state machine it drives is small and one-directional: registering
// becomes ready, ready stays ready, and draining stays draining until the
// heartbeat that holds nothing, which is the engine reporting its drain
// complete.
func (k *KV) Heartbeat(ctx context.Context, h *dholev1.EngineHeartbeat) error {
	if h.GetEngineId() == "" {
		return fmt.Errorf("registry: heartbeat: %w", ErrEngineRequired)
	}
	rec, _, err := k.get(ctx, k.tenantID, h.GetEngineId())
	if err != nil {
		return err
	}
	if rec.State == StateGone {
		return fmt.Errorf("registry: heartbeat %q: %w", h.GetEngineId(), ErrNotRegistered)
	}

	rec.InFlight = len(h.GetInFlight())
	rec.Jobs = make([]job, 0, rec.InFlight)
	for _, held := range h.GetInFlight() {
		rec.Jobs = append(rec.Jobs, job{
			RunID:      held.GetRunId(),
			StepID:     held.GetStepId(),
			Attempt:    held.GetAttempt(),
			FenceToken: held.GetFenceToken(),
		})
	}
	switch rec.State {
	case StateDraining:
		// A drain finishes when the last job does, and not before. Nothing
		// here interrupts the work; the engine's own heartbeat says when it
		// has let go of everything.
		if rec.InFlight == 0 {
			rec.State = StateGone
		}
	case StateRegistering:
		// Readiness is proof, not a claim: an engine is dispatchable once it
		// has heartbeaten at least once under its own hand.
		rec.State = StateReady
	case StateReady, StateGone:
	}
	return k.put(ctx, rec)
}

// Instances is the live fleet for one tenant.
//
// Gone instances are filtered rather than deleted. The tombstone is what makes
// a late heartbeat from an exited engine an error instead of a resurrection,
// and it ages out with everything else, so the filter costs one comparison and
// the alternative costs a delete path that has to be correct under a crash.
func (k *KV) Instances(ctx context.Context, tenantID string) ([]Instance, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("registry: instances: %w", ErrTenantRequired)
	}
	keys, err := k.kv.ListKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("registry: listing %s: %w", Bucket, err)
	}
	defer func() { _ = keys.Stop() }()

	prefix := tenantPrefix(tenantID)
	var out []Instance
	for key := range keys.Keys() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		entry, err := k.kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue // aged out between the listing and the read
		}
		if err != nil {
			return nil, fmt.Errorf("registry: reading %s: %w", key, err)
		}
		var rec record
		if err := json.Unmarshal(entry.Value(), &rec); err != nil {
			return nil, fmt.Errorf("registry: decoding %s: %w", key, err)
		}
		// The key prefix is what scopes this listing; the tenant in the record
		// is the same fact written twice, and a disagreement means something
		// wrote a key it had no business writing. That is a bug to shout
		// about, not one to skip past quietly: skipping would leave the
		// listing looking correct while the bucket was corrupt.
		if rec.TenantID != tenantID {
			return nil, fmt.Errorf("registry: key %s holds tenant %q, not %q", key, rec.TenantID, tenantID)
		}
		if rec.State == StateGone {
			continue
		}
		out = append(out, rec.instance())
	}
	slices.SortFunc(out, func(a, b Instance) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

// Drain stops new work reaching an engine and stops nothing else. The instance
// stays in the fleet, alive and heartbeating, until its last job finishes:
// scheduler.Match refuses to dispatch to anything that is not ready, so no
// running step has to be killed for a rolling upgrade to proceed.
//
// It is idempotent, and an engine the registry does not hold is not an error —
// an operator draining from a list a few seconds old is the normal case, and
// the engine being gone already is the outcome they wanted.
func (k *KV) Drain(ctx context.Context, engineID string) error {
	if engineID == "" {
		return nil
	}
	rec, _, err := k.get(ctx, k.tenantID, engineID)
	if errors.Is(err, ErrNotRegistered) {
		return nil
	}
	if err != nil {
		return err
	}
	if rec.State == StateDraining || rec.State == StateGone {
		return nil
	}
	rec.State = StateDraining
	return k.put(ctx, rec)
}

// get reads one instance. A missing key is ErrNotRegistered: it aged out, and
// there is nothing here that can tell that apart from an engine that never
// registered — nor does anything need to, since the remedy is the same.
func (k *KV) get(ctx context.Context, tenantID, engineID string) (record, uint64, error) {
	entry, err := k.kv.Get(ctx, instanceKey(tenantID, engineID))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return record{}, 0, fmt.Errorf("registry: %q: %w", engineID, ErrNotRegistered)
	}
	if err != nil {
		return record{}, 0, fmt.Errorf("registry: reading %q: %w", engineID, err)
	}
	var rec record
	if err := json.Unmarshal(entry.Value(), &rec); err != nil {
		return record{}, 0, fmt.Errorf("registry: decoding %q: %w", engineID, err)
	}
	return rec, entry.Revision(), nil
}

// put writes an instance and, in doing so, resets its TTL. Every path that
// touches an instance goes through here, so there is no way to update one
// without also renewing it — and no way to renew one without saying what it is.
func (k *KV) put(ctx context.Context, rec record) error {
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("registry: encoding %q: %w", rec.EngineID, err)
	}
	if _, err := k.kv.Put(ctx, instanceKey(rec.TenantID, rec.EngineID), data); err != nil {
		return fmt.Errorf("registry: writing %q: %w", rec.EngineID, err)
	}
	return nil
}

// instance is the public view of a record.
func (r record) instance() Instance {
	caps := make([]dholev1.Capability, 0, len(r.Capabilities))
	for _, c := range r.Capabilities {
		caps = append(caps, dholev1.Capability(c))
	}
	jobs := make([]Job, 0, len(r.Jobs))
	for _, held := range r.Jobs {
		jobs = append(jobs, Job(held))
	}
	return Instance{
		ID:               r.EngineID,
		InFlight:         jobs,
		State:            r.State,
		Capabilities:     caps,
		OS:               r.OS,
		Arch:             r.Arch,
		Slots:            r.Slots,
		ProtocolVersions: slices.Clone(r.ProtocolVersions),
	}
}

// capabilityNumbers stores capabilities as the enum NUMBERS the wire carries,
// not Go's names for them: a record written by this build must still be
// readable after the enum is renamed, and the wire is the contract.
func capabilityNumbers(caps []dholev1.Capability) []int32 {
	out := make([]int32, 0, len(caps))
	for _, c := range caps {
		out = append(out, int32(c))
	}
	return out
}

// instanceKey is where tenant scoping is enforced. The tenant is always the
// first segment and every segment is encoded, so an engine id containing a dot
// cannot pose as another tenant's key, and two tenants naming an engine
// identically hold two independent instances.
func instanceKey(tenantID, engineID string) string {
	return tenantPrefix(tenantID) + encode(engineID)
}

func tenantPrefix(tenantID string) string {
	return instancePrefix + encode(tenantID) + "."
}

// encode keeps identifiers inside the character set NATS KV keys allow, without
// losing the boundary between segments.
func encode(segment string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(segment))
}
