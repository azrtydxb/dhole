package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// A run is a state machine over an event log, so two planes advancing the same
// run write two versions of what happened next. The store allocates the
// sequence in its own transaction, so a collision no longer silently drops an
// event — but both planes still dispatch the same step, under two fences, and
// the loser's work is thrown away after it has already run somewhere.
//
// Partitioning by run id is what makes exactly one plane responsible. The run
// id hashes to a partition, a plane claims partitions, and a plane advances
// only the runs whose partition it holds (ADR 0003: "consumers partition by
// run id so exactly one instance advances a given run at a time").
//
// The claims live in NATS KV, following the same pattern as internal/lease: a
// record carrying its own deadline, taken over with a revision-checked write so
// the SERVER picks the winner when two planes reach for the same partition at
// the same moment. There is no client-side agreement protocol here and no
// coordinator — the bucket is the only thing the planes share.

// PartitionCount is how many partitions the run-id space is divided into. It is
// fixed rather than derived from the number of planes because it is part of
// what a claim means: a partition has to name the same set of runs to every
// plane, including the one that has not started yet. Sixty-four leaves room to
// scale out well past any plane count this system expects while keeping each
// plane's share of a small deployment even.
const PartitionCount = 64

// PartitionBucket is the KV bucket partition claims and plane membership live
// in.
const PartitionBucket = "dhole-partitions"

// The bucket holds two kinds of key per deployment:
//
//	part.<deployment>.<partition>   the claim on one partition
//	member.<deployment>.<instance>  the plane, proving it is still alive
//
// Membership is separate from the claims because a plane has to be counted
// before it holds anything: an instance that has just started owns no partition
// and would otherwise be invisible, so the planes already holding the ring
// would never work out that they should give any of it back.
const (
	claimKeyPrefix  = "part."
	memberKeyPrefix = "member."
)

// defaultPartitionTTL is how long a claim and a membership stand without being
// renewed. It bounds how long a dead plane's runs sit unadvanced, so it is
// short; it is not shorter because every renewal is a write to the bucket and
// the whole ring is renewed at once.
const defaultPartitionTTL = 6 * time.Second

// partitionRenewInterval is how often a running plane renews. It is a third of
// the TTL so two renewals can be lost to a slow bus before the fleet decides
// the plane is gone.
const partitionRenewInterval = defaultPartitionTTL / 3

// PartitionFor is the run id's partition. It is the only place a run is mapped
// to an owner, and it must be a pure function of the id: a run whose partition
// depended on anything else — the time, the plane asking, the set of planes
// alive — would change hands between two advances, and both planes would write
// it.
//
// FNV-1a because it is stable across processes and builds (unlike Go's map
// hash, which is randomly seeded per process — using that here would give two
// planes two different answers for the same run) and because it spreads
// sequential and near-identical ids, which is what run ids are.
func PartitionFor(runID string, n int) int {
	if n <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(runID))
	return int(h.Sum64() % uint64(n)) //nolint:gosec // a modulus by n is in [0,n)
}

// PartitionOption tunes a Partitioner.
type PartitionOption func(*partitionOptions)

type partitionOptions struct {
	ttl   time.Duration
	count int
	now   func() time.Time
}

// WithPartitionTTL sets how long a claim and a membership stand without being
// renewed. It is the delay between a plane dying and its runs moving.
func WithPartitionTTL(d time.Duration) PartitionOption {
	return func(o *partitionOptions) { o.ttl = d }
}

// Partitioner is one plane's view of its deployment's partition ring.
//
// It is scoped to a DEPLOYMENT and not to a tenant. A partition holds whatever
// runs hash into it, whoever they belong to — the ownership question is "which
// plane advances this run", and that has no tenant in it. The deployment is in
// every key because two deployments sharing one NATS are two separate rings:
// each owns all sixty-four partitions of its own, and neither may be talked out
// of a partition because the other one exists. It is the same reason
// Config.DeploymentID scopes the outbox claim.
type Partitioner struct {
	kv           jetstream.KeyValue
	deploymentID string
	count        int
	ttl          time.Duration
	now          func() time.Time

	// held is read on the advance path, once per run, so it is kept as an
	// immutable map replaced wholesale under mu rather than mutated.
	mu   sync.RWMutex
	held map[int]struct{}
}

// claimRecord is what a claim or a membership looks like at rest. The deadline
// is in the value rather than being the bucket's max age because a plane has to
// be able to tell an expired claim from a missing one: taking over an expired
// claim is a revision-checked write against the key that is still there, which
// is what makes exactly one taker win.
type partitionRecord struct {
	DeploymentID string `json:"deployment_id"`
	InstanceID   string `json:"instance_id"`
	Partition    int    `json:"partition,omitempty"`
	ExpiresAt    int64  `json:"expires_at_unix_nano"`
}

// NewPartitioner binds the partition bucket on conn for one deployment,
// creating it if it is not there yet.
func NewPartitioner(ctx context.Context, conn *nats.Conn, deploymentID string, opts ...PartitionOption) (*Partitioner, error) {
	if deploymentID == "" {
		return nil, errors.New("server: partitioning needs a deployment id")
	}
	cfg := partitionOptions{ttl: defaultPartitionTTL, count: PartitionCount, now: time.Now}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.ttl <= 0 {
		return nil, errors.New("server: partition ttl must be positive")
	}
	if cfg.count <= 0 {
		return nil, errors.New("server: partition count must be positive")
	}

	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("server: partitions: jetstream: %w", err)
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      PartitionBucket,
		Description: "Control-plane partition claims. Each key carries its own deadline.",
		History:     1,
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("server: partitions: bucket %q: %w", PartitionBucket, err)
	}
	return &Partitioner{
		kv:           kv,
		deploymentID: deploymentID,
		count:        cfg.count,
		ttl:          cfg.ttl,
		now:          cfg.now,
		held:         map[int]struct{}{},
	}, nil
}

// ClaimPartitions renews what this instance holds, gives back whatever is more
// than its fair share, takes whatever nobody live is holding, and returns the
// partitions it owns afterwards, in order.
//
// It is the whole protocol, and it is deliberately one call: announcing
// membership, expiring the dead, releasing surplus and claiming free capacity
// have to happen against ONE reading of the bucket, or a plane can hold a
// partition it has already decided to give up.
//
// It converges rather than agreeing. A plane that was alone holds the whole
// ring, and it is its NEXT claim — after a joiner has announced itself — that
// hands the surplus back. Nothing blocks in the meantime, and nobody waits for
// a plane that may never answer.
func (p *Partitioner) ClaimPartitions(ctx context.Context, instanceID string) ([]int, error) {
	if instanceID == "" {
		return nil, errors.New("server: claiming partitions needs an instance id")
	}
	if err := p.announce(ctx, instanceID); err != nil {
		return nil, err
	}
	members, err := p.liveMembers(ctx, instanceID)
	if err != nil {
		return nil, err
	}
	owners, err := p.owners(ctx)
	if err != nil {
		return nil, err
	}

	// Ceiling, so the shares of every plane still cover the ring: with three
	// planes and sixty-four partitions, floor would leave one unowned and the
	// runs in it unadvanced by anybody.
	share := (p.count + len(members) - 1) / len(members)

	mine := make([]int, 0, share)
	for part := range p.count {
		if owner, ok := owners[part]; ok && owner.instance == instanceID && !p.expired(owner) {
			mine = append(mine, part)
		}
	}
	slices.Sort(mine)

	// The surplus goes back FIRST, and the local view drops it before the
	// write is even attempted: a partition this plane has decided is somebody
	// else's must stop being advanced here whether or not the release
	// succeeds.
	for len(mine) > share {
		part := mine[len(mine)-1]
		mine = mine[:len(mine)-1]
		p.forget(part)
		p.release(ctx, part, owners[part].revision)
	}

	kept := mine[:0]
	for _, part := range mine {
		if p.write(ctx, p.claimKey(part), partitionRecord{
			DeploymentID: p.deploymentID,
			InstanceID:   instanceID,
			Partition:    part,
			ExpiresAt:    p.deadline(),
		}, owners[part].revision, true) {
			kept = append(kept, part)
			continue
		}
		// The renewal was refused, which means the key moved under us: this
		// plane is not the owner any more and must stop advancing its runs.
		p.forget(part)
	}
	mine = kept

	// Start where this plane's share begins rather than at zero, so planes
	// reaching for free partitions at the same moment mostly do not collide.
	// Colliding is safe — the server picks one — but it wastes a pass.
	start := slices.Index(members, instanceID) * p.count / len(members)
	for offset := range p.count {
		if len(mine) >= share {
			break
		}
		part := (start + offset) % p.count
		owner, taken := owners[part]
		if slices.Contains(mine, part) {
			continue
		}
		if taken && !p.expired(owner) {
			continue
		}
		if p.write(ctx, p.claimKey(part), partitionRecord{
			DeploymentID: p.deploymentID,
			InstanceID:   instanceID,
			Partition:    part,
			ExpiresAt:    p.deadline(),
		}, owner.revision, taken) {
			mine = append(mine, part)
		}
	}

	slices.Sort(mine)
	p.publish(mine)
	return slices.Clone(mine), nil
}

// Owns reports whether this instance is the plane responsible for runID. It is
// consulted before EVERY run the advance loop touches, not once per pass: a
// plane that loses a partition has to stop at the next run rather than finish
// the pass it began, because every run it advances after the handover is a
// second writer.
func (p *Partitioner) Owns(runID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.held[PartitionFor(runID, p.count)]
	return ok
}

// Held is the partitions this instance currently owns, in order.
func (p *Partitioner) Held() []int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]int, 0, len(p.held))
	for part := range p.held {
		out = append(out, part)
	}
	slices.Sort(out)
	return out
}

func (p *Partitioner) publish(parts []int) {
	held := make(map[int]struct{}, len(parts))
	for _, part := range parts {
		held[part] = struct{}{}
	}
	p.mu.Lock()
	p.held = held
	p.mu.Unlock()
}

// forget drops one partition from the local view immediately, without waiting
// for the next full claim. This is the promptness half of single-writer.
func (p *Partitioner) forget(part int) {
	p.mu.Lock()
	held := make(map[int]struct{}, len(p.held))
	for existing := range p.held {
		if existing != part {
			held[existing] = struct{}{}
		}
	}
	p.held = held
	p.mu.Unlock()
}

// announce writes this plane's membership, resetting its deadline. It is a
// plain Put: a membership has no contended owner, and the last writer is by
// definition the plane itself.
func (p *Partitioner) announce(ctx context.Context, instanceID string) error {
	data, err := json.Marshal(partitionRecord{
		DeploymentID: p.deploymentID,
		InstanceID:   instanceID,
		ExpiresAt:    p.deadline(),
	})
	if err != nil {
		return fmt.Errorf("server: partitions: encoding membership: %w", err)
	}
	if _, err := p.kv.Put(ctx, p.memberKey(instanceID), data); err != nil {
		return fmt.Errorf("server: partitions: announcing %q: %w", instanceID, err)
	}
	return nil
}

// liveMembers is every plane of THIS deployment whose membership has not
// lapsed, sorted, with this instance always in it — it has just announced
// itself, and a bucket read that raced the write must not make this plane
// invisible to its own arithmetic.
func (p *Partitioner) liveMembers(ctx context.Context, instanceID string) ([]string, error) {
	records, err := p.records(ctx, memberKeyPrefix+encodePartitionSegment(p.deploymentID)+".")
	if err != nil {
		return nil, err
	}
	members := []string{instanceID}
	for _, rec := range records {
		if rec.record.InstanceID == instanceID || p.expired(rec) {
			continue
		}
		members = append(members, rec.record.InstanceID)
	}
	slices.Sort(members)
	return slices.Compact(members), nil
}

// owners is every partition of this deployment that somebody has claimed,
// whether or not the claim is still good — an expired claim is kept because
// taking it over is a write against the key that is still there.
func (p *Partitioner) owners(ctx context.Context) (map[int]ownership, error) {
	records, err := p.records(ctx, claimKeyPrefix+encodePartitionSegment(p.deploymentID)+".")
	if err != nil {
		return nil, err
	}
	out := make(map[int]ownership, len(records))
	for _, rec := range records {
		if rec.record.Partition < 0 || rec.record.Partition >= p.count {
			continue
		}
		out[rec.record.Partition] = ownership{
			instance:  rec.record.InstanceID,
			expiresAt: rec.record.ExpiresAt,
			revision:  rec.revision,
		}
	}
	return out, nil
}

type ownership struct {
	instance  string
	expiresAt int64
	revision  uint64
}

func (o ownership) deadline() int64 { return o.expiresAt }

type keyedRecord struct {
	record   partitionRecord
	revision uint64
}

func (r keyedRecord) deadline() int64 { return r.record.ExpiresAt }

// expirable is whatever carries a deadline, so the one expiry rule serves both
// claims and memberships.
type expirable interface{ deadline() int64 }

func (p *Partitioner) expired(e expirable) bool {
	return p.now().After(time.Unix(0, e.deadline()))
}

func (p *Partitioner) deadline() int64 { return p.now().Add(p.ttl).UnixNano() }

// records reads every key under prefix. A key that vanishes between the
// listing and the read is skipped: somebody released it, which is exactly the
// state this read is trying to observe.
func (p *Partitioner) records(ctx context.Context, prefix string) ([]keyedRecord, error) {
	keys, err := p.kv.ListKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("server: partitions: listing %s: %w", PartitionBucket, err)
	}
	defer func() { _ = keys.Stop() }()

	var out []keyedRecord
	for key := range keys.Keys() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		entry, err := p.kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("server: partitions: reading %s: %w", key, err)
		}
		var rec partitionRecord
		if err := json.Unmarshal(entry.Value(), &rec); err != nil {
			return nil, fmt.Errorf("server: partitions: decoding %s: %w", key, err)
		}
		// The prefix is what scopes this read; the deployment in the record is
		// the same fact written twice, and a disagreement means something wrote
		// a key it had no business writing. Refusing loudly beats a listing
		// that looks right over a corrupt bucket.
		if rec.DeploymentID != p.deploymentID {
			return nil, fmt.Errorf("server: partitions: key %s holds deployment %q, not %q",
				key, rec.DeploymentID, p.deploymentID)
		}
		out = append(out, keyedRecord{record: rec, revision: entry.Revision()})
	}
	return out, nil
}

// write puts a claim, either creating the key or replacing it at the exact
// revision this pass read. Losing that race is a normal outcome and not an
// error: somebody else got the partition, and this plane simply does not have
// it. It reports whether the write landed.
func (p *Partitioner) write(ctx context.Context, key string, rec partitionRecord, revision uint64, exists bool) bool {
	data, err := json.Marshal(rec)
	if err != nil {
		return false
	}
	if exists {
		_, err = p.kv.Update(ctx, key, data, revision)
	} else {
		_, err = p.kv.Create(ctx, key, data)
	}
	return err == nil
}

// release drops a claim at the revision this pass read, so a plane that has
// already been superseded cannot delete the new owner's claim.
func (p *Partitioner) release(ctx context.Context, part int, revision uint64) {
	_ = p.kv.Delete(ctx, p.claimKey(part), jetstream.LastRevision(revision))
}

// claimKey and memberKey are where deployment scoping is enforced. The
// deployment is always the first segment and it is encoded, so a deployment id
// containing a dot cannot pose as another deployment's key.
func (p *Partitioner) claimKey(part int) string {
	return claimKeyPrefix + encodePartitionSegment(p.deploymentID) + "." + strconv.Itoa(part)
}

func (p *Partitioner) memberKey(instanceID string) string {
	return memberKeyPrefix + encodePartitionSegment(p.deploymentID) + "." + encodePartitionSegment(instanceID)
}

// encodePartitionSegment keeps identifiers inside the character set NATS KV
// keys allow, without losing the boundary between segments.
func encodePartitionSegment(segment string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(segment))
}

// ---------------------------------------------------------------------------
// The plane's own membership.
// ---------------------------------------------------------------------------

// planePartitions is this process's place in the ring: the partitioner and the
// instance id it claims under. The id is per PROCESS, not per deployment —
// every process of one plane shares the DeploymentID and must not share this,
// or two of them would renew each other's claims and both advance the same run.
type planePartitions struct {
	parts      *Partitioner
	instanceID string
}

// startPartitioning claims this process's share before the advance loop runs.
// Doing it synchronously matters: a plane that started advancing before its
// first claim would either advance nothing (a puzzling stall) or, worse,
// advance everything.
func startPartitioning(ctx context.Context, conn *nats.Conn, deploymentID string) (*planePartitions, error) {
	parts, err := NewPartitioner(ctx, conn, deploymentID)
	if err != nil {
		return nil, err
	}
	pp := &planePartitions{parts: parts, instanceID: deploymentID + "-" + randomID()}
	if _, err := parts.ClaimPartitions(ctx, pp.instanceID); err != nil {
		return nil, err
	}
	return pp, nil
}

// renewLoop keeps this plane's claims and membership alive, and is also what
// notices a joiner and hands it a share. It stops with the server: the claims
// then lapse on their own deadline and somebody else takes them, which is the
// same path a crash takes.
func (s *Server) renewLoop(ctx context.Context) {
	if s.partitions == nil {
		return
	}
	ticker := time.NewTicker(partitionRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if _, err := s.partitions.parts.ClaimPartitions(ctx, s.partitions.instanceID); err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("claiming partitions", "instance", s.partitions.instanceID, "error", err)
		}
	}
}

// ownsRun is the advance loop's guard. A plane with no partitioner owns
// everything: that is the single-process deployment, where there is nobody to
// share with and a ring would be ceremony.
func (s *Server) ownsRun(runID string) bool {
	if s.partitions == nil {
		return true
	}
	return s.partitions.parts.Owns(runID)
}
