package lease

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Bucket is the KV bucket every lease lives in.
const Bucket = "dhole-leases"

// The bucket holds two keys per leased step:
//
//	lease.<tenant>.<run>.<step>   the claim. Its REVISION is the fence.
//	renew.<tenant>.<run>.<step>   the heartbeat, rewritten on every Renew.
//
// They are separate because the fence must not move when a holder proves it is
// alive: renewing in place would bump the revision and invalidate the very
// token the engine is carrying. Splitting them lets the fence be immutable for
// the life of an attempt while the deadline moves freely.
const (
	claimPrefix = "lease."
	renewPrefix = "renew."
)

// KV is the NATS KV implementation of Manager.
type KV struct {
	kv  jetstream.KeyValue
	now func() time.Time
}

// claimRecord is the value stored under a claim key. The fence is not in it —
// the fence is the key's revision, and duplicating it in the value would create
// a second source of truth that could disagree with the server.
type claimRecord struct {
	TenantID  string `json:"tenant_id"`
	RunID     string `json:"run_id"`
	StepID    string `json:"step_id"`
	Attempt   uint32 `json:"attempt"`
	TTLNanos  int64  `json:"ttl_nanos"`
	ExpiresAt int64  `json:"expires_at_unix_nano"`
	// Offered marks a lease taken for a dispatch put on a work queue. Until a
	// renewal for its fence exists nobody holds it, and ExpiresAt is ignored:
	// the attempt is waiting for an engine slot, not overdue. ExpiresAt is
	// still written as claim time plus TTL so that a plane from before this
	// field — one that cannot tell an offer from a claim — goes on expiring it
	// as it always did during a rolling upgrade, rather than reading zero as a
	// deadline long past and sweeping every fresh dispatch at once.
	Offered bool `json:"offered,omitempty"`
}

// renewRecord is the value stored under a renew key. It names the fence it
// belongs to, so a renewal written by a holder that has since been superseded
// can never extend the new holder's deadline.
type renewRecord struct {
	Fence     uint64 `json:"fence"`
	ExpiresAt int64  `json:"expires_at_unix_nano"`
}

// New binds the lease bucket on conn, creating it if it is not there yet.
func New(ctx context.Context, conn *nats.Conn) (*KV, error) {
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("lease: jetstream: %w", err)
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      Bucket,
		Description: "Step leases. Each key's revision is that step's fence token.",
		History:     1,
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("lease: bucket %q: %w", Bucket, err)
	}
	return &KV{kv: kv, now: time.Now}, nil
}

// Claim supersedes whatever held the step and returns the new fence. The write
// is a plain Put: the revision the server assigns is monotonic, so two control
// planes racing on the same step cannot come away with the same fence, and the
// later writer is by definition the current holder.
func (k *KV) Claim(ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration) (Token, error) {
	return k.claim(ctx, tenantID, runID, stepID, attempt, ttl, false)
}

// Offer is Claim for a dispatch that will wait in a work queue. The fence is
// taken now, because it travels inside the dispatch, and the heartbeat
// deadline is not: it starts at the first Renew, which is the engine saying it
// has the step. See Manager.
func (k *KV) Offer(ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration) (Token, error) {
	return k.claim(ctx, tenantID, runID, stepID, attempt, ttl, true)
}

func (k *KV) claim(
	ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration, offered bool,
) (Token, error) {
	key, err := claimKey(tenantID, runID, stepID)
	if err != nil {
		return Token{}, err
	}
	if ttl <= 0 {
		return Token{}, fmt.Errorf("lease: claim %s/%s/%s: ttl must be positive", tenantID, runID, stepID)
	}

	record := claimRecord{
		TenantID:  tenantID,
		RunID:     runID,
		StepID:    stepID,
		Attempt:   attempt,
		TTLNanos:  ttl.Nanoseconds(),
		ExpiresAt: k.now().Add(ttl).UnixNano(),
		Offered:   offered,
	}
	data, err := json.Marshal(record)
	if err != nil {
		return Token{}, fmt.Errorf("lease: encoding claim: %w", err)
	}
	fence, err := k.kv.Put(ctx, key, data)
	if err != nil {
		return Token{}, fmt.Errorf("lease: claim %s/%s/%s: %w", tenantID, runID, stepID, err)
	}
	return Token{Value: key, Fence: fence}, nil
}

// Renew extends the deadline of the lease behind t, leaving its fence where it
// is. A token that is not the current lease is refused before anything is
// written, so a superseded holder's heartbeat never props up the new holder.
//
// On an offer the first renewal is also the acceptance: only a holder renews,
// so from here the offer has a heartbeat deadline like any claim.
func (k *KV) Renew(ctx context.Context, t Token) error {
	record, err := k.current(ctx, t)
	if err != nil {
		return err
	}
	renewal := renewRecord{
		Fence:     t.Fence,
		ExpiresAt: k.now().Add(time.Duration(record.TTLNanos)).UnixNano(),
	}
	data, err := json.Marshal(renewal)
	if err != nil {
		return fmt.Errorf("lease: encoding renewal: %w", err)
	}
	if _, err := k.kv.Put(ctx, renewKeyFor(t.Value), data); err != nil {
		return fmt.Errorf("lease: renew %s: %w", t.Value, err)
	}
	return nil
}

// Validate refuses anything that is not the current lease. This is the check a
// resurrected engine's late report meets.
func (k *KV) Validate(ctx context.Context, t Token) error {
	_, err := k.current(ctx, t)
	return err
}

// current resolves t to the claim it names, or ErrFenced. A missing key is
// fenced too: the lease was swept and the step belongs to somebody else now.
func (k *KV) current(ctx context.Context, t Token) (claimRecord, error) {
	if t.Value == "" || t.Fence == 0 {
		return claimRecord{}, fmt.Errorf("%w: %q at fence %d is not a lease", ErrFenced, t.Value, t.Fence)
	}
	entry, err := k.kv.Get(ctx, t.Value)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return claimRecord{}, fmt.Errorf("%w: lease %s is gone", ErrFenced, t.Value)
	}
	if err != nil {
		return claimRecord{}, fmt.Errorf("lease: reading %s: %w", t.Value, err)
	}
	if entry.Revision() != t.Fence {
		return claimRecord{}, fmt.Errorf("%w: %s is at fence %d, token carries %d",
			ErrFenced, t.Value, entry.Revision(), t.Fence)
	}
	var record claimRecord
	if err := json.Unmarshal(entry.Value(), &record); err != nil {
		return claimRecord{}, fmt.Errorf("lease: decoding %s: %w", t.Value, err)
	}
	return record, nil
}

// Expire sweeps the leases that have passed their deadline and returns them as
// orphans for re-dispatch.
//
// Each orphan is CLAIMED by deleting its key at the exact revision this sweep
// read, so when several control planes sweep at once the server hands each dead
// step to exactly one of them. A plane that loses the race stays quiet rather
// than reporting a step somebody else is already re-dispatching.
func (k *KV) Expire(ctx context.Context) ([]Orphan, error) {
	keys, err := k.kv.ListKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("lease: listing %s: %w", Bucket, err)
	}
	defer func() { _ = keys.Stop() }()

	var orphans []Orphan
	for key := range keys.Keys() {
		if err := ctx.Err(); err != nil {
			return orphans, err
		}
		if !strings.HasPrefix(key, claimPrefix) {
			continue
		}
		orphan, ok, err := k.expireOne(ctx, key)
		if err != nil {
			return nil, err
		}
		if ok {
			orphans = append(orphans, orphan)
		}
	}
	return orphans, nil
}

func (k *KV) expireOne(ctx context.Context, key string) (Orphan, bool, error) {
	entry, err := k.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return Orphan{}, false, nil // swept by another plane between list and get
	}
	if err != nil {
		return Orphan{}, false, fmt.Errorf("lease: reading %s: %w", key, err)
	}
	var record claimRecord
	if err := json.Unmarshal(entry.Value(), &record); err != nil {
		return Orphan{}, false, fmt.Errorf("lease: decoding %s: %w", key, err)
	}
	deadline, accepted, err := k.deadline(ctx, key, entry.Revision(), record)
	if err != nil {
		return Orphan{}, false, err
	}
	if !accepted {
		// Nobody has taken the step off the queue yet, so there is no holder
		// whose silence could mean anything. The queue holds the dispatch
		// durably and redelivers it if an engine fetches it and dies before
		// accepting; what can strand it is the fleet losing every engine able
		// to take it, and that is the scheduler's question (Unaccepted).
		return Orphan{}, false, nil
	}
	if !k.now().After(time.Unix(0, deadline)) {
		return Orphan{}, false, nil
	}

	// Delete at the revision we read: exactly one sweeper wins, and the loser's
	// error means the orphan is already somebody else's to re-dispatch.
	if err := k.kv.Delete(ctx, key, jetstream.LastRevision(entry.Revision())); err != nil {
		return Orphan{}, false, nil //nolint:nilerr // losing the race is a normal outcome, not a failure
	}
	_ = k.kv.Delete(ctx, renewKeyFor(key))

	return Orphan{
		TenantID: record.TenantID,
		RunID:    record.RunID,
		StepID:   record.StepID,
		Attempt:  record.Attempt,
		Fence:    entry.Revision(),
	}, true, nil
}

// deadline is the claim's own expiry, extended by the latest renewal that
// belongs to THIS fence. A renewal left behind by a superseded holder names an
// older fence and is ignored.
//
// It also reports whether the lease has a holder at all. A claim always does.
// An offer does only once a renewal for its fence exists, and from then on
// that renewal alone is its deadline: an engine that accepted a step after it
// waited, and died straight after, must be lost one heartbeat window after its
// last renewal — not held to anything written when the step was queued.
func (k *KV) deadline(ctx context.Context, key string, fence uint64, record claimRecord) (int64, bool, error) {
	entry, err := k.kv.Get(ctx, renewKeyFor(key))
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return record.ExpiresAt, !record.Offered, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("lease: reading renewal for %s: %w", key, err)
	}
	var renewal renewRecord
	if err := json.Unmarshal(entry.Value(), &renewal); err != nil {
		return 0, false, fmt.Errorf("lease: decoding renewal for %s: %w", key, err)
	}
	if renewal.Fence != fence {
		return record.ExpiresAt, !record.Offered, nil
	}
	if record.Offered || renewal.ExpiresAt >= record.ExpiresAt {
		return renewal.ExpiresAt, true, nil
	}
	return record.ExpiresAt, true, nil
}

// Unaccepted lists every current offer that no renewal has accepted. It reads
// and never writes, so it is as safe on several planes as a Validate is.
func (k *KV) Unaccepted(ctx context.Context) ([]Waiting, error) {
	keys, err := k.kv.ListKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("lease: listing %s: %w", Bucket, err)
	}
	defer func() { _ = keys.Stop() }()

	var waiting []Waiting
	for key := range keys.Keys() {
		if err := ctx.Err(); err != nil {
			return waiting, err
		}
		if !strings.HasPrefix(key, claimPrefix) {
			continue
		}
		entry, err := k.kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue // swept between list and get
		}
		if err != nil {
			return nil, fmt.Errorf("lease: reading %s: %w", key, err)
		}
		var record claimRecord
		if err := json.Unmarshal(entry.Value(), &record); err != nil {
			return nil, fmt.Errorf("lease: decoding %s: %w", key, err)
		}
		if !record.Offered {
			continue
		}
		_, accepted, err := k.deadline(ctx, key, entry.Revision(), record)
		if err != nil {
			return nil, err
		}
		if accepted {
			continue
		}
		waiting = append(waiting, Waiting{
			TenantID: record.TenantID,
			RunID:    record.RunID,
			StepID:   record.StepID,
			Attempt:  record.Attempt,
			Fence:    entry.Revision(),
		})
	}
	return waiting, nil
}

// claimKey is where tenant scoping is enforced. Every segment is encoded so no
// identifier containing a dot can pose as another step's key, and the tenant is
// always the first segment: two tenants naming the same run and step hold two
// independent leases.
func claimKey(tenantID, runID, stepID string) (string, error) {
	if tenantID == "" {
		return "", fmt.Errorf("lease: %w", ErrTenantRequired)
	}
	if runID == "" || stepID == "" {
		return "", errors.New("lease: run and step are required")
	}
	return claimPrefix + encode(tenantID) + "." + encode(runID) + "." + encode(stepID), nil
}

func renewKeyFor(claim string) string {
	return renewPrefix + strings.TrimPrefix(claim, claimPrefix)
}

// encode keeps identifiers inside the character set NATS KV keys allow, without
// losing the boundary between segments.
func encode(segment string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(segment))
}
