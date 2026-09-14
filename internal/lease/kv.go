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
//
// Unlike Claim it is a compare-and-set. It writes only over no record at all,
// or over one for an EARLIER attempt, and at exactly the revision it read — so
// of two offers of one attempt, from one plane or several, the server lets one
// through and the other is refused with ErrAlreadyOffered, whichever order
// their reads and writes interleave in. A plain Put let the second replace the
// fence the first had already dispatched under, and every step on kw lost an
// attempt to it.
func (k *KV) Offer(ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration) (Token, error) {
	key, data, err := k.record(tenantID, runID, stepID, attempt, ttl, true)
	if err != nil {
		return Token{}, err
	}
	// Each lost compare-and-set means somebody else wrote this key between our
	// read and our write, and the next read decides against what they wrote. A
	// handful is generous: a step's key is written once per attempt.
	for range maxOfferRaces {
		entry, err := k.kv.Get(ctx, key)
		var fence uint64
		switch {
		case errors.Is(err, jetstream.ErrKeyNotFound):
			// Never leased, or swept or withdrawn: a delete marker counts as
			// absent to Create, which writes at the marker's revision.
			fence, err = k.kv.Create(ctx, key, data)
		case err != nil:
			return Token{}, fmt.Errorf("lease: reading %s: %w", key, err)
		default:
			var current claimRecord
			if err := json.Unmarshal(entry.Value(), &current); err != nil {
				return Token{}, fmt.Errorf("lease: decoding %s: %w", key, err)
			}
			if current.Attempt >= attempt {
				return Token{}, fmt.Errorf("%w: %s/%s/%s is at attempt %d (fence %d), offering %d",
					ErrAlreadyOffered, tenantID, runID, stepID, current.Attempt, entry.Revision(), attempt)
			}
			fence, err = k.kv.Update(ctx, key, data, entry.Revision())
		}
		if err == nil {
			return Token{Value: key, Fence: fence}, nil
		}
		if !lostRace(err) {
			return Token{}, fmt.Errorf("lease: offer %s/%s/%s: %w", tenantID, runID, stepID, err)
		}
	}
	return Token{}, fmt.Errorf("lease: offer %s/%s/%s: the key kept changing under %d attempts to write it",
		tenantID, runID, stepID, maxOfferRaces)
}

// maxOfferRaces bounds how many compare-and-set conflicts one Offer re-reads
// through before giving up with an error.
const maxOfferRaces = 8

// lostRace reports whether err is a compare-and-set refused because the key
// moved since it was read.
func lostRace(err error) bool {
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return true
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && (apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant)
}

func (k *KV) claim(
	ctx context.Context, tenantID, runID, stepID string, attempt uint32, ttl time.Duration, offered bool,
) (Token, error) {
	key, data, err := k.record(tenantID, runID, stepID, attempt, ttl, offered)
	if err != nil {
		return Token{}, err
	}
	fence, err := k.kv.Put(ctx, key, data)
	if err != nil {
		return Token{}, fmt.Errorf("lease: claim %s/%s/%s: %w", tenantID, runID, stepID, err)
	}
	return Token{Value: key, Fence: fence}, nil
}

// record builds the key and the encoded claim record a Claim or an Offer
// writes.
func (k *KV) record(
	tenantID, runID, stepID string, attempt uint32, ttl time.Duration, offered bool,
) (string, []byte, error) {
	key, err := claimKey(tenantID, runID, stepID)
	if err != nil {
		return "", nil, err
	}
	if ttl <= 0 {
		return "", nil, fmt.Errorf("lease: claim %s/%s/%s: ttl must be positive", tenantID, runID, stepID)
	}
	data, err := json.Marshal(claimRecord{
		TenantID:  tenantID,
		RunID:     runID,
		StepID:    stepID,
		Attempt:   attempt,
		TTLNanos:  ttl.Nanoseconds(),
		ExpiresAt: k.now().Add(ttl).UnixNano(),
		Offered:   offered,
	})
	if err != nil {
		return "", nil, fmt.Errorf("lease: encoding claim: %w", err)
	}
	return key, data, nil
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

// Withdraw deletes the unaccepted offer behind t at exactly its revision, so
// the attempt it was for can be offered again.
//
// It is for an offer whose dispatch never committed — its plane died, or gave
// up, between offering and committing — which nobody would otherwise ever
// accept or replace. Deciding that is the caller's business (it takes the run
// log and a grace window); what this guarantees is that nothing else goes:
// a key that has moved to another fence, a key already gone, a claim, and an
// offer that has been accepted are all refused with ErrFenced. The delete is
// conditional on the revision, so an offer written between the read and the
// delete survives it.
func (k *KV) Withdraw(ctx context.Context, t Token) error {
	record, err := k.current(ctx, t)
	if err != nil {
		return err
	}
	// A claim has a holder from its first instant, so deadline reports it
	// accepted and it is refused here along with an offer an engine has taken.
	_, accepted, err := k.deadline(ctx, t.Value, t.Fence, record)
	if err != nil {
		return err
	}
	if accepted {
		return fmt.Errorf("%w: %s at fence %d has a holder", ErrFenced, t.Value, t.Fence)
	}
	if err := k.kv.Delete(ctx, t.Value, jetstream.LastRevision(t.Fence)); err != nil {
		if lostRace(err) {
			return fmt.Errorf("%w: %s moved past fence %d before it was withdrawn", ErrFenced, t.Value, t.Fence)
		}
		return fmt.Errorf("lease: withdrawing %s: %w", t.Value, err)
	}
	return nil
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
			// ExpiresAt is written as offer time plus TTL, in one clock
			// reading, so this is the offer time exactly.
			OfferedAt: time.Unix(0, record.ExpiresAt-record.TTLNanos),
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
