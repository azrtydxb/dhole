package secrets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// HandleBucket is the JetStream KV bucket every plane records handles in
// (ADR 0031).
const HandleBucket = "dhole-secret-handles"

// HandleRetention is the longest the bucket keeps a record. A handle asked for
// a longer expiry is refused at issue: the bucket would drop its record first,
// and the handle would be refused before the expiry it carries.
const HandleRetention = 30 * time.Minute

// Kind names which of the plane's sources a handle resolves from. They are
// kept apart for ADR 0027's reason: a model credential the operator gave the
// plane is not thereby a secret a pipeline step may read.
type Kind string

const (
	// SourceStep is the operator's step secrets (ADR 0027).
	SourceStep Kind = "step"
	// SourcePlane is the plane's own model credentials (ADR 0024).
	SourcePlane Kind = "plane"
)

// Reference is what a handle stands for: a secret by NAME in one source, and
// the name the redeemer binds its value to. It is never the value.
type Reference struct {
	Source Kind
	// Secret is the secret's name in the source.
	Secret string
	// Binding is the SecretRef's name: the environment variable a step's
	// engine binds the value to.
	Binding string
}

// Record is what the bucket stores for one live handle. Every field is
// something needed to decide a redemption; none is the value and none is the
// handle.
type Record struct {
	TenantID string `json:"tenant_id"`
	RunID    string `json:"run_id,omitempty"`
	StepID   string `json:"step_id,omitempty"`
	Attempt  uint32 `json:"attempt,omitempty"`
	Source   Kind   `json:"source"`
	// Name is the secret's name in the source: a name, never its value.
	Name      string `json:"name"`
	ExpiresAt int64  `json:"expires_at_unix_nano"`
}

// Filter selects records to drop. Every non-empty field must match; a filter
// naming neither a tenant nor a digest matches nothing.
type Filter struct {
	TenantID string
	RunID    string
	StepID   string
	Attempt  uint32
	// Digest is one handle's digest.
	Digest string
}

func (f Filter) matches(digest string, r Record) bool {
	switch {
	case f.Digest != "" && f.Digest != digest:
		return false
	case f.TenantID != "" && f.TenantID != r.TenantID:
		return false
	case f.RunID != "" && f.RunID != r.RunID:
		return false
	case f.StepID != "" && f.StepID != r.StepID:
		return false
	case f.Attempt != 0 && f.Attempt != r.Attempt:
		return false
	}
	return f.Digest != "" || f.TenantID != ""
}

// Handles is where a broker keeps its records. A deployment of more than one
// plane must give every plane the same one (ADR 0031).
type Handles interface {
	// Put records a handle by its digest.
	Put(ctx context.Context, digest string, r Record) error
	// Spend takes the record of a digest, atomically: of any number of
	// concurrent spenders — on any number of planes — at most one gets it.
	Spend(ctx context.Context, digest string) (Record, bool, error)
	// Drop removes every record the filter matches and reports how many.
	Drop(ctx context.Context, f Filter) (int, error)
	// List returns every live record, keyed by digest.
	List(ctx context.Context) (map[string]Record, error)
}

// Digest is the name a handle is recorded under. The bucket holds this and
// never the handle, so reading the bucket is not a way to redeem.
func Digest(handle string) string {
	sum := sha256.Sum256([]byte(handle))
	return hex.EncodeToString(sum[:])
}

// MemoryHandles keeps records in one process. It is correct for exactly one
// plane, and it is what a broker built with no bucket gets.
type MemoryHandles struct {
	mu      sync.Mutex
	records map[string]Record
}

var _ Handles = (*MemoryHandles)(nil)

// NewMemoryHandles returns an empty in-process store.
func NewMemoryHandles() *MemoryHandles {
	return &MemoryHandles{records: map[string]Record{}}
}

// Put implements Handles. It also forgets whatever has expired, so a handle
// nobody redeemed does not stay for the life of the process.
func (m *MemoryHandles) Put(_ context.Context, digest string, r Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UnixNano()
	for d, rec := range m.records {
		if now > rec.ExpiresAt {
			delete(m.records, d)
		}
	}
	m.records[digest] = r
	return nil
}

// Spend implements Handles.
func (m *MemoryHandles) Spend(_ context.Context, digest string) (Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.records[digest]
	delete(m.records, digest)
	return r, ok, nil
}

// Drop implements Handles.
func (m *MemoryHandles) Drop(_ context.Context, f Filter) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for d, r := range m.records {
		if f.matches(d, r) {
			delete(m.records, d)
			n++
		}
	}
	return n, nil
}

// List implements Handles.
func (m *MemoryHandles) List(context.Context) (map[string]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Record, len(m.records))
	for d, r := range m.records {
		out[d] = r
	}
	return out, nil
}

// KVHandles keeps records in the shared bucket, so every plane on the bus
// redeems, spends and revokes the same handles (ADR 0031).
//
// A key is `<tenant>.<run digest>.<handle digest>`. The tenant is in the key
// so that a run's handles are one filtered listing; the run is a digest
// because a run id is not guaranteed to be a key token; and the handle is a
// digest so the bucket holds no bearer credential.
type KVHandles struct {
	kv jetstream.KeyValue
}

var _ Handles = (*KVHandles)(nil)

// NewKVHandles binds the handle bucket on conn, creating it if it is not there.
func NewKVHandles(ctx context.Context, conn *nats.Conn) (*KVHandles, error) {
	js, err := jetstream.New(conn)
	if err != nil {
		return nil, fmt.Errorf("secrets: jetstream: %w", err)
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: HandleBucket,
		Description: "Live secret handles, as references: tenant, run, step, attempt, " +
			"source and secret name. Never a value, never a handle (ADR 0031).",
		History: 1,
		TTL:     HandleRetention,
		Storage: jetstream.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("secrets: bucket %q: %w", HandleBucket, err)
	}
	return &KVHandles{kv: kv}, nil
}

// planeRunToken stands for "no run" in a key: a handle the plane issued for
// itself. It cannot collide with a run digest, which is hex.
const planeRunToken = "plane"

func runToken(runID string) string {
	if runID == "" {
		return planeRunToken
	}
	sum := sha256.Sum256([]byte(runID))
	return hex.EncodeToString(sum[:16])
}

// tenantToken refuses a tenant that cannot be one key token. internal/tenant
// is narrower still; this is the part of its rule a key depends on.
func tenantToken(tenantID string) error {
	if tenantID == "" {
		return errors.New("secrets: a tenant is required")
	}
	for _, r := range tenantID {
		switch {
		case r == '-', r == '_', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		default:
			return fmt.Errorf("secrets: tenant %q cannot name a handle record: "+
				"only letters, digits, '-' and '_'", tenantID)
		}
	}
	return nil
}

func validTenantToken(tenantID string) bool { return tenantToken(tenantID) == nil }

// Put implements Handles.
func (k *KVHandles) Put(ctx context.Context, digest string, r Record) error {
	if err := tenantToken(r.TenantID); err != nil {
		return err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("secrets: encoding a handle record: %w", err)
	}
	if _, err := k.kv.Create(ctx, r.TenantID+"."+runToken(r.RunID)+"."+digest, data); err != nil {
		return fmt.Errorf("secrets: recording a handle: %w", err)
	}
	return nil
}

// Spend implements Handles by deleting the key at the revision it was read at.
// The server refuses the delete of a key that moved since, so of two planes
// spending one handle concurrently exactly one succeeds.
func (k *KVHandles) Spend(ctx context.Context, digest string) (Record, bool, error) {
	keys, err := k.keys(ctx, "*.*."+digest)
	if err != nil || len(keys) == 0 {
		return Record{}, false, err
	}
	entry, err := k.kv.Get(ctx, keys[0])
	switch {
	case errors.Is(err, jetstream.ErrKeyNotFound):
		return Record{}, false, nil
	case err != nil:
		return Record{}, false, fmt.Errorf("secrets: reading a handle record: %w", err)
	}
	var r Record
	if err := json.Unmarshal(entry.Value(), &r); err != nil {
		return Record{}, false, fmt.Errorf("secrets: decoding a handle record: %w", err)
	}
	if err := k.kv.Delete(ctx, keys[0], jetstream.LastRevision(entry.Revision())); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) || lostRace(err) {
			return Record{}, false, nil
		}
		return Record{}, false, fmt.Errorf("secrets: spending a handle: %w", err)
	}
	return r, true, nil
}

// Drop implements Handles.
func (k *KVHandles) Drop(ctx context.Context, f Filter) (int, error) {
	pattern := ""
	switch {
	case f.TenantID != "" && f.RunID != "":
		pattern = f.TenantID + "." + runToken(f.RunID) + ".*"
	case f.TenantID != "":
		pattern = f.TenantID + ".*.*"
	case f.Digest != "":
		pattern = "*.*." + f.Digest
	default:
		return 0, nil
	}
	if f.TenantID != "" && !validTenantToken(f.TenantID) {
		// A tenant no record can carry has no records to drop, and spelled
		// into a filter it could be a wildcard.
		return 0, nil
	}
	keys, err := k.keys(ctx, pattern)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, key := range keys {
		digest := key[strings.LastIndexByte(key, '.')+1:]
		entry, err := k.kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return n, fmt.Errorf("secrets: reading a handle record: %w", err)
		}
		var r Record
		if err := json.Unmarshal(entry.Value(), &r); err != nil {
			return n, fmt.Errorf("secrets: decoding a handle record: %w", err)
		}
		if !f.matches(digest, r) {
			continue
		}
		err = k.kv.Delete(ctx, key, jetstream.LastRevision(entry.Revision()))
		switch {
		case err == nil:
			n++
		case errors.Is(err, jetstream.ErrKeyNotFound) || lostRace(err):
			// Spent or revoked by somebody else in the meantime: gone either way.
		default:
			return n, fmt.Errorf("secrets: revoking a handle: %w", err)
		}
	}
	return n, nil
}

// List implements Handles.
func (k *KVHandles) List(ctx context.Context) (map[string]Record, error) {
	keys, err := k.keys(ctx, "*.*.*")
	if err != nil {
		return nil, err
	}
	out := make(map[string]Record, len(keys))
	for _, key := range keys {
		entry, err := k.kv.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("secrets: reading a handle record: %w", err)
		}
		var r Record
		if err := json.Unmarshal(entry.Value(), &r); err != nil {
			return nil, fmt.Errorf("secrets: decoding a handle record: %w", err)
		}
		out[key[strings.LastIndexByte(key, '.')+1:]] = r
	}
	return out, nil
}

// keys lists the live keys matching one subject-style filter.
func (k *KVHandles) keys(ctx context.Context, filter string) ([]string, error) {
	lister, err := k.kv.ListKeysFiltered(ctx, filter)
	if errors.Is(err, jetstream.ErrNoKeysFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("secrets: listing handle records: %w", err)
	}
	var keys []string
	for key := range lister.Keys() {
		keys = append(keys, key)
	}
	// The lister closes its channel on a cancelled context too, and a partial
	// listing read as a complete one would refuse a live handle.
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("secrets: listing handle records: %w", err)
	}
	return keys, nil
}

// lostRace reports whether err is a delete refused because the key moved
// since it was read.
func lostRace(err error) bool {
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return true
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && (apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence ||
		apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant)
}
