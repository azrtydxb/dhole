package cache

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// timeFormat matches the run store: SQLite has no time type, and RFC3339 with
// nanoseconds in UTC sorts lexicographically in chronological order.
const timeFormat = time.RFC3339Nano

// Cache is the persisted side of the primitive: which keys have been seen and
// what they produced. It holds references, never bytes — the bytes are in the
// CAS, addressed by the digests recorded here.
//
// It shares the run store's database rather than owning one, because cache
// entries and the runs that pin their blobs are collected together (ADR 0009's
// refcount reclamation) and a reclamation that spans two databases cannot be
// made atomic. That database is SQLite for a developer and a homelab and
// Postgres for the tuned target, so the dialect travels with the handle.
type Cache struct {
	db      *sql.DB
	dialect runstore.Dialect
	// ownsDB is true only when this cache opened the handle itself, which is
	// what makes Close safe: a cache handed a shared handle must not close the
	// definition store's and the collector's database out from under them.
	ownsDB bool
}

// New returns a cache over an already-open handle speaking dialect. The handle
// is the caller's: the cache shares the run store's database rather than
// owning one, because cache entries and the runs that pin their blobs are
// collected together (ADR 0009's refcount reclamation) and a reclamation that
// spans two databases cannot be made atomic.
func New(db *sql.DB, dialect runstore.Dialect) *Cache {
	return &Cache{db: db, dialect: dialect}
}

// NewSQLite is the single-file convenience: it opens the SQLite database at
// path, applies the embedded migrations, and hands back a cache that owns and
// closes that handle. It is a shorthand for New over runstore.OpenSQLite, not
// the only way in — a Postgres deployment passes its own handle to New.
func NewSQLite(path string) (*Cache, error) {
	db, err := runstore.OpenSQLite(path)
	if err != nil {
		return nil, fmt.Errorf("cache: open sqlite: %w", err)
	}
	c := New(db, runstore.DialectSQLite)
	c.ownsDB = true
	return c, nil
}

// Lookup returns the outputs recorded for key k under the tenant, and whether
// there was an entry at all. A miss is the ordinary answer, not an error: it
// simply means the step has to run.
func (c *Cache) Lookup(
	ctx context.Context, tenantID string, k *dholev1.Digest,
) ([]*dholev1.OutputRef, bool, error) {
	if tenantID == "" {
		return nil, false, runstore.ErrTenantRequired
	}
	keyText, err := keyText(k)
	if err != nil {
		return nil, false, err
	}

	const q = `SELECT outputs FROM cache_entries WHERE tenant_id = ? AND key = ?`
	var encoded []byte
	switch err := c.db.QueryRowContext(ctx, c.dialect.Rebind(q), tenantID, keyText).Scan(&encoded); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("cache: lookup entry: %w", err)
	}

	outs, err := decodeOutputs(encoded)
	if err != nil {
		return nil, false, err
	}
	return outs, true, nil
}

// Record stores the outputs a pure step produced for key k.
//
// It is idempotent on (tenant, key): at-least-once delivery makes a
// re-reported success routine, and the same key is by construction the same
// work, so the stored row is simply refreshed rather than duplicated or
// refused.
func (c *Cache) Record(
	ctx context.Context, tenantID string, k *dholev1.Digest, outs []*dholev1.OutputRef,
) error {
	if tenantID == "" {
		return runstore.ErrTenantRequired
	}
	keyText, err := keyText(k)
	if err != nil {
		return err
	}
	encoded, err := encodeOutputs(outs)
	if err != nil {
		return err
	}

	const q = `INSERT INTO cache_entries (tenant_id, key, outputs, created_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (tenant_id, key) DO UPDATE SET
			outputs = excluded.outputs,
			created_at = excluded.created_at`
	if _, err := c.db.ExecContext(ctx, c.dialect.Rebind(q),
		tenantID, keyText, encoded, time.Now().UTC().Format(timeFormat)); err != nil {
		return fmt.Errorf("cache: record entry: %w", err)
	}
	return nil
}

// Close releases the cache's database handle, and only if the cache opened
// it. A handle passed to New belongs to whoever opened it.
func (c *Cache) Close() error {
	if !c.ownsDB {
		return nil
	}
	if err := c.db.Close(); err != nil {
		return fmt.Errorf("cache: close: %w", err)
	}
	return nil
}

// keyText renders a key digest as its stored form. A malformed key is refused
// rather than stored: an entry nobody can compute the key for again is dead
// weight, and one stored under a truncated key could be hit by a different
// step.
func keyText(k *dholev1.Digest) (string, error) {
	if k.GetAlgo() == "" || k.GetHex() == "" {
		return "", fmt.Errorf("cache: malformed key %q:%q", k.GetAlgo(), k.GetHex())
	}
	return k.GetAlgo() + ":" + k.GetHex(), nil
}

// encodeOutputs frames each OutputRef with its length so the list can be read
// back apart. The same reason the key is framed applies to what is stored
// under it: concatenated messages have no unambiguous boundary.
func encodeOutputs(outs []*dholev1.OutputRef) ([]byte, error) {
	encoded := binary.BigEndian.AppendUint64(nil, uint64(len(outs)))
	for _, out := range outs {
		message, err := proto.Marshal(out)
		if err != nil {
			return nil, fmt.Errorf("cache: encode output %q: %w", out.GetPort(), err)
		}
		encoded = binary.BigEndian.AppendUint64(encoded, uint64(len(message)))
		encoded = append(encoded, message...)
	}
	return encoded, nil
}

// decodeOutputs reverses encodeOutputs, refusing anything it cannot read whole
// rather than returning a partial output list that would look like a complete
// step result.
func decodeOutputs(encoded []byte) ([]*dholev1.OutputRef, error) {
	const frame = 8
	if len(encoded) < frame {
		return nil, errors.New("cache: truncated cache entry")
	}
	count := binary.BigEndian.Uint64(encoded[:frame])
	rest := encoded[frame:]

	outs := make([]*dholev1.OutputRef, 0, min(count, 1024))
	for range count {
		if len(rest) < frame {
			return nil, errors.New("cache: truncated cache entry")
		}
		length := binary.BigEndian.Uint64(rest[:frame])
		rest = rest[frame:]
		if uint64(len(rest)) < length {
			return nil, errors.New("cache: truncated cache entry")
		}
		out := &dholev1.OutputRef{}
		if err := proto.Unmarshal(rest[:length], out); err != nil {
			return nil, fmt.Errorf("cache: decode output: %w", err)
		}
		outs = append(outs, out)
		rest = rest[length:]
	}
	if len(rest) != 0 {
		return nil, errors.New("cache: trailing bytes in cache entry")
	}
	return outs, nil
}
