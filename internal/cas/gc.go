package cas

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// Deleter is the removal half of a Store. It is deliberately not part of
// Store: every component in the system reads and writes blobs, exactly one
// deletes them, and a Delete reachable from a step executor is a Delete that
// will eventually be called by one.
//
// Delete reports an error satisfying errors.Is(err, ErrNotFound) when the blob
// is already gone, so a collector can tell "nothing to do" apart from "the
// filesystem refused".
type Deleter interface {
	Delete(ctx context.Context, tenantID string, d *dholev1.Digest) error
}

// GC reclaims blobs no run needs any more.
//
// It is refcounting, not ownership: identical bytes produced by twenty runs
// are one object, and it survives until the last run holding a reference has
// aged out of the retention window. Deleting on the first unreferencing run
// would take bytes a live run is still entitled to read.
//
// Runs supplies the ages: a run's last event is when it last did anything, and
// a run with no events at all is treated as live, because a collector must
// never guess in the direction that deletes.
//
// DB is the run store's own handle — blob_refs and cache_entries live beside
// the run event log precisely so a reclamation that spans both can be one
// transaction. Open it with runstore.OpenSQLite or runstore.OpenPostgres, or
// with OpenIndex for the SQLite case.
//
// Dialect is the SQL that handle speaks, and it must match: pgx rejects the
// `?` placeholders these statements are written with, so a Postgres handle
// left on the zero value collects nothing at all. The zero value is
// DialectSQLite, which is what the single-binary deployment runs.
type GC struct {
	Store   Store
	Runs    runstore.Store
	DB      *sql.DB
	Dialect runstore.Dialect
	// Collected is told about every blob this collector actually removed.
	//
	// It exists because storage accounting only ever grew: the collector
	// deleted blobs and nothing offset them, so a tenant who reclaimed a
	// terabyte was still charged for it and was eventually refused every write
	// against a store that was nearly empty.
	//
	// It is an interface declared HERE and implemented elsewhere because the
	// dependency runs one way: the metering package knows about storage, and
	// storage must not know about metering. Nil means nobody is counting,
	// which is the right default for a collector run without a ledger.
	Collected Collector
}

// Collector is told which blobs a sweep reclaimed, so something can credit
// them back. A digest is enough: whoever charged for these bytes recorded how
// many there were under the same digest.
type Collector interface {
	Collected(ctx context.Context, tenantID string, d *dholev1.Digest) error
}

// OpenIndex opens the reference index over the SQLite database at path,
// applying the embedded migrations. It is the single-file convenience; a
// Postgres deployment opens its handle with runstore.OpenPostgres and sets
// GC.Dialect to match.
//
// The transactions it opens are IMMEDIATE: the collector takes SQLite's write
// lock the moment it starts, which is what serialises it against a concurrent
// GC.Reference rather than letting both read a snapshot and act on it.
func OpenIndex(path string) (*sql.DB, error) {
	db, err := runstore.OpenSQLite(path)
	if err != nil {
		return nil, fmt.Errorf("cas: open reference index: %w", err)
	}
	return db, nil
}

// Reference records that run runID needs the blob with digest d. It is
// idempotent on (tenant, digest, run): a redelivered step result re-records
// the same reference rather than inflating a count that would strand the blob
// forever.
//
// The blob's existence is checked inside the same IMMEDIATE transaction that
// inserts the row, and that is not belt and braces — it is how the collector's
// last race is closed. A collection holds the write lock across both its
// deletions and its unlinks, so this insert either commits before the
// collection began (and the reference protects the blob) or blocks until after
// it finished — at which point the check sees the missing bytes and refuses,
// instead of writing a row that points at nothing. A caller refused this way
// simply re-Puts the bytes; in a content-addressed store that restores exactly
// the same object.
func (g *GC) Reference(ctx context.Context, tenantID string, d *dholev1.Digest, runID string) error {
	if tenantID == "" {
		return runstore.ErrTenantRequired
	}
	if runID == "" {
		return errors.New("cas: reference requires a run id")
	}
	text, err := digestText(d)
	if err != nil {
		return err
	}

	tx, err := g.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("cas: begin reference: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	switch has, err := g.Store.Has(ctx, tenantID, d); {
	case err != nil:
		return err
	case !has:
		return fmt.Errorf("%w: %s", ErrNotFound, text)
	}

	const q = `INSERT INTO blob_refs (tenant_id, digest, run_id)
		VALUES (?, ?, ?) ON CONFLICT DO NOTHING`
	if _, err := tx.ExecContext(ctx, g.Dialect.Rebind(q), tenantID, text, runID); err != nil {
		return fmt.Errorf("cas: record blob reference: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cas: commit reference: %w", err)
	}
	return nil
}

// Collect reclaims the tenant's blobs that no run within the retain window
// still references, and returns how many blobs it removed.
//
// The order is the whole of the safety argument, and it is only ever this way
// round:
//
//  1. Compute the retained run set FIRST, from the run event log.
//  2. Only then read the references and delete the ones whose runs expired.
//  3. Delete a blob only if no reference to it remained after step 2.
//
// Deleting first and reconciling afterwards — or classifying runs from a
// snapshot taken after references were already dropped — loses bytes a running
// step is about to read, intermittently, and the failure lands on whatever
// reads the blob next rather than here.
func (g *GC) Collect(ctx context.Context, tenantID string, retain time.Duration) (int, error) {
	if tenantID == "" {
		return 0, runstore.ErrTenantRequired
	}
	deleter, ok := g.Store.(Deleter)
	if !ok {
		return 0, errors.New("cas: store cannot delete blobs")
	}

	// Step 1, before anything is deleted: which runs are still retained.
	expired, err := g.expiredRuns(ctx, tenantID, retain)
	if err != nil {
		return 0, err
	}

	tx, err := g.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("cas: begin collection: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	// Step 2: read the references under the write lock. A run that appeared
	// between step 1 and here is unknown to the expired set and therefore
	// counts as retained — the conservative direction is the only one that
	// cannot delete live bytes.
	collectable, err := unreferencedDigests(ctx, tx, g.Dialect, tenantID, expired)
	if err != nil {
		return 0, err
	}
	const dropRefs = `DELETE FROM blob_refs WHERE tenant_id = ? AND run_id = ?`
	for _, runID := range expired {
		if _, err := tx.ExecContext(ctx, g.Dialect.Rebind(dropRefs), tenantID, runID); err != nil {
			return 0, fmt.Errorf("cas: drop expired references: %w", err)
		}
	}

	// The cache entry goes before the bytes it names, and the asymmetry is the
	// reason: an entry dropped while its blob still exists is a cold cache —
	// the step reruns and repopulates it. A blob deleted while its entry
	// survives is a cache HIT handing back a digest nobody can read, which
	// does not rerun the step, it fails whatever consumes the output. So if
	// only one of the two lands, it must be this one.
	if err := dropCacheEntries(ctx, tx, g.Dialect, tenantID, collectable); err != nil {
		return 0, err
	}

	// Step 3, still holding the write lock so no Reference can slip in behind
	// us. A partial failure here still commits: forgetting a reference to a
	// blob that survived is safe, keeping one to a blob that did not is not.
	freed := 0
	var deleteErr error
	var collected []*dholev1.Digest
	for _, text := range collectable {
		d, err := parseDigestText(text)
		if err != nil {
			deleteErr = err
			break
		}
		switch err := deleter.Delete(ctx, tenantID, d); {
		case errors.Is(err, ErrNotFound):
			// Already collected: the row outlived its bytes, which is exactly
			// the state this sweep exists to tidy up.
		case err != nil:
			deleteErr = err
		default:
			freed++
			// Remembered here and credited after the COMMIT, never inside the
			// transaction. The credit reads the ledger, which lives in this
			// same database, and a SQLite run store holds exactly one
			// connection — so a query issued while this transaction is open
			// waits for a connection the transaction will not release until
			// the query returns. That is a deadlock in Go's connection pool,
			// where no busy timeout applies: the sweep simply never finishes.
			collected = append(collected, d)
		}
		if deleteErr != nil {
			break
		}
	}

	if err := tx.Commit(); err != nil {
		return freed, fmt.Errorf("cas: commit collection: %w", err)
	}
	committed = true
	if deleteErr != nil {
		return freed, deleteErr
	}

	// The credits, now that the transaction is closed and the connection is
	// free. A failure here does NOT undo the collection — the bytes are gone,
	// and pretending otherwise would charge for them forever — but it IS
	// returned, because a credit that silently fails is the bug this hook
	// exists to fix, arriving one sweep later.
	//
	// Safe to run after the commit precisely because it is idempotent: the
	// credit is keyed by the same digest as the charge, so a blob whose index
	// row outlived its bytes is credited once however many sweeps see it.
	if g.Collected != nil {
		for _, d := range collected {
			if err := g.Collected.Collected(ctx, tenantID, d); err != nil {
				return freed, fmt.Errorf("cas: crediting collected blob: %w", err)
			}
		}
	}
	return freed, nil
}

// expiredRuns returns the runs holding references for this tenant whose last
// recorded event is older than the retain window. A run with no events is
// omitted — unknown age means retained, never collectable.
func (g *GC) expiredRuns(ctx context.Context, tenantID string, retain time.Duration) ([]string, error) {
	rows, err := g.DB.QueryContext(ctx,
		g.Dialect.Rebind(`SELECT DISTINCT run_id FROM blob_refs WHERE tenant_id = ?`), tenantID)
	if err != nil {
		return nil, fmt.Errorf("cas: list referencing runs: %w", err)
	}
	var runIDs []string
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("cas: list referencing runs: %w", err)
		}
		runIDs = append(runIDs, runID)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, fmt.Errorf("cas: list referencing runs: %w", err)
	}

	cutoff := time.Now().UTC().Add(-retain)
	var expired []string
	for _, runID := range runIDs {
		events, err := g.Runs.Replay(ctx, tenantID, runID)
		if err != nil {
			return nil, fmt.Errorf("cas: age run %s: %w", runID, err)
		}
		if len(events) == 0 {
			continue
		}
		last := events[0].At
		for _, e := range events[1:] {
			if e.At.After(last) {
				last = e.At
			}
		}
		if last.Before(cutoff) {
			expired = append(expired, runID)
		}
	}
	return expired, nil
}

// unreferencedDigests returns the tenant's digests whose every remaining
// reference belongs to an expired run — the only blobs that may be deleted.
func unreferencedDigests(
	ctx context.Context, tx *sql.Tx, dialect runstore.Dialect, tenantID string, expired []string,
) ([]string, error) {
	if len(expired) == 0 {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx,
		dialect.Rebind(`SELECT digest, run_id FROM blob_refs WHERE tenant_id = ?`), tenantID)
	if err != nil {
		return nil, fmt.Errorf("cas: read blob references: %w", err)
	}
	isExpired := make(map[string]bool, len(expired))
	for _, runID := range expired {
		isExpired[runID] = true
	}

	retainedBy := map[string]bool{}
	var order []string
	for rows.Next() {
		var digest, runID string
		if err := rows.Scan(&digest, &runID); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("cas: read blob references: %w", err)
		}
		if _, seen := retainedBy[digest]; !seen {
			order = append(order, digest)
			retainedBy[digest] = false
		}
		if !isExpired[runID] {
			retainedBy[digest] = true
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, fmt.Errorf("cas: read blob references: %w", err)
	}

	var collectable []string
	for _, digest := range order {
		if !retainedBy[digest] {
			collectable = append(collectable, digest)
		}
	}
	return collectable, nil
}

// dropCacheEntries removes every cache entry naming one of the digests about
// to be deleted.
//
// It reads and deletes the rows directly because the cache package exports no
// way to enumerate or drop entries — only Lookup by key and Record. The two
// tables share one database so that this stays a single transaction; splitting
// it across an API boundary would either reintroduce two-file atomicity or
// leave the cache pointing at bytes this transaction removed.
// debt: this reads and writes internal/cache's table directly, and decodes its
// length-framed output encoding locally, because that package exports no way to
// enumerate or drop entries. Ceiling: it holds only while the two tables share
// one database and one transaction — the moment the cache moves to its own
// store, or its encoding changes without this decoder changing with it, the
// sweep silently stops dropping stale entries. Revisit when internal/cache
// grows a DropEntries/iteration API, and delete this function's SQL then.
func dropCacheEntries(
	ctx context.Context, tx *sql.Tx, dialect runstore.Dialect, tenantID string, collectable []string,
) error {
	if len(collectable) == 0 {
		return nil
	}
	doomed := make(map[string]bool, len(collectable))
	for _, digest := range collectable {
		doomed[digest] = true
	}

	rows, err := tx.QueryContext(ctx,
		dialect.Rebind(`SELECT key, outputs FROM cache_entries WHERE tenant_id = ?`), tenantID)
	if err != nil {
		return fmt.Errorf("cas: read cache entries: %w", err)
	}
	var stale []string
	for rows.Next() {
		var (
			key     string
			encoded []byte
		)
		if err := rows.Scan(&key, &encoded); err != nil {
			_ = rows.Close()
			return fmt.Errorf("cas: read cache entries: %w", err)
		}
		for _, digest := range entryDigests(encoded) {
			if doomed[digest] {
				stale = append(stale, key)
				break
			}
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("cas: read cache entries: %w", err)
	}

	for _, key := range stale {
		if _, err := tx.ExecContext(ctx,
			dialect.Rebind(`DELETE FROM cache_entries WHERE tenant_id = ? AND key = ?`),
			tenantID, key); err != nil {
			return fmt.Errorf("cas: drop cache entry: %w", err)
		}
	}
	return nil
}

// entryDigests reads the digests out of a stored cache entry: a count followed
// by length-framed OutputRef messages, the encoding the cache writes.
//
// A row it cannot parse yields no digests, so the entry is left alone. That is
// the safe direction here — an unreadable entry is the cache's problem to
// report on lookup, and guessing would drop rows this collection never
// examined.
func entryDigests(encoded []byte) []string {
	const frame = 8
	if len(encoded) < frame {
		return nil
	}
	count := binary.BigEndian.Uint64(encoded[:frame])
	rest := encoded[frame:]

	var digests []string
	for range count {
		if len(rest) < frame {
			return digests
		}
		length := binary.BigEndian.Uint64(rest[:frame])
		rest = rest[frame:]
		if uint64(len(rest)) < length {
			return digests
		}
		out := &dholev1.OutputRef{}
		if err := proto.Unmarshal(rest[:length], out); err != nil {
			return digests
		}
		rest = rest[length:]
		if text, err := digestText(out.GetDigest()); err == nil {
			digests = append(digests, text)
		}
	}
	return digests
}

// digestText renders a digest in the "<algo>:<hex>" form the reference index
// and the cache both store.
func digestText(d *dholev1.Digest) (string, error) {
	if d.GetAlgo() == "" || d.GetHex() == "" {
		return "", fmt.Errorf("cas: malformed digest %q:%q", d.GetAlgo(), d.GetHex())
	}
	return d.GetAlgo() + ":" + d.GetHex(), nil
}

func parseDigestText(text string) (*dholev1.Digest, error) {
	algo, hexDigest, ok := strings.Cut(text, ":")
	if !ok || algo == "" || hexDigest == "" {
		return nil, fmt.Errorf("cas: malformed stored digest %q", text)
	}
	return &dholev1.Digest{Algo: algo, Hex: hexDigest}, nil
}
