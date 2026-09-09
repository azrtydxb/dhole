package cas_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// newGC wires a collector over a fresh blob tree and a fresh run store sharing
// one SQLite file, which is where blob_refs and cache_entries both live.
func newGC(t *testing.T) (*cas.GC, cas.Store, runstore.Store, string) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "dhole.db")

	runs, err := runstore.NewSQLite(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = runs.Close() })

	db, err := cas.OpenIndex(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store := cas.NewFilesystem(filepath.Join(dir, "blobs"))
	return &cas.GC{Store: store, Runs: runs, DB: db}, store, runs, dbPath
}

// finish records a run whose last event sits `age` in the past, which is what
// makes a run expired or retained relative to Collect's retain window.
func finish(t *testing.T, runs runstore.Store, tenantID, runID string, age time.Duration) {
	t.Helper()
	require.NoError(t, runs.Append(context.Background(), tenantID, runstore.Event{
		RunID:    runID,
		StepID:   "step",
		Attempt:  1,
		Sequence: 1,
		Type:     runstore.RunCompleted,
		At:       time.Now().UTC().Add(-age),
	}))
}

// put stores bytes and references them from a run, the pairing every producer
// makes: the blob is only reachable because some run points at it.
func put(t *testing.T, gc *cas.GC, store cas.Store, tenantID, runID, content string) *dholev1.Digest {
	t.Helper()
	ctx := context.Background()
	d, err := store.Put(ctx, tenantID, strings.NewReader(content))
	require.NoError(t, err)
	require.NoError(t, gc.Reference(ctx, tenantID, d, runID))
	return d
}

func has(t *testing.T, store cas.Store, tenantID string, d *dholev1.Digest) bool {
	t.Helper()
	ok, err := store.Has(context.Background(), tenantID, d)
	require.NoError(t, err)
	return ok
}

// TestRefcountGCPreservesRetainedRunBlobs is the whole point of the collector:
// it may reclaim what has aged out and must not touch what is still retained.
// Getting this backwards deletes bytes a live run is about to read.
func TestRefcountGCPreservesRetainedRunBlobs(t *testing.T) {
	ctx := context.Background()
	gc, store, runs, _ := newGC(t)

	expired := put(t, gc, store, "acme", "run-old", "aged output")
	retained := put(t, gc, store, "acme", "run-new", "fresh output")
	finish(t, runs, "acme", "run-old", 48*time.Hour)
	finish(t, runs, "acme", "run-new", time.Minute)

	freed, err := gc.Collect(ctx, "acme", 24*time.Hour)
	require.NoError(t, err)
	require.Equal(t, 1, freed)

	require.False(t, has(t, store, "acme", expired), "expired run's blob should be collected")
	require.True(t, has(t, store, "acme", retained), "retained run's blob must survive")
}

// TestGCNeverCollectsBlobSharedWithRetainedRun pins the refcount property.
// Deleting on the first unreferencing run is the classic garbage-collector
// bug: the blob is still reachable from a run inside the retain window, and
// its bytes are still the thing that run's cache entry promises.
func TestGCNeverCollectsBlobSharedWithRetainedRun(t *testing.T) {
	ctx := context.Background()
	gc, store, runs, _ := newGC(t)

	shared := put(t, gc, store, "acme", "run-old", "shared output")
	require.NoError(t, gc.Reference(ctx, "acme", shared, "run-new"))
	lonely := put(t, gc, store, "acme", "run-old", "lonely output")
	finish(t, runs, "acme", "run-old", 48*time.Hour)
	finish(t, runs, "acme", "run-new", time.Minute)

	freed, err := gc.Collect(ctx, "acme", 24*time.Hour)
	require.NoError(t, err)
	require.Equal(t, 1, freed, "only the blob with no retained reference is freed")
	require.True(t, has(t, store, "acme", shared), "a blob a retained run still references must survive")
	require.False(t, has(t, store, "acme", lonely))

	// The expired run's reference is gone even though the blob stayed, so once
	// run-new falls outside the window too — here by narrowing it rather than
	// waiting — the last reference goes and the blob is reclaimed.
	freed, err = gc.Collect(ctx, "acme", 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, 1, freed)
	require.False(t, has(t, store, "acme", shared))
}

// TestCacheEntryIsDroppedWhenItsOutputBlobIsCollected: a cache that hits and
// hands back a digest whose bytes no longer exist is worse than a cold cache —
// the step does not rerun, it fails downstream. The entry must go with the
// blob.
func TestCacheEntryIsDroppedWhenItsOutputBlobIsCollected(t *testing.T) {
	ctx := context.Background()
	gc, store, runs, dbPath := newGC(t)

	entries, err := cache.NewSQLite(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = entries.Close() })

	collected := put(t, gc, store, "acme", "run-old", "cached output")
	kept := put(t, gc, store, "acme", "run-new", "still referenced output")
	finish(t, runs, "acme", "run-old", 48*time.Hour)
	finish(t, runs, "acme", "run-new", time.Minute)

	staleKey := &dholev1.Digest{Algo: "sha256", Hex: strings.Repeat("a", 64)}
	liveKey := &dholev1.Digest{Algo: "sha256", Hex: strings.Repeat("b", 64)}
	require.NoError(t, entries.Record(ctx, "acme", staleKey,
		[]*dholev1.OutputRef{{Port: "out", Digest: collected}}))
	require.NoError(t, entries.Record(ctx, "acme", liveKey,
		[]*dholev1.OutputRef{{Port: "out", Digest: kept}}))

	_, err = gc.Collect(ctx, "acme", 24*time.Hour)
	require.NoError(t, err)

	_, ok, err := entries.Lookup(ctx, "acme", staleKey)
	require.NoError(t, err)
	require.False(t, ok, "the entry pointing at collected bytes must miss, not dangle")

	outs, ok, err := entries.Lookup(ctx, "acme", liveKey)
	require.NoError(t, err)
	require.True(t, ok, "an entry whose blob survived is still a valid hit")
	require.Equal(t, kept.GetHex(), outs[0].GetDigest().GetHex())
}

// TestCollectIsScopedToOneTenant: a collector that leaks across tenants
// deletes another customer's artifacts on a schedule, which is the worst
// possible way to discover a missing WHERE clause.
func TestCollectIsScopedToOneTenant(t *testing.T) {
	ctx := context.Background()
	gc, store, runs, _ := newGC(t)

	// Identical bytes, so both tenants hold the same digest: only the tenant
	// scope tells the two blobs apart.
	mine := put(t, gc, store, "a", "run-old", "same bytes")
	theirs := put(t, gc, store, "b", "run-old", "same bytes")
	require.Equal(t, mine.GetHex(), theirs.GetHex())
	finish(t, runs, "a", "run-old", 48*time.Hour)
	finish(t, runs, "b", "run-old", 48*time.Hour)

	freed, err := gc.Collect(ctx, "a", 24*time.Hour)
	require.NoError(t, err)
	require.Equal(t, 1, freed)
	require.False(t, has(t, store, "a", mine))
	require.True(t, has(t, store, "b", theirs), "tenant b's blob is not tenant a's to collect")
}

// TestCollectRejectsEmptyTenant: an unscoped collection is not a collection
// over everything, it is a bug — and this one deletes.
func TestCollectRejectsEmptyTenant(t *testing.T) {
	gc, _, _, _ := newGC(t)

	freed, err := gc.Collect(context.Background(), "", 24*time.Hour)
	require.ErrorIs(t, err, runstore.ErrTenantRequired)
	require.EqualError(t, err, "tenant scope required")
	require.Zero(t, freed)
}

// TestCollectToleratesAlreadyMissingBlob: a reference whose bytes are already
// gone describes work already done. Failing the whole collection over it would
// wedge the collector behind one stale row forever.
func TestCollectToleratesAlreadyMissingBlob(t *testing.T) {
	ctx := context.Background()
	gc, store, runs, _ := newGC(t)

	ghost := put(t, gc, store, "acme", "run-old", "bytes that vanish")
	present := put(t, gc, store, "acme", "run-old", "bytes that are here")
	finish(t, runs, "acme", "run-old", 48*time.Hour)

	deleter, ok := store.(cas.Deleter)
	require.True(t, ok)
	require.NoError(t, deleter.Delete(ctx, "acme", ghost))

	freed, err := gc.Collect(ctx, "acme", 24*time.Hour)
	require.NoError(t, err, "an already-collected blob is not a collection failure")
	require.Equal(t, 1, freed, "only the blob this run actually removed counts as freed")
	require.False(t, has(t, store, "acme", present))
}

// TestReferenceRefusesABlobThatIsNotStored closes the window the collector
// cannot otherwise close: a run taking a reference while a collection is
// unlinking. Reference validates under the same write lock the collector
// holds, so a reference recorded after the bytes went away is refused instead
// of becoming a dangling row.
func TestReferenceRefusesABlobThatIsNotStored(t *testing.T) {
	ctx := context.Background()
	gc, _, _, _ := newGC(t)

	absent := &dholev1.Digest{Algo: "sha256", Hex: strings.Repeat("c", 64)}
	require.ErrorIs(t, gc.Reference(ctx, "acme", absent, "run-new"), cas.ErrNotFound)
	require.ErrorIs(t, gc.Reference(ctx, "", absent, "run-new"), runstore.ErrTenantRequired)
}

// TestCollectDoesNotReadAnotherTenantsReferences is the other half of tenant
// scoping, and the half a missing WHERE clause survives: not just whose bytes
// are deleted, but whose rows are consulted to decide. Two tenants building the
// same source hold the same digest, so an unscoped read makes tenant b's live
// run pin tenant a's garbage forever — and an unscoped cache sweep drops
// tenant b's perfectly valid entry.
func TestCollectDoesNotReadAnotherTenantsReferences(t *testing.T) {
	ctx := context.Background()
	gc, store, runs, dbPath := newGC(t)

	entries, err := cache.NewSQLite(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = entries.Close() })

	mine := put(t, gc, store, "a", "run-old", "same bytes")
	theirs := put(t, gc, store, "b", "run-fresh", "same bytes")
	require.Equal(t, mine.GetHex(), theirs.GetHex())
	finish(t, runs, "a", "run-old", 48*time.Hour)
	finish(t, runs, "b", "run-fresh", time.Minute)

	theirKey := &dholev1.Digest{Algo: "sha256", Hex: strings.Repeat("d", 64)}
	require.NoError(t, entries.Record(ctx, "b", theirKey,
		[]*dholev1.OutputRef{{Port: "out", Digest: theirs}}))

	freed, err := gc.Collect(ctx, "a", 24*time.Hour)
	require.NoError(t, err)
	require.Equal(t, 1, freed, "tenant b's live run must not pin tenant a's garbage")
	require.False(t, has(t, store, "a", mine))
	require.True(t, has(t, store, "b", theirs))

	_, ok, err := entries.Lookup(ctx, "b", theirKey)
	require.NoError(t, err)
	require.True(t, ok, "tenant a's collection must not drop tenant b's cache entry")
}

// TestCollectRetainsBlobsOfRunsWithNoRecordedAge covers the run the collector
// knows nothing about: a run that has taken a reference but whose first event
// is not in the log yet — the exact moment a run is at its most fragile. An
// age it cannot compute is not an age of zero. The collector must guess in the
// direction that keeps bytes, because guessing the other way deletes the
// output of a run that is still starting.
func TestCollectRetainsBlobsOfRunsWithNoRecordedAge(t *testing.T) {
	ctx := context.Background()
	gc, store, runs, _ := newGC(t)

	starting := put(t, gc, store, "acme", "run-unlogged", "output of a run with no events")
	stale := put(t, gc, store, "acme", "run-old", "output of a finished run")
	finish(t, runs, "acme", "run-old", 48*time.Hour)

	freed, err := gc.Collect(ctx, "acme", 24*time.Hour)
	require.NoError(t, err)
	require.Equal(t, 1, freed)
	require.True(t, has(t, store, "acme", starting), "a run with no known age is live, not expired")
	require.False(t, has(t, store, "acme", stale))
}
