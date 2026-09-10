package tenancy_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/runstore"
	"github.com/azrtydxb/dhole/internal/tenancy"
)

// GuardCAS was written, tested against a bare store and never put in front of
// a real deployment's blobs. Wrapping it in changes what the blob store IS to
// everything else holding one, and the collector is the half nobody would
// think to check: it asks the store whether it can delete, and a wrapper that
// forgot to pass that on turns reclamation into a no-op that reports an error
// once a day and grows a disk forever.

// guardedGC is the collector over a quota-guarded blob store, which is what a
// wired deployment has.
func guardedGC(t *testing.T) (*cas.GC, cas.Store, runstore.Store) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "dhole.db")

	runs, err := runstore.NewSQLite(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = runs.Close() })

	db, err := cas.OpenIndex(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store, err := tenancy.NewStore(db, runstore.DialectSQLite)
	require.NoError(t, err)
	enforcer, err := tenancy.NewEnforcer(tenancy.EnforcerConfig{Store: store})
	require.NoError(t, err)

	guarded, err := tenancy.GuardCAS(cas.NewFilesystem(filepath.Join(dir, "blobs")), enforcer)
	require.NoError(t, err)

	return &cas.GC{Store: guarded, Runs: runs, DB: db}, guarded, runs
}

// TestAQuotaGuardedBlobStoreCanStillBeCollectedFrom. cas.GC asks its store
// whether it can delete and refuses to collect anything when it cannot. A
// guard that answered "no" would leave every deployment that enforces a
// storage quota unable to reclaim a byte of it — a quota that fills up and a
// collector that cannot empty it is the worst of both.
func TestAQuotaGuardedBlobStoreCanStillBeCollectedFrom(t *testing.T) {
	ctx := context.Background()
	gc, store, runs := guardedGC(t)

	digest, err := store.Put(ctx, "acme", strings.NewReader("collect me"))
	require.NoError(t, err)
	require.NoError(t, gc.Reference(ctx, "acme", digest, "run-1"))
	require.NoError(t, runs.Append(ctx, "acme", runstore.Event{
		RunID: "run-1", StepID: "step", Attempt: 1, Sequence: 1,
		Type: runstore.RunCompleted, At: time.Now().UTC().Add(-48 * time.Hour),
	}))

	collected, err := gc.Collect(ctx, "acme", time.Hour)
	require.NoError(t, err, "the guard hid the store's ability to delete from the collector")
	require.Equal(t, 1, collected, "the blob of an expired run was not reclaimed")

	held, err := store.Has(ctx, "acme", digest)
	require.NoError(t, err)
	require.False(t, held, "the collector reported a deletion that did not happen")
}

// TestAQuotaGuardOverAStoreThatCannotDeleteDoesNotClaimItCan. The other half
// of the same property: the guard must not ADD a capability its inner store
// does not have, or the collector proceeds to delete blob after blob and fails
// on each one, having already dropped their references.
func TestAQuotaGuardOverAStoreThatCannotDeleteDoesNotClaimItCan(t *testing.T) {
	db, err := cas.OpenIndex(filepath.Join(t.TempDir(), "dhole.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	store, err := tenancy.NewStore(db, runstore.DialectSQLite)
	require.NoError(t, err)
	enforcer, err := tenancy.NewEnforcer(tenancy.EnforcerConfig{Store: store})
	require.NoError(t, err)

	guarded, err := tenancy.GuardCAS(readOnlyCAS{}, enforcer)
	require.NoError(t, err)
	require.NotImplements(t, (*cas.Deleter)(nil), guarded,
		"the guard claims a store that cannot delete can")
}

// readOnlyCAS is a store with no Delete, which is what a future backend
// without one looks like.
type readOnlyCAS struct{ cas.Store }
