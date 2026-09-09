package cas_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// uniqueTenant keeps every dialect run in its own tenant. SQLite gets a fresh
// file per test; Postgres is shared and persistent, so a collection that only
// freed one blob because the others were absent proves nothing.
var (
	tenantSeq atomic.Uint64
	tenantRun = strconv.FormatInt(time.Now().UnixNano(), 36)
)

func uniqueTenant(t *testing.T) string {
	t.Helper()
	name := strings.Map(func(r rune) rune {
		if r == '/' || r == ' ' {
			return '-'
		}
		return r
	}, t.Name())
	return fmt.Sprintf("%s-%s-%d", name, tenantRun, tenantSeq.Add(1))
}

// collector is everything one dialect's contract run needs: the GC itself, the
// blob store behind it, the run log it ages runs from, and a cache sharing the
// same database — because dropping a stale cache entry with the blob it names
// is part of the contract, not an extra.
type collector struct {
	gc      *cas.GC
	store   cas.Store
	runs    runstore.Store
	entries *cache.Cache
}

// sqliteCollector wires the collector over a fresh SQLite file, through the
// same dialect-explicit constructors Postgres uses.
func sqliteCollector(t *testing.T) collector {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "dhole.db")

	runs, err := runstore.NewSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = runs.Close() })

	db, err := runstore.OpenSQLite(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return newCollector(t, db, runstore.DialectSQLite, runs, filepath.Join(dir, "blobs"))
}

// postgresCollector wires the collector over the live Postgres, or skips with
// a reason. An integration test that silently degrades to nothing reports
// green while testing nothing.
func postgresCollector(t *testing.T) collector {
	t.Helper()
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set: this test needs a live Postgres")
	}
	ctx := context.Background()

	runs, err := runstore.NewPostgres(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = runs.Close() })

	db, err := runstore.OpenPostgres(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// The blobs themselves stay on the filesystem in both runs: this test is
	// about the reference index's dialect, not about where bytes live.
	return newCollector(t, db, runstore.DialectPostgres, runs, t.TempDir())
}

func newCollector(
	t *testing.T, db *sql.DB, dialect runstore.Dialect, runs runstore.Store, blobDir string,
) collector {
	t.Helper()
	store := cas.NewFilesystem(blobDir)
	entries := cache.New(db, dialect)
	return collector{
		gc:      &cas.GC{Store: store, Runs: runs, DB: db, Dialect: dialect},
		store:   store,
		runs:    runs,
		entries: entries,
	}
}

// TestSQLiteSatisfiesCollectorContract and its Postgres twin hold both
// dialects to one contract. A collector that cannot read its reference index
// on the tuned target either deletes nothing forever or, worse, reads an empty
// index and deletes bytes a live run still needs.
func TestSQLiteSatisfiesCollectorContract(t *testing.T) {
	collectorContract(t, sqliteCollector(t))
}

func TestPostgresSatisfiesCollectorContract(t *testing.T) {
	collectorContract(t, postgresCollector(t))
}

// collectorContract is the behaviour the collector must show on both dialects.
func collectorContract(t *testing.T, c collector) {
	t.Helper()
	ctx := context.Background()

	put := func(t *testing.T, scope, runID, content string) *dholev1.Digest {
		t.Helper()
		d, err := c.store.Put(ctx, scope, strings.NewReader(content))
		require.NoError(t, err)
		require.NoError(t, c.gc.Reference(ctx, scope, d, runID))
		return d
	}
	finish := func(t *testing.T, scope, runID string, age time.Duration) {
		t.Helper()
		require.NoError(t, c.runs.Append(ctx, scope, runstore.Event{
			RunID: runID, StepID: "step", Attempt: 1, Sequence: 1,
			Type: runstore.RunCompleted, At: time.Now().UTC().Add(-age),
		}))
	}
	has := func(t *testing.T, scope string, d *dholev1.Digest) bool {
		t.Helper()
		ok, err := c.store.Has(ctx, scope, d)
		require.NoError(t, err)
		return ok
	}

	t.Run("RetainedRunBlobsSurviveAndExpiredOnesAreReclaimed", func(t *testing.T) {
		scope := uniqueTenant(t)
		expired := put(t, scope, "run-old", "aged output "+scope)
		retained := put(t, scope, "run-new", "fresh output "+scope)
		finish(t, scope, "run-old", 48*time.Hour)
		finish(t, scope, "run-new", time.Minute)

		freed, err := c.gc.Collect(ctx, scope, 24*time.Hour)
		require.NoError(t, err)
		require.Equal(t, 1, freed)
		require.False(t, has(t, scope, expired), "an expired run's blob is collectable")
		require.True(t, has(t, scope, retained), "a retained run's blob must survive")
	})

	t.Run("ABlobSharedWithARetainedRunIsNeverCollected", func(t *testing.T) {
		scope := uniqueTenant(t)
		shared := put(t, scope, "run-old", "shared output "+scope)
		require.NoError(t, c.gc.Reference(ctx, scope, shared, "run-new"))
		lonely := put(t, scope, "run-old", "lonely output "+scope)
		finish(t, scope, "run-old", 48*time.Hour)
		finish(t, scope, "run-new", time.Minute)

		freed, err := c.gc.Collect(ctx, scope, 24*time.Hour)
		require.NoError(t, err)
		require.Equal(t, 1, freed, "only the blob with no retained reference is freed")
		require.True(t, has(t, scope, shared))
		require.False(t, has(t, scope, lonely))
	})

	t.Run("ACacheEntryGoesWithTheBlobItNames", func(t *testing.T) {
		scope := uniqueTenant(t)
		collected := put(t, scope, "run-old", "cached output "+scope)
		kept := put(t, scope, "run-new", "still referenced "+scope)
		finish(t, scope, "run-old", 48*time.Hour)
		finish(t, scope, "run-new", time.Minute)

		staleKey := &dholev1.Digest{Algo: "sha256", Hex: strings.Repeat("a", 64)}
		liveKey := &dholev1.Digest{Algo: "sha256", Hex: strings.Repeat("b", 64)}
		require.NoError(t, c.entries.Record(ctx, scope, staleKey,
			[]*dholev1.OutputRef{{Port: "out", Digest: collected}}))
		require.NoError(t, c.entries.Record(ctx, scope, liveKey,
			[]*dholev1.OutputRef{{Port: "out", Digest: kept}}))

		freed, err := c.gc.Collect(ctx, scope, 24*time.Hour)
		require.NoError(t, err)
		require.Equal(t, 1, freed)

		_, hit, err := c.entries.Lookup(ctx, scope, staleKey)
		require.NoError(t, err)
		require.False(t, hit, "a hit handing back bytes nobody can read fails downstream")

		_, hit, err = c.entries.Lookup(ctx, scope, liveKey)
		require.NoError(t, err)
		require.True(t, hit, "the entry whose blob survived must survive with it")
	})

	t.Run("CollectionIsScopedToItsTenant", func(t *testing.T) {
		mine := uniqueTenant(t)
		theirs := uniqueTenant(t)

		ours := put(t, mine, "run-old", "our aged output "+mine)
		hers := put(t, theirs, "run-old", "their aged output "+theirs)
		finish(t, mine, "run-old", 48*time.Hour)
		finish(t, theirs, "run-old", 48*time.Hour)

		freed, err := c.gc.Collect(ctx, mine, 24*time.Hour)
		require.NoError(t, err)
		require.Equal(t, 1, freed, "a collection must never reach into another tenant's bytes")
		require.False(t, has(t, mine, ours))
		require.True(t, has(t, theirs, hers))
	})

	t.Run("AnUnscopedCollectionIsRefused", func(t *testing.T) {
		_, err := c.gc.Collect(ctx, "", time.Hour)
		require.ErrorIs(t, err, runstore.ErrTenantRequired)
		require.ErrorIs(t,
			c.gc.Reference(ctx, "", &dholev1.Digest{Algo: "sha256", Hex: "aa"}, "run-1"),
			runstore.ErrTenantRequired)
	})
}
