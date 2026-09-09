package cache_test

import (
	"context"
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
	"github.com/azrtydxb/dhole/internal/runstore"
)

// uniqueTenant keeps every dialect run in its own tenant. SQLite gets a fresh
// file per test; Postgres is shared and persistent, so an assertion that only
// holds because an earlier run's rows were absent is not an assertion.
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

// postgresCache opens a cache over the live Postgres, or skips with a reason.
// An integration test that silently degrades to nothing reports green while
// testing nothing.
func postgresCache(t *testing.T) *cache.Cache {
	t.Helper()
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set: this test needs a live Postgres")
	}
	db, err := runstore.OpenPostgres(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	c := cache.New(db, runstore.DialectPostgres)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

// sqliteCache opens a cache over a fresh SQLite file, through the same
// dialect-explicit constructor Postgres uses.
func sqliteCache(t *testing.T) *cache.Cache {
	t.Helper()
	db, err := runstore.OpenSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	c := cache.New(db, runstore.DialectSQLite)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

// TestSQLiteSatisfiesCacheContract and its Postgres twin hold both dialects to
// one contract. A cache that records on the development store and refuses on
// the tuned target turns every rebuild in production back into an execution.
func TestSQLiteSatisfiesCacheContract(t *testing.T) {
	cacheContract(t, sqliteCache(t))
}

func TestPostgresSatisfiesCacheContract(t *testing.T) {
	cacheContract(t, postgresCache(t))
}

// cacheContract is the behaviour the cache must show on both dialects.
func cacheContract(t *testing.T, c *cache.Cache) {
	t.Helper()

	key := func(hex string) *dholev1.Digest {
		return &dholev1.Digest{Algo: "sha256", Hex: hex}
	}
	outputs := func(hex string) []*dholev1.OutputRef {
		return []*dholev1.OutputRef{{
			Port:   "out",
			Digest: &dholev1.Digest{Algo: "sha256", Hex: hex},
		}}
	}

	t.Run("AMissIsTheOrdinaryAnswer", func(t *testing.T) {
		ctx := context.Background()
		outs, hit, err := c.Lookup(ctx, uniqueTenant(t), key("aaa"))
		require.NoError(t, err, "a miss is not an error: it means the step has to run")
		require.False(t, hit)
		require.Empty(t, outs)
	})

	t.Run("RecordedOutputsComeBackWhole", func(t *testing.T) {
		ctx := context.Background()
		scope := uniqueTenant(t)

		require.NoError(t, c.Record(ctx, scope, key("aaa"), outputs("beef")))
		outs, hit, err := c.Lookup(ctx, scope, key("aaa"))
		require.NoError(t, err)
		require.True(t, hit)
		require.Len(t, outs, 1)
		require.Equal(t, "out", outs[0].GetPort())
		require.Equal(t, "beef", outs[0].GetDigest().GetHex())
	})

	t.Run("RecordIsIdempotentOnTheKey", func(t *testing.T) {
		ctx := context.Background()
		scope := uniqueTenant(t)

		require.NoError(t, c.Record(ctx, scope, key("aaa"), outputs("beef")))
		// At-least-once delivery makes a re-reported success routine, and the
		// same key is by construction the same work: the row is refreshed
		// rather than duplicated or refused.
		require.NoError(t, c.Record(ctx, scope, key("aaa"), outputs("cafe")))

		outs, hit, err := c.Lookup(ctx, scope, key("aaa"))
		require.NoError(t, err)
		require.True(t, hit)
		require.Len(t, outs, 1)
		require.Equal(t, "cafe", outs[0].GetDigest().GetHex())
	})

	t.Run("EntriesAreScopedToTheirTenant", func(t *testing.T) {
		ctx := context.Background()
		mine := uniqueTenant(t)
		theirs := uniqueTenant(t)

		require.NoError(t, c.Record(ctx, mine, key("aaa"), outputs("beef")))
		_, hit, err := c.Lookup(ctx, theirs, key("aaa"))
		require.NoError(t, err)
		require.False(t, hit, "the key is a pure function of content: only the tenant keeps results apart")
	})

	t.Run("AnUnscopedLookupIsRefused", func(t *testing.T) {
		ctx := context.Background()
		_, _, err := c.Lookup(context.Background(), "", key("aaa"))
		require.ErrorIs(t, err, runstore.ErrTenantRequired)
		require.ErrorIs(t, c.Record(ctx, "", key("aaa"), outputs("beef")), runstore.ErrTenantRequired)
	})
}
