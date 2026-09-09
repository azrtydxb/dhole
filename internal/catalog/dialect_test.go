package catalog_test

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
	"github.com/azrtydxb/dhole/internal/catalog"
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

// postgresCatalog opens a catalog over the live Postgres, or skips with a
// reason. An integration test that silently degrades to nothing reports green
// while testing nothing.
func postgresCatalog(t *testing.T) catalog.Store {
	t.Helper()
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set: this test needs a live Postgres")
	}
	db, err := runstore.OpenPostgres(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	c := catalog.New(db, runstore.DialectPostgres)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

// sqliteCatalog opens a catalog over a fresh SQLite file, through the same
// dialect-explicit constructor Postgres uses.
func sqliteCatalog(t *testing.T) catalog.Store {
	t.Helper()
	db, err := runstore.OpenSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	c := catalog.New(db, runstore.DialectSQLite)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

// TestSQLiteSatisfiesCatalogContract and its Postgres twin hold both dialects
// to one contract. A control plane whose catalog cannot be read on the tuned
// target has forgotten what every step type means.
func TestSQLiteSatisfiesCatalogContract(t *testing.T) {
	catalogContract(t, sqliteCatalog(t))
}

func TestPostgresSatisfiesCatalogContract(t *testing.T) {
	catalogContract(t, postgresCatalog(t))
}

// catalogContract is the behaviour the catalog must show on both dialects.
func catalogContract(t *testing.T, store catalog.Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("PublishedTypesResolveBackWhole", func(t *testing.T) {
		scope := uniqueTenant(t)
		m := notifyPlugin()
		require.NoError(t, store.Publish(ctx, scope, m))

		entry, err := store.Resolve(ctx, scope, m.Ref())
		require.NoError(t, err)
		require.Equal(t, m.Namespace, entry.Namespace)
		require.Equal(t, m.Name, entry.Name)
		require.Equal(t, m.Version, entry.Version)
		require.Equal(t, m.Digest.GetHex(), entry.Digest.GetHex())
		require.Equal(t, m.EffectClass, entry.EffectClass)
		require.Equal(t, m.Capabilities, entry.Capabilities)
		require.Equal(t, m.InputSchema, entry.InputSchema)
	})

	t.Run("AnIdenticalRepublishIsANoOpAndADifferingOneIsRefused", func(t *testing.T) {
		scope := uniqueTenant(t)
		m := notifyPlugin()
		require.NoError(t, store.Publish(ctx, scope, m))
		require.NoError(t, store.Publish(ctx, scope, m),
			"publishing is retried; re-running the same deploy must not fail")

		changed := notifyPlugin()
		changed.Digest = &dholev1.Digest{Algo: "sha256", Hex: "different"}
		require.ErrorIs(t, store.Publish(ctx, scope, changed), catalog.ErrVersionExists,
			"a version is immutable: every lockfile already written pins this digest")
	})

	t.Run("ListReturnsTheTenantsCatalogInReferenceOrder", func(t *testing.T) {
		scope := uniqueTenant(t)
		first := notifyPlugin()
		second := notifyPlugin()
		second.Name = "zzz-last"
		require.NoError(t, store.Publish(ctx, scope, second))
		require.NoError(t, store.Publish(ctx, scope, first))

		entries, err := store.List(ctx, scope)
		require.NoError(t, err)
		require.Len(t, entries, 2)
		require.Equal(t, first.Name, entries[0].Name)
		require.Equal(t, second.Name, entries[1].Name)
	})

	t.Run("ResolveIsScopedToItsTenant", func(t *testing.T) {
		mine := uniqueTenant(t)
		theirs := uniqueTenant(t)
		m := notifyPlugin()
		require.NoError(t, store.Publish(ctx, mine, m))

		_, err := store.Resolve(ctx, theirs, m.Ref())
		require.ErrorIs(t, err, catalog.ErrNotFound)

		entries, err := store.List(ctx, theirs)
		require.NoError(t, err)
		require.Empty(t, entries)
	})

	t.Run("AStepInheritsItsPluginsEffectClass", func(t *testing.T) {
		scope := uniqueTenant(t)
		m := notifyPlugin()
		require.NoError(t, store.Publish(ctx, scope, m))

		entry, err := store.ResolveStep(ctx, scope, &dholev1.Step{
			Id:        "notify",
			PluginRef: m.Ref(),
		})
		require.NoError(t, err)
		require.Equal(t, m.EffectClass, entry.EffectClass, "ADR 0002: silence means inherit")
		require.Empty(t, entry.OverrideWarnings)
	})

	t.Run("AnUnscopedQueryIsRefused", func(t *testing.T) {
		require.ErrorIs(t, store.Publish(ctx, "", notifyPlugin()), runstore.ErrTenantRequired)
		_, err := store.Resolve(ctx, "", notifyPlugin().Ref())
		require.ErrorIs(t, err, runstore.ErrTenantRequired)
		_, err = store.List(ctx, "")
		require.ErrorIs(t, err, runstore.ErrTenantRequired)
	})
}
