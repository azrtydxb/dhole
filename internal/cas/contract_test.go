package cas_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/blobstore"
	"github.com/azrtydxb/dhole/internal/cas"
)

// backends is every content-addressed store this repo ships. The properties
// below are the store's contract rather than one implementation's behaviour,
// so each backend answers all of them — a backend added later inherits the
// suite by appearing here.
func backends(t *testing.T) map[string]func() cas.Store {
	t.Helper()
	return map[string]func() cas.Store{
		"filesystem": func() cas.Store { return cas.NewFilesystem(t.TempDir()) },
		"over-blobs": func() cas.Store {
			return cas.NewOverBlobs(blobstore.NewFilesystem(t.TempDir()))
		},
	}
}

func TestEveryBackendIsContentAddressedAndStable(t *testing.T) {
	for name, build := range backends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := build()

			first, err := store.Put(ctx, "acme", strings.NewReader("hello world"))
			require.NoError(t, err)
			require.Equal(t, "sha256", first.GetAlgo())
			require.Len(t, first.GetHex(), 64)

			second, err := store.Put(ctx, "acme", strings.NewReader("hello world"))
			require.NoError(t, err)
			require.Equal(t, first.GetHex(), second.GetHex(),
				"the same bytes must be the same object")

			other, err := store.Put(ctx, "acme", strings.NewReader("goodbye world"))
			require.NoError(t, err)
			require.NotEqual(t, first.GetHex(), other.GetHex())

			rc, err := store.Get(ctx, "acme", first)
			require.NoError(t, err)
			defer func() { require.NoError(t, rc.Close()) }()
			got, err := io.ReadAll(rc)
			require.NoError(t, err)
			require.Equal(t, "hello world", string(got))
		})
	}
}

func TestEveryBackendReportsAMissAsNotFound(t *testing.T) {
	for name, build := range backends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := build()

			absent, err := cas.NewFilesystem(t.TempDir()).Put(ctx, "acme", strings.NewReader("never stored here"))
			require.NoError(t, err)

			has, err := store.Has(ctx, "acme", absent)
			require.NoError(t, err)
			require.False(t, has)

			_, err = store.Get(ctx, "acme", absent)
			require.ErrorIs(t, err, cas.ErrNotFound)
		})
	}
}

func TestNoBackendLetsATenantReadAnothersBlobs(t *testing.T) {
	for name, build := range backends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := build()

			mine, err := store.Put(ctx, "acme", strings.NewReader("acme's secret"))
			require.NoError(t, err)

			has, err := store.Has(ctx, "globex", mine)
			require.NoError(t, err)
			require.False(t, has, "a digest guessed exactly still must not cross tenants")

			_, err = store.Get(ctx, "globex", mine)
			require.ErrorIs(t, err, cas.ErrNotFound)
		})
	}
}

func TestNoBackendAcceptsATenantIDThatCouldEscapeItsScope(t *testing.T) {
	for name, build := range backends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := build()

			for _, bad := range []string{"", ".", "..", "../other", `a\b`} {
				_, err := store.Put(ctx, bad, strings.NewReader("x"))
				require.Error(t, err, "tenant %q was accepted", bad)
			}
		})
	}
}

// The property a mixed or migrated deployment rests on. If two backends
// disagreed about a digest, a step's output stored by one would be invisible
// to the other AND would poison the cache, which is keyed on exactly this.
func TestAllBackendsAgreeOnTheDigestOfTheSameBytes(t *testing.T) {
	ctx := context.Background()
	const payload = "the same bytes, whoever stored them"

	digests := map[string]string{}
	for name, build := range backends(t) {
		d, err := build().Put(ctx, "acme", strings.NewReader(payload))
		require.NoError(t, err)
		digests[name] = d.GetAlgo() + ":" + d.GetHex()
	}

	var want string
	for name, got := range digests {
		if want == "" {
			want = got
			continue
		}
		require.Equal(t, want, got, "backend %q hashes the same bytes differently", name)
	}
}

// A store whose collector cannot delete grows without bound, so Deleter is
// part of what a backend must provide rather than an optional extra.
func TestEveryBackendCanDeleteWhatItStored(t *testing.T) {
	for name, build := range backends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := build()

			d, err := store.Put(ctx, "acme", strings.NewReader("collect me"))
			require.NoError(t, err)

			deleter, ok := store.(cas.Deleter)
			require.True(t, ok, "backend %q cannot be collected", name)
			require.NoError(t, deleter.Delete(ctx, "acme", d))

			has, err := store.Has(ctx, "acme", d)
			require.NoError(t, err)
			require.False(t, has)

			require.ErrorIs(t, deleter.Delete(ctx, "acme", d), cas.ErrNotFound,
				"a second delete must say the work was already done")
		})
	}
}
