package cas_test

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cas"
)

// TestPutIsContentAddressedAndStable pins the property the whole cache rests
// on (ADR 0001): identity is the hash of the bytes. The same bytes put twice
// must yield the same digest, and different bytes must never collide onto it.
func TestPutIsContentAddressedAndStable(t *testing.T) {
	ctx := context.Background()
	store := cas.NewFilesystem(t.TempDir())

	first, err := store.Put(ctx, "acme", strings.NewReader("hello world"))
	require.NoError(t, err)
	require.Equal(t, "sha256", first.GetAlgo())
	require.Len(t, first.GetHex(), 64)

	second, err := store.Put(ctx, "acme", strings.NewReader("hello world"))
	require.NoError(t, err)
	require.Equal(t, first.GetHex(), second.GetHex())

	has, err := store.Has(ctx, "acme", first)
	require.NoError(t, err)
	require.True(t, has)

	other, err := store.Put(ctx, "acme", strings.NewReader("goodbye world"))
	require.NoError(t, err)
	require.NotEqual(t, first.GetHex(), other.GetHex())
}

// TestGetMissingDigestReturnsNotFound: a miss is a named, matchable condition,
// not an opaque os error, so callers (the cache lookup above all) can tell
// "not stored" apart from "storage is broken".
func TestGetMissingDigestReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	store := cas.NewFilesystem(t.TempDir())

	absent := &dholev1.Digest{
		Algo: "sha256",
		Hex:  "0000000000000000000000000000000000000000000000000000000000000000",
	}

	rc, err := store.Get(ctx, "acme", absent)
	require.Nil(t, rc)
	require.ErrorIs(t, err, cas.ErrNotFound)
}

// TestTenantsCannotReadEachOthersBlobs: tenant isolation is structural — the
// tenant is part of the path, so another tenant's blob is not merely filtered
// out of the answer, it is not on the lookup path at all.
func TestTenantsCannotReadEachOthersBlobs(t *testing.T) {
	ctx := context.Background()
	store := cas.NewFilesystem(t.TempDir())

	d, err := store.Put(ctx, "a", strings.NewReader("tenant a secret"))
	require.NoError(t, err)

	has, err := store.Has(ctx, "b", d)
	require.NoError(t, err)
	require.False(t, has)

	rc, err := store.Get(ctx, "b", d)
	require.Nil(t, rc)
	require.ErrorIs(t, err, cas.ErrNotFound)
}

// TestTenantIDThatWouldEscapeTheRootIsRejected: a tenant id is a path segment,
// so "../" or a separator in it would place one tenant's blobs inside another's
// tree, or outside the root entirely. Rejected loudly, never sanitised into
// something that silently addresses the wrong place.
func TestTenantIDThatWouldEscapeTheRootIsRejected(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := cas.NewFilesystem(root)

	for _, tenant := range []string{"..", "../evil", "a/b", "", ".", `..\evil`, `a\b`, "nul\x00byte"} {
		_, err := store.Put(ctx, tenant, strings.NewReader("payload"))
		require.ErrorIs(t, err, cas.ErrInvalidTenant, "Put(%q)", tenant)

		_, err = store.Has(ctx, tenant, &dholev1.Digest{Algo: "sha256", Hex: strings.Repeat("a", 64)})
		require.ErrorIs(t, err, cas.ErrInvalidTenant, "Has(%q)", tenant)
	}

	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	require.Empty(t, entries, "a rejected tenant must not create anything under the root")
}

// TestFailedPutLeavesNoPartialObject: a Put that dies mid-stream must leave the
// store as it was. The blob is only ever made visible by the rename, and the
// temp file is cleaned up rather than left to accumulate.
func TestFailedPutLeavesNoPartialObject(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := cas.NewFilesystem(root)

	_, err := store.Put(ctx, "acme", io.MultiReader(
		strings.NewReader("half a blob"),
		iotest.ErrReader(errors.New("connection reset")),
	))
	require.Error(t, err)

	var files []string
	require.NoError(t, filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, path)
		}
		return nil
	}))
	require.Empty(t, files, "a failed Put must leave neither a partial object nor a temp file")
}

// TestPutOfExistingBytesIsCheapAndDoesNotCorruptTheObject: re-putting bytes
// already stored is the common case on a cache hit. It must not truncate or
// rewrite the object another reader may be streaming.
func TestPutOfExistingBytesIsCheapAndDoesNotCorruptTheObject(t *testing.T) {
	ctx := context.Background()
	store := cas.NewFilesystem(t.TempDir())

	const payload = "artifact bytes"
	d, err := store.Put(ctx, "acme", strings.NewReader(payload))
	require.NoError(t, err)

	rc, err := store.Get(ctx, "acme", d)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rc.Close() })

	again, err := store.Put(ctx, "acme", strings.NewReader(payload))
	require.NoError(t, err)
	require.Equal(t, d.GetHex(), again.GetHex())

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, payload, string(got))
}

// TestMalformedDigestIsRejectedRatherThanProbed: a digest that is nil, of an
// unknown algorithm, or not 64 lowercase hex characters cannot name a stored
// object. It is refused before it ever becomes a path.
func TestMalformedDigestIsRejectedRatherThanProbed(t *testing.T) {
	ctx := context.Background()
	store := cas.NewFilesystem(t.TempDir())

	for name, d := range map[string]*dholev1.Digest{
		"nil":                nil,
		"empty":              {},
		"unknown algo":       {Algo: "md5", Hex: strings.Repeat("a", 32)},
		"short hex":          {Algo: "sha256", Hex: "abc"},
		"uppercase hex":      {Algo: "sha256", Hex: strings.Repeat("A", 64)},
		"non-hex characters": {Algo: "sha256", Hex: strings.Repeat("z", 64)},
	} {
		_, err := store.Has(ctx, "acme", d)
		require.Error(t, err, name)
		require.NotErrorIs(t, err, cas.ErrNotFound, name)
	}
}
