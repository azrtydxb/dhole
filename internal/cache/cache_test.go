package cache_test

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/cas"
)

const tenant = "acme"

// openCache opens a cache over a fresh SQLite file, applying the embedded
// migrations, and closes it when the test ends.
func openCache(t *testing.T) *cache.Cache {
	t.Helper()
	c, err := cache.NewSQLite(filepath.Join(t.TempDir(), "runs.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c
}

// TestCacheHitOnUnchangedInputsAndInvalidationOnChange runs the whole primitive
// end to end over the two stores it actually lives on: the SQLite run store
// holds the entries, the filesystem CAS holds the bytes. The point of ADR 0009
// is that a rebuild on unchanged inputs is a lookup rather than an execution —
// and that a single changed byte anywhere upstream turns it back into one.
func TestCacheHitOnUnchangedInputsAndInvalidationOnChange(t *testing.T) {
	ctx := context.Background()
	blobs := cas.NewFilesystem(t.TempDir())
	entries := openCache(t)

	step := pureStep()
	const env = "sha256:image-identity"
	lockfile := map[string]string{"acme/build": "1.4.2"}

	source, err := blobs.Put(ctx, tenant, strings.NewReader("package main\n"))
	require.NoError(t, err)

	key, err := cache.Key(step, env, []*dholev1.Digest{source}, lockfile)
	require.NoError(t, err)

	// Nothing has ever been built: the first look must miss, or the very first
	// run of a pipeline would skip work it never did.
	got, hit, err := entries.Lookup(ctx, tenant, key)
	require.NoError(t, err)
	require.False(t, hit)
	require.Nil(t, got)

	// The step "runs": its output lands in the CAS and the result is recorded.
	artifact, err := blobs.Put(ctx, tenant, strings.NewReader("compiled binary"))
	require.NoError(t, err)
	outs := []*dholev1.OutputRef{{
		Port:      "binary",
		Digest:    artifact,
		Key:       "runs/run-1/binary",
		SizeBytes: uint64(len("compiled binary")),
	}}
	require.NoError(t, entries.Record(ctx, tenant, key, outs))

	// The same inputs a second time: a hit, carrying back references good
	// enough to skip the step entirely.
	got, hit, err = entries.Lookup(ctx, tenant, key)
	require.NoError(t, err)
	require.True(t, hit)
	require.Len(t, got, 1)
	require.Equal(t, "binary", got[0].GetPort())
	require.Equal(t, artifact.GetHex(), got[0].GetDigest().GetHex())
	require.Equal(t, "sha256", got[0].GetDigest().GetAlgo())
	require.Equal(t, "runs/run-1/binary", got[0].GetKey())
	require.Equal(t, uint64(len("compiled binary")), got[0].GetSizeBytes())

	// The recorded reference must still resolve to real bytes: a hit that
	// points at nothing is worse than a miss.
	reader, err := blobs.Get(ctx, tenant, got[0].GetDigest())
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "compiled binary", string(content))

	// Edit the source. The input digest changes, so the key changes, so the
	// entry recorded above is unreachable — invalidation is a property of the
	// key, not a sweep somebody has to remember to run.
	edited, err := blobs.Put(ctx, tenant, strings.NewReader("package main // edited\n"))
	require.NoError(t, err)
	require.NotEqual(t, source.GetHex(), edited.GetHex())

	editedKey, err := cache.Key(step, env, []*dholev1.Digest{edited}, lockfile)
	require.NoError(t, err)
	require.NotEqual(t, key.GetHex(), editedKey.GetHex())

	got, hit, err = entries.Lookup(ctx, tenant, editedKey)
	require.NoError(t, err)
	require.False(t, hit, "changed inputs must miss")
	require.Nil(t, got)

	// Bumping the pinned plugin version invalidates just as surely: the same
	// source built by a different tool is a different artifact.
	bumpedKey, err := cache.Key(step, env, []*dholev1.Digest{source},
		map[string]string{"acme/build": "1.5.0"})
	require.NoError(t, err)
	_, hit, err = entries.Lookup(ctx, tenant, bumpedKey)
	require.NoError(t, err)
	require.False(t, hit, "a changed plugin lockfile must miss")
}

// TestCacheIsTenantScoped keeps one tenant's entries from answering another
// tenant's lookup. The key is a pure function of content, so two tenants
// running the same build produce the same key; only the tenant column keeps
// their results apart, and there is no unscoped read in this system.
func TestCacheIsTenantScoped(t *testing.T) {
	ctx := context.Background()
	entries := openCache(t)

	key, err := cache.Key(pureStep(), "sha256:image", []*dholev1.Digest{digest(hexA)}, nil)
	require.NoError(t, err)

	require.NoError(t, entries.Record(ctx, "acme", key, []*dholev1.OutputRef{
		{Port: "binary", Digest: digest(hexB)},
	}))

	_, hit, err := entries.Lookup(ctx, "globex", key)
	require.NoError(t, err)
	require.False(t, hit, "another tenant's entry must not answer this lookup")

	_, hit, err = entries.Lookup(ctx, "acme", key)
	require.NoError(t, err)
	require.True(t, hit)
}

// TestRecordIsIdempotentAndRefusesUnscopedAccess covers the two ways the
// caller can get this wrong: a redelivered success re-recording the same
// result, and a missing tenant. At-least-once delivery makes the first
// routine; the second must be a refusal, never a wildcard.
func TestRecordIsIdempotentAndRefusesUnscopedAccess(t *testing.T) {
	ctx := context.Background()
	entries := openCache(t)

	key, err := cache.Key(pureStep(), "sha256:image", []*dholev1.Digest{digest(hexA)}, nil)
	require.NoError(t, err)
	outs := []*dholev1.OutputRef{{Port: "binary", Digest: digest(hexB)}}

	require.NoError(t, entries.Record(ctx, tenant, key, outs))
	require.NoError(t, entries.Record(ctx, tenant, key, outs), "a redelivered result must not fail")

	got, hit, err := entries.Lookup(ctx, tenant, key)
	require.NoError(t, err)
	require.True(t, hit)
	require.Len(t, got, 1)

	require.Error(t, entries.Record(ctx, "", key, outs))
	_, _, err = entries.Lookup(ctx, "", key)
	require.Error(t, err)
}
