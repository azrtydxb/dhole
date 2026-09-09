package plugins_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/plugins"
)

const testTenant = "tenant-a"

// registryHost returns the host:port of the registry the integration tests push
// to, or skips. A test that cannot reach its dependency must say so out loud:
// an integration test that silently degrades to nothing reports green while
// proving nothing.
func registryHost(t *testing.T) string {
	t.Helper()
	host := os.Getenv("DHOLE_TEST_OCI_REGISTRY")
	if host == "" {
		t.Skip("DHOLE_TEST_OCI_REGISTRY not set: start the zot service in docker-compose.test.yml and set it to its host:port")
	}
	return host
}

// newResolver builds the resolver under test over a fresh filesystem CAS,
// trusting the test registry over plain HTTP.
func newResolver(t *testing.T, insecureHosts ...string) (plugins.Resolver, cas.Store) {
	t.Helper()
	store := cas.NewFilesystem(t.TempDir())
	return plugins.NewResolver(store, plugins.WithInsecureRegistries(insecureHosts...)), store
}

// pushImage publishes a single-layer image carrying payload at repo:tag and
// returns its manifest digest. Building the image in-process keeps the suite
// free of a docker daemon — the registry is the only real dependency.
func pushImage(t *testing.T, host, repo, tag string, payload []byte) v1.Hash {
	t.Helper()
	// An OCI-typed, single-layer image: the registry validates media types, and
	// the payload is what a plugin author would have appended on top.
	layer := static.NewLayer(payload, types.OCILayer)
	img, err := mutate.AppendLayers(empty.Image, layer)
	require.NoError(t, err)
	img = mutate.MediaType(img, types.OCIManifestSchema1)
	img = mutate.ConfigMediaType(img, types.OCIConfigJSON)

	ref, err := name.NewTag(fmt.Sprintf("%s/%s:%s", host, repo, tag), name.Insecure)
	require.NoError(t, err)
	require.NoError(t, remote.Write(ref, img))

	h, err := img.Digest()
	require.NoError(t, err)
	return h
}

// uniqueRepo keeps runs from colliding with each other's tags in a registry
// that outlives any one `go test`.
func uniqueRepo(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("dhole-test/%s-%d", strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-")), time.Now().UnixNano())
}

func readAll(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	t.Cleanup(func() { _ = rc.Close() })
	b, err := io.ReadAll(rc)
	require.NoError(t, err)
	return b
}

// TestResolverHandlesBothSchemesUniformly is the point of ADR 0011: the policy
// and dispatch layers above must not be able to tell which distribution path an
// artifact came from. If either scheme resolved without a digest, or fetched
// through a different shape, every caller would grow a scheme switch.
func TestResolverHandlesBothSchemesUniformly(t *testing.T) {
	host := registryHost(t)
	ctx := context.Background()
	r, store := newResolver(t, host)

	ociPayload := []byte("plugin payload over oci")
	repo := uniqueRepo(t)
	pushImage(t, host, repo, "v1", ociPayload)

	casPayload := []byte("plugin payload over cas")
	casDigest, err := store.Put(ctx, testTenant, strings.NewReader(string(casPayload)))
	require.NoError(t, err)

	cases := []struct {
		name   string
		ref    string
		scheme plugins.Scheme
		want   []byte
	}{
		{
			name:   "oci",
			ref:    fmt.Sprintf("oci://%s/%s:v1", host, repo),
			scheme: plugins.SchemeOCI,
			want:   ociPayload,
		},
		{
			name:   "cas",
			ref:    fmt.Sprintf("cas://%s:%s", casDigest.GetAlgo(), casDigest.GetHex()),
			scheme: plugins.SchemeCAS,
			want:   casPayload,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, err := r.Resolve(ctx, testTenant, tc.ref)
			require.NoError(t, err)
			require.Equal(t, tc.scheme, a.Scheme)
			require.Equal(t, tc.ref, a.Ref)
			require.NotNil(t, a.Digest, "resolve must populate a digest for every scheme")
			require.Equal(t, "sha256", a.Digest.GetAlgo())
			require.Len(t, a.Digest.GetHex(), 64)
			require.NotEmpty(t, a.MediaType)

			rc, err := r.Fetch(ctx, testTenant, a)
			require.NoError(t, err)
			require.Equal(t, tc.want, readAll(t, rc))
		})
	}
}

// TestTagIsResolvedToDigestAtSaveAndNeverAtDispatch pins the property the whole
// package exists for. A tag is resolved once, when a definition is saved; at
// dispatch only a digest is accepted. Resolving a tag at dispatch would let a
// re-run of a year-old pipeline execute different code than it ran the first
// time, and every cache key folded over the reference would be a lie.
func TestTagIsResolvedToDigestAtSaveAndNeverAtDispatch(t *testing.T) {
	host := registryHost(t)
	ctx := context.Background()
	r, _ := newResolver(t, host)

	repo := uniqueRepo(t)
	want := pushImage(t, host, repo, "v1", []byte("code as saved"))
	ref := fmt.Sprintf("oci://%s/%s:v1", host, repo)

	a, err := r.Resolve(ctx, testTenant, ref)
	require.NoError(t, err)
	require.NotNil(t, a.Digest)
	require.Equal(t, strings.TrimPrefix(want.String(), "sha256:"), a.Digest.GetHex())

	// Dispatch of the same reference with no digest recorded: the resolver must
	// refuse rather than go back to the registry and ask what :v1 means now.
	_, err = r.Fetch(ctx, testTenant, plugins.Artifact{Ref: ref, Scheme: plugins.SchemeOCI})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unresolved tag")
	require.ErrorIs(t, err, plugins.ErrUnresolvedTag)
}

// TestMovedTagDoesNotChangeExistingRevision is the test this task exists for.
// Tags are mutable; a saved revision is not. Once :v1 has been resolved and
// recorded, moving :v1 in the registry must not change a single byte that a
// stored artifact fetches.
func TestMovedTagDoesNotChangeExistingRevision(t *testing.T) {
	host := registryHost(t)
	ctx := context.Background()
	r, _ := newResolver(t, host)

	repo := uniqueRepo(t)
	imageA := []byte("image A: the code the pipeline was saved against")
	digestA := pushImage(t, host, repo, "v1", imageA)

	ref := fmt.Sprintf("oci://%s/%s:v1", host, repo)
	saved, err := r.Resolve(ctx, testTenant, ref)
	require.NoError(t, err)
	require.Equal(t, strings.TrimPrefix(digestA.String(), "sha256:"), saved.Digest.GetHex())

	// Keep A reachable by digest under a second tag. zot drops a manifest that
	// no tag points at any more, and this test is about our resolver's
	// behaviour, not the registry's retention policy: without this the fetch
	// below would fail for the wrong reason.
	pushImage(t, host, repo, "retained-a", imageA)

	// Somebody retags :v1 onto entirely different code.
	imageB := []byte("image B: something else entirely, pushed later")
	digestB := pushImage(t, host, repo, "v1", imageB)
	require.NotEqual(t, digestA.String(), digestB.String(), "the test needs two genuinely different images")

	// A resolve now sees B — tags move, that is what they are for.
	fresh, err := r.Resolve(ctx, testTenant, ref)
	require.NoError(t, err)
	require.Equal(t, strings.TrimPrefix(digestB.String(), "sha256:"), fresh.Digest.GetHex())

	// The saved artifact still fetches A. This is the whole property.
	rc, err := r.Fetch(ctx, testTenant, saved)
	require.NoError(t, err)
	require.Equal(t, imageA, readAll(t, rc))
}

// TestEmptyTenantIsRejected: every stored record and every read is tenant
// scoped, even while one tenant exists. An unscoped fetch would be the one call
// site that reads across tenants once a second one arrives.
func TestEmptyTenantIsRejected(t *testing.T) {
	ctx := context.Background()
	r, _ := newResolver(t)

	_, err := r.Resolve(ctx, "", "cas://sha256:"+strings.Repeat("a", 64))
	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant scope required")
	require.ErrorIs(t, err, plugins.ErrTenantRequired)

	_, err = r.Fetch(ctx, "", plugins.Artifact{
		Ref:    "cas://sha256:" + strings.Repeat("a", 64),
		Scheme: plugins.SchemeCAS,
		Digest: plugins.NewDigest(strings.Repeat("a", 64)),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant scope required")
}

// TestMalformedRefIsRejectedWithTheExpectedForm: the error has to teach, since
// the person reading it is holding a reference they believed was valid.
func TestMalformedRefIsRejectedWithTheExpectedForm(t *testing.T) {
	ctx := context.Background()
	r, _ := newResolver(t)

	for _, ref := range []string{"", "no-scheme-at-all", "oci://", "cas://", "cas://not-a-digest", "://x"} {
		t.Run(ref, func(t *testing.T) {
			_, err := r.Resolve(ctx, testTenant, ref)
			require.Error(t, err)
			require.ErrorIs(t, err, plugins.ErrMalformedRef)
			require.Contains(t, err.Error(), "oci://", "the message must show the expected form")
			require.Contains(t, err.Error(), "cas://")
		})
	}
}

// TestUnknownSchemeIsRejectedNotDefaulted: defaulting an unrecognised scheme to
// one of the two would fetch from somewhere the author never named.
func TestUnknownSchemeIsRejectedNotDefaulted(t *testing.T) {
	ctx := context.Background()
	r, _ := newResolver(t)

	for _, ref := range []string{"https://example.com/plugin.tar", "file:///tmp/plugin.tar", "docker://library/alpine:3"} {
		t.Run(ref, func(t *testing.T) {
			_, err := r.Resolve(ctx, testTenant, ref)
			require.Error(t, err)
			require.ErrorIs(t, err, plugins.ErrUnknownScheme)
			require.NotErrorIs(t, err, plugins.ErrNotFound)
		})
	}

	_, err := r.Fetch(ctx, testTenant, plugins.Artifact{
		Ref:    "https://example.com/plugin.tar",
		Scheme: plugins.Scheme("https"),
		Digest: plugins.NewDigest(strings.Repeat("b", 64)),
	})
	require.ErrorIs(t, err, plugins.ErrUnknownScheme)
}

// TestCASMissLooksLikeNotFoundNotLikeATransportError: a plugin that was never
// mirrored into this tenant's CAS is a different operational problem from a
// store that is down, and the caller has to be able to tell them apart.
func TestCASMissLooksLikeNotFoundNotLikeATransportError(t *testing.T) {
	ctx := context.Background()
	r, _ := newResolver(t)

	absent := strings.Repeat("c", 64)
	_, err := r.Resolve(ctx, testTenant, "cas://sha256:"+absent)
	require.Error(t, err)
	require.ErrorIs(t, err, plugins.ErrNotFound)
	require.NotErrorIs(t, err, plugins.ErrMalformedRef)

	_, err = r.Fetch(ctx, testTenant, plugins.Artifact{
		Ref:    "cas://sha256:" + absent,
		Scheme: plugins.SchemeCAS,
		Digest: plugins.NewDigest(absent),
	})
	require.ErrorIs(t, err, plugins.ErrNotFound)
}

// TestCASRefIsNotAffectedByAnotherTenantsBlob: CAS content is addressed by the
// same digest for everyone, so the only thing keeping tenants apart is the
// scope passed in. A resolver that dropped it would resolve happily against a
// blob it must not see.
func TestCASRefIsNotAffectedByAnotherTenantsBlob(t *testing.T) {
	ctx := context.Background()
	r, store := newResolver(t)

	d, err := store.Put(ctx, "tenant-other", strings.NewReader("someone else's plugin"))
	require.NoError(t, err)

	_, err = r.Resolve(ctx, testTenant, fmt.Sprintf("cas://%s:%s", d.GetAlgo(), d.GetHex()))
	require.ErrorIs(t, err, plugins.ErrNotFound)
}
