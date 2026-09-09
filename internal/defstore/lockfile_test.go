package defstore_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/cache"
	"github.com/azrtydxb/dhole/internal/cas"
	"github.com/azrtydxb/dhole/internal/defstore"
	"github.com/azrtydxb/dhole/internal/plugins"
)

// registryHost returns the host:port of the live registry these tests push to,
// or skips. The moved-tag property cannot be shown against a fake: a fake
// resolver that answers the same digest every time cannot tell a save-time
// resolution from a dispatch-time one, which is exactly the difference this
// file exists to pin. So the registry is a hard dependency, and a run without
// one says so rather than reporting green.
func registryHost(t *testing.T) string {
	t.Helper()
	host := os.Getenv("DHOLE_TEST_OCI_REGISTRY")
	if host == "" {
		t.Skip("DHOLE_TEST_OCI_REGISTRY not set: start the zot service in docker-compose.test.yml and set it to its host:port")
	}
	return host
}

// ociResolver is the real resolver, trusting the test registry over plain HTTP.
func ociResolver(t *testing.T, host string) plugins.Resolver {
	t.Helper()
	return plugins.NewResolver(cas.NewFilesystem(t.TempDir()), plugins.WithInsecureRegistries(host))
}

// pushImage publishes a single-layer image carrying payload at repo:tag and
// returns its manifest digest, building the image in-process so the suite needs
// no docker daemon.
func pushImage(t *testing.T, host, repo, tag string, payload []byte) v1.Hash {
	t.Helper()
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

// uniqueRepo keeps concurrent and repeated runs from colliding over tags in a
// registry that outlives any one `go test`.
func uniqueRepo(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("dhole-test/%s-%d", strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-")), time.Now().UnixNano())
}

// lockfilePipeline builds a pipeline whose steps carry exactly the refs given,
// so a test controls precisely which references the save has to resolve.
func lockfilePipeline(id string, refs ...string) *dholev1.Pipeline {
	p := &dholev1.Pipeline{Id: id, Tenant: &dholev1.Tenant{Id: tenant}}
	for i, ref := range refs {
		p.Steps = append(p.Steps, &dholev1.Step{
			Id:          fmt.Sprintf("step-%d", i),
			Name:        fmt.Sprintf("Step %d", i),
			PluginRef:   ref,
			EffectClass: dholev1.EffectClass_EFFECT_CLASS_PURE,
		})
	}
	return p
}

// artifactFrom rebuilds the artifact a dispatch would fetch from what the
// lockfile recorded: the reference as the author wrote it, and the digest the
// save pinned. Nothing here consults the registry's current view of the tag.
func artifactFrom(t *testing.T, ref, pinned string) plugins.Artifact {
	t.Helper()
	algo, hex, found := strings.Cut(pinned, ":")
	require.True(t, found, "a lockfile entry records algorithm and hex, got %q", pinned)
	require.Equal(t, "sha256", algo)
	return plugins.Artifact{Ref: ref, Digest: plugins.NewDigest(hex), Scheme: plugins.SchemeOCI}
}

// countingResolver answers from a fixed table and records how many times each
// reference was asked for. It is used only where the question is about this
// package's own behaviour — never to stand in for the registry in the
// moved-tag test, where a fake would answer the same digest at save time and
// at dispatch time and so could not fail.
type countingResolver struct {
	digests map[string]string
	calls   map[string]int
}

func (c *countingResolver) Resolve(_ context.Context, tenantID, ref string) (plugins.Artifact, error) {
	if tenantID == "" {
		return plugins.Artifact{}, plugins.ErrTenantRequired
	}
	if c.calls == nil {
		c.calls = map[string]int{}
	}
	c.calls[ref]++
	hex, ok := c.digests[ref]
	if !ok {
		return plugins.Artifact{}, fmt.Errorf("%w: oci resolve %q", plugins.ErrNotFound, ref)
	}
	return plugins.Artifact{Ref: ref, Digest: plugins.NewDigest(hex), Scheme: plugins.SchemeOCI}, nil
}

func (c *countingResolver) Fetch(context.Context, string, plugins.Artifact) (io.ReadCloser, error) {
	return nil, errors.New("countingResolver does not fetch")
}

// stubResolver pins any reference to a digest derived from its text. It is for
// the tests in this package that are about revisions rather than plugins: they
// still have to get past save-time resolution, and nothing they assert depends
// on what the digest is. It is deliberately NOT used anywhere the pinning
// property itself is under test — a resolver that answers the same digest
// forever cannot tell a save-time resolution from a dispatch-time one.
type stubResolver struct{}

func (stubResolver) Resolve(_ context.Context, tenantID, ref string) (plugins.Artifact, error) {
	if tenantID == "" {
		return plugins.Artifact{}, plugins.ErrTenantRequired
	}
	sum := sha256.Sum256([]byte(ref))
	return plugins.Artifact{
		Ref:    ref,
		Digest: plugins.NewDigest(hex.EncodeToString(sum[:])),
		Scheme: plugins.SchemeOCI,
	}, nil
}

func (stubResolver) Fetch(context.Context, string, plugins.Artifact) (io.ReadCloser, error) {
	return nil, errors.New("stubResolver does not fetch")
}

// TestLockfilePinsPluginDigestsAgainstMovedTag is the test this task exists
// for. A definition stops depending on the outside world the moment it is
// saved: every plugin reference is resolved once, there and then, and what the
// revision runs is fixed from that point on. Move the tag upstream afterwards
// and a run of the saved revision must still fetch the original bytes.
func TestLockfilePinsPluginDigestsAgainstMovedTag(t *testing.T) {
	host := registryHost(t)
	ctx := context.Background()
	resolver := ociResolver(t, host)
	store := newStore(t, defstore.WithResolver(resolver))

	repo := uniqueRepo(t)
	imageA := []byte("image A: the code the pipeline was saved against")
	digestA := pushImage(t, host, repo, "v1", imageA)
	ref := fmt.Sprintf("oci://%s/%s:v1", host, repo)

	saved, err := store.Save(ctx, tenant, lockfilePipeline("p1", ref), "ada")
	require.NoError(t, err)
	require.Equal(t, digestA.String(), saved.Lockfile[ref],
		"the save is the one moment the tag is read, and the digest it read is what the revision keeps")

	// Keep A reachable by digest under a second tag: zot drops a manifest no
	// tag points at, and this test is about our pinning, not the registry's
	// retention policy.
	pushImage(t, host, repo, "retained-a", imageA)

	// Somebody retags :v1 onto entirely different code.
	imageB := []byte("image B: something else entirely, pushed later")
	digestB := pushImage(t, host, repo, "v1", imageB)
	require.NotEqual(t, digestA.String(), digestB.String(), "the test needs two genuinely different images")

	// The stored revision is unmoved: read it back from the database, not from
	// the value Save returned, because persistence is half the property.
	stored, err := store.Revision(ctx, tenant, saved.ID)
	require.NoError(t, err)
	require.Equal(t, digestA.String(), stored.Lockfile[ref],
		"a moved tag upstream must not change a byte of a saved revision")

	// A re-save of identical content is the same revision, lockfile and all: an
	// existing revision is never re-pinned to whatever the tag means today.
	again, err := store.Save(ctx, tenant, lockfilePipeline("p1", ref), "someone-else")
	require.NoError(t, err)
	require.Equal(t, saved.ID, again.ID)
	require.Equal(t, digestA.String(), again.Lockfile[ref])

	// And the run: dispatch fetches by the pinned digest and gets image A,
	// while the tag now means image B.
	rc, err := resolver.Fetch(ctx, tenant, artifactFrom(t, ref, stored.Lockfile[ref]))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rc.Close() })
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, imageA, got, "a run of the saved revision fetches the code it was saved against")
}

// TestSaveFailsWhenAPluginCannotBeResolved: a half-resolved lockfile is worse
// than none, because it looks pinned. Save refuses, names the reference the
// operator has to fix, and leaves nothing behind.
func TestSaveFailsWhenAPluginCannotBeResolved(t *testing.T) {
	ctx := context.Background()
	good := "oci://reg.example/good:v1"
	bad := "oci://reg.example/typo:v1"
	resolver := &countingResolver{digests: map[string]string{good: strings.Repeat("a", 64)}}
	store := newStore(t, defstore.WithResolver(resolver))

	p := lockfilePipeline("p1", good, bad)
	_, err := store.Save(ctx, tenant, p, "ada")
	require.Error(t, err)
	require.Contains(t, err.Error(), bad, "the error must name the reference that could not be resolved")
	require.ErrorIs(t, err, plugins.ErrNotFound)

	// Nothing persisted: not the revision, not a lockfile carrying only the
	// half that did resolve.
	_, err = store.Revision(ctx, tenant, "rev_"+defstore.ContentHash(p))
	require.ErrorIs(t, err, defstore.ErrNotFound,
		"a failed resolution must not leave a partially pinned revision behind")
}

// TestLockfileChangeProducesVisibleDiff: an upgrade nobody can see is how a
// supply chain moves without anyone noticing. Re-saving after an intentional
// plugin upgrade yields a diff naming both digests.
func TestLockfileChangeProducesVisibleDiff(t *testing.T) {
	host := registryHost(t)
	ctx := context.Background()
	store := newStore(t, defstore.WithResolver(ociResolver(t, host)))

	repo := uniqueRepo(t)
	oldDigest := pushImage(t, host, repo, "v1", []byte("plugin, version 1"))
	newDigest := pushImage(t, host, repo, "v2", []byte("plugin, version 2"))
	oldRef := fmt.Sprintf("oci://%s/%s:v1", host, repo)
	newRef := fmt.Sprintf("oci://%s/%s:v2", host, repo)

	before, err := store.Save(ctx, tenant, lockfilePipeline("p1", oldRef), "ada")
	require.NoError(t, err)
	after, err := store.Save(ctx, tenant, lockfilePipeline("p1", newRef), "ada")
	require.NoError(t, err)
	require.NotEqual(t, before.ID, after.ID)

	changes := defstore.LockfileDiff(before.Lockfile, after.Lockfile)
	require.NotEmpty(t, changes, "an upgrade that produces no diff is invisible")

	rendered := defstore.FormatLockfileDiff(changes)
	require.Contains(t, rendered, oldDigest.String(), "the diff must show the digest being left behind")
	require.Contains(t, rendered, newDigest.String(), "the diff must show the digest being adopted")
	require.Contains(t, rendered, oldRef)
	require.Contains(t, rendered, newRef)

	// A same-reference upgrade — the lockfile entry whose digest moved — is
	// reported as one change carrying both digests, not as an unrelated
	// removal and addition.
	same := defstore.LockfileDiff(
		map[string]string{oldRef: oldDigest.String()},
		map[string]string{oldRef: newDigest.String()},
	)
	require.Len(t, same, 1)
	require.Equal(t, oldRef, same[0].Ref)
	require.Equal(t, oldDigest.String(), same[0].Old)
	require.Equal(t, newDigest.String(), same[0].New)

	// Nothing changed is an empty diff, never a noisy one.
	require.Empty(t, defstore.LockfileDiff(before.Lockfile, before.Lockfile))
}

// TestCacheKeyChangesWhenLockfileChanges: the Task 15 key folds the lockfile
// in, so two revisions of identical definition text pinned to different plugin
// digests are different cache entries. Were they not, a plugin upgrade would
// serve the previous plugin's outputs — a wrong answer, fast.
func TestCacheKeyChangesWhenLockfileChanges(t *testing.T) {
	host := registryHost(t)
	ctx := context.Background()
	store := newStore(t, defstore.WithResolver(ociResolver(t, host)))

	repo := uniqueRepo(t)
	imageA := []byte("image A")
	digestA := pushImage(t, host, repo, "v1", imageA)
	ref := fmt.Sprintf("oci://%s/%s:v1", host, repo)

	// Two tenants save the identical definition either side of a retag, which
	// is the only way to get two revisions differing in nothing but lockfile:
	// the revision identity is the definition's content, and that is unchanged.
	first, err := store.Save(ctx, "tenant-before", lockfilePipeline("p1", ref), "ada")
	require.NoError(t, err)

	pushImage(t, host, repo, "retained-a", imageA)
	digestB := pushImage(t, host, repo, "v1", []byte("image B"))
	require.NotEqual(t, digestA.String(), digestB.String())

	second, err := store.Save(ctx, "tenant-after", lockfilePipeline("p1", ref), "ada")
	require.NoError(t, err)

	require.Equal(t, first.ContentHash, second.ContentHash, "the definitions differ only in lockfile")
	require.NotEqual(t, first.Lockfile, second.Lockfile)

	step := lockfilePipeline("p1", ref).GetSteps()[0]
	inputs := []*dholev1.Digest{{Algo: "sha256", Hex: strings.Repeat("c", 64)}}

	keyBefore, err := cache.Key(step, "env-1", inputs, first.Lockfile)
	require.NoError(t, err)
	keyAfter, err := cache.Key(step, "env-1", inputs, second.Lockfile)
	require.NoError(t, err)
	require.NotEqual(t, keyBefore.GetHex(), keyAfter.GetHex(),
		"a step whose plugin digest moved must not hit the earlier entry")
}

// TestResolveLockfileRequiresATenant: every operation is tenant scoped, and an
// empty tenant is a bug in the caller rather than a wildcard. Resolution reads
// a tenant's CAS, so an unscoped one would be the call that reads across
// tenants the day a second exists.
func TestResolveLockfileRequiresATenant(t *testing.T) {
	ctx := context.Background()
	resolver := &countingResolver{digests: map[string]string{"oci://reg/img:v1": strings.Repeat("a", 64)}}

	_, err := defstore.ResolveLockfile(ctx, "", lockfilePipeline("p1", "oci://reg/img:v1"), resolver)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant scope required")
	require.ErrorIs(t, err, defstore.ErrTenantRequired)
	require.Empty(t, resolver.calls, "an unscoped call must be refused before anything is resolved")
}

// TestResolveLockfileWithNoResolverRefusesRatherThanPinningNothing: handing
// this function a pipeline to pin and nothing to pin it with is a caller bug.
// Returning an empty lockfile instead would hand back something that reads
// exactly like a definition with no plugins at all.
func TestResolveLockfileWithNoResolverRefusesRatherThanPinningNothing(t *testing.T) {
	_, err := defstore.ResolveLockfile(context.Background(), tenant, lockfilePipeline("p1", "oci://reg/img:v1"), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no resolver configured")
}

// TestPipelineWithoutPluginRefsResolvesToAnEmptyLockfile: a definition with
// nothing to pin is pinned trivially, not rejected. Refusing it would make an
// empty lockfile impossible to tell from a failed one.
func TestPipelineWithoutPluginRefsResolvesToAnEmptyLockfile(t *testing.T) {
	ctx := context.Background()
	resolver := &countingResolver{}

	lockfile, err := defstore.ResolveLockfile(ctx, tenant, lockfilePipeline("p1"), resolver)
	require.NoError(t, err)
	require.NotNil(t, lockfile, "a caller ranges over the lockfile; it is empty, never nil")
	require.Empty(t, lockfile)

	// A step that declares no plugin has nothing to resolve either.
	lockfile, err = defstore.ResolveLockfile(ctx, tenant, lockfilePipeline("p1", ""), resolver)
	require.NoError(t, err)
	require.Empty(t, lockfile)
	require.Empty(t, resolver.calls)

	store := newStore(t, defstore.WithResolver(resolver))
	saved, err := store.Save(ctx, tenant, lockfilePipeline("p1"), "ada")
	require.NoError(t, err)
	require.Empty(t, saved.Lockfile)
}

// TestRepeatedRefIsResolvedOnceAndConsistently: the same plugin on two steps is
// one artifact. Resolving it twice would cost a second round trip and, worse,
// could pin the two steps to different digests if the tag moved in between —
// one definition running two versions of the same plugin.
func TestRepeatedRefIsResolvedOnceAndConsistently(t *testing.T) {
	ctx := context.Background()
	ref := "oci://reg/img:v1"
	other := "oci://reg/other:v1"
	resolver := &countingResolver{digests: map[string]string{
		ref:   strings.Repeat("a", 64),
		other: strings.Repeat("b", 64),
	}}

	lockfile, err := defstore.ResolveLockfile(ctx, tenant, lockfilePipeline("p1", ref, other, ref), resolver)
	require.NoError(t, err)
	require.Len(t, lockfile, 2)
	require.Equal(t, "sha256:"+strings.Repeat("a", 64), lockfile[ref])
	require.Equal(t, 1, resolver.calls[ref], "one reference is one artifact, resolved once")
	require.Equal(t, 1, resolver.calls[other])
}

// TestLockfileContentHashesTheSameWhateverTheIterationOrder: a lockfile is a Go
// map, and map iteration is deliberately randomised. Anything downstream that
// depended on that order would produce a different cache key run to run, which
// is a cache that misses at random and an audit trail that never reproduces.
func TestLockfileContentHashesTheSameWhateverTheIterationOrder(t *testing.T) {
	step := lockfilePipeline("p1", "oci://reg/a:v1").GetSteps()[0]
	refs := []string{"oci://reg/a:v1", "oci://reg/b:v1", "oci://reg/c:v1", "oci://reg/d:v1"}

	var want string
	for i := range 16 {
		lockfile := map[string]string{}
		// Insert in a different order each round; Go's map iteration order is
		// randomised per range anyway, so this only widens the net.
		for j := range refs {
			// A different insertion order each round, with each reference
			// keeping its own digest: only the ORDER varies.
			n := (j + i) % len(refs)
			lockfile[refs[n]] = fmt.Sprintf("sha256:%064d", n)
		}
		key, err := cache.Key(step, "env-1", nil, lockfile)
		require.NoError(t, err)
		if want == "" {
			want = key.GetHex()
			continue
		}
		require.Equal(t, want, key.GetHex(),
			"cache/key.go sorts the lockfile pairs; the key must not depend on map order")
	}
}

// TestSaveRefusesToStoreAnUnpinnedRevision closes the one way an unpinned
// revision could reach the database.
//
// ResolveLockfile has always refused to pin nothing when there is something to
// pin. Save did not call it at all without a resolver, so a store built by a
// caller who simply forgot the option accepted a definition naming plugins and
// stored an empty lockfile beside it. That revision looks pinned — it has a
// lockfile — and it is not, so every guarantee built on top of it (a moved tag
// changing nothing, a cache key that stays honest) is quietly void.
//
// Opting out is still possible, because tests and definitions that name no
// plugin need it, but it now has to be typed: WithoutPinning.
func TestSaveRefusesToStoreAnUnpinnedRevision(t *testing.T) {
	ctx := context.Background()
	store := defstore.New(openTestDB(t))

	_, err := store.Save(ctx, tenant, lockfilePipeline("p1", "oci://reg/img:v1"), "ada")
	require.Error(t, err)
	require.ErrorContains(t, err, "no resolver configured")
	require.ErrorContains(t, err, "WithoutPinning",
		"the error has to name the way out, or the only way past it is to guess")
}

// TestWithoutPinningIsAnExplicitChoice: the opt-out works, and it is the only
// thing that makes an unpinned save legal.
func TestWithoutPinningIsAnExplicitChoice(t *testing.T) {
	ctx := context.Background()
	store := defstore.New(openTestDB(t), defstore.WithoutPinning())

	saved, err := store.Save(ctx, tenant, lockfilePipeline("p1", "oci://reg/img:v1"), "ada")
	require.NoError(t, err)
	require.NotNil(t, saved.Lockfile)
	require.Empty(t, saved.Lockfile, "nothing pinned, and nothing pretending to be pinned")
}

// TestSaveWithoutAResolverIsFineWhenThereIsNothingToPin keeps the refusal
// narrow: it is about unpinned plugin references, not about resolvers.
func TestSaveWithoutAResolverIsFineWhenThereIsNothingToPin(t *testing.T) {
	ctx := context.Background()
	store := defstore.New(openTestDB(t))

	saved, err := store.Save(ctx, tenant, lockfilePipeline("p1"), "ada")
	require.NoError(t, err)
	require.Empty(t, saved.Lockfile)
}
