package plugins_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/plugins"
	"github.com/azrtydxb/dhole/internal/runstore"
)

// The two federations' release bots. They are DIFFERENT principals: trust is
// per-upstream, and a signer vouched for in one federation is not
// automatically vouched for in another.
const (
	identityFedA = "release-bot@a.example"
	identityFedB = "release-bot@b.example"
)

// startUpstreamRegistry runs a real OCI distribution registry in-process and
// returns its host:port together with the function that STOPS it.
//
// It is a real registry — go-containerregistry's own in-memory implementation,
// serving _catalog, tags/list, manifests and blobs — reached over a real
// socket, and stop() closes that socket. That matters more than it looks: a
// fake upstream that is always reachable cannot prove the mirror is what
// served a later fetch, because nothing distinguishes "read from the mirror"
// from "read from the upstream again".
func startUpstreamRegistry(t *testing.T) (host string, stop func()) {
	t.Helper()
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	var closed bool
	stop = func() {
		if !closed {
			closed = true
			srv.Close()
		}
	}
	t.Cleanup(stop)
	return strings.TrimPrefix(srv.URL, "http://"), stop
}

// requireUnreachable proves the upstream is genuinely down before a test claims
// the mirror served anything.
func requireUnreachable(t *testing.T, host string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + host + "/v2/") //nolint:noctx // a liveness probe, not a client call
	if err == nil {
		_ = resp.Body.Close()
		t.Fatalf("upstream %s is still answering: the outage this test depends on never happened", host)
	}
}

// pushSignedImage publishes a plugin image and, beside it, the cosign
// signature an upstream would carry: the artifact and its provenance, which is
// what sync has to pick up.
func pushSignedImage(t *testing.T, host, repo, tag string, payload []byte, identity, issuer string) *dholev1.Digest {
	t.Helper()
	h := pushImage(t, host, repo, tag, payload)
	d := plugins.NewDigest(h.Hex)
	pushCosignSignature(t, host, repo, d, identity, issuer)
	return d
}

// pushCosignSignature writes the cosign signature artifact for d at the
// conventional `sha256-<hex>.sig` tag: one layer carrying the simple-signing
// payload, with the signature and certificate in its annotations.
func pushCosignSignature(t *testing.T, host, repo string, d *dholev1.Digest, identity, issuer string) {
	t.Helper()
	var bundle struct {
		Base64Signature string `json:"base64Signature"`
		Cert            string `json:"cert"`
		Payload         string `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(signedBundleFor(t, d, identity, issuer), &bundle))

	payload, err := base64.StdEncoding.DecodeString(bundle.Payload)
	require.NoError(t, err)
	certPEM, err := base64.StdEncoding.DecodeString(bundle.Cert)
	require.NoError(t, err)

	layer := static.NewLayer(payload, types.MediaType("application/vnd.dev.cosign.simplesigning.v1+json"))
	img, err := mutate.Append(empty.Image, mutate.Addendum{
		Layer: layer,
		Annotations: map[string]string{
			"dev.cosignproject.cosign/signature": bundle.Base64Signature,
			"dev.sigstore.cosign/certificate":    string(certPEM),
		},
	})
	require.NoError(t, err)
	img = mutate.MediaType(img, types.OCIManifestSchema1)
	img = mutate.ConfigMediaType(img, types.OCIConfigJSON)

	ref, err := name.NewTag(fmt.Sprintf("%s/%s:sha256-%s.sig", host, repo, d.GetHex()), name.Insecure)
	require.NoError(t, err)
	require.NoError(t, remote.Write(ref, img))
}

// upstreamsOver builds the federation store under test over an open handle:
// the registrations and the signature records live in the database, and the
// mirror itself is a repository prefix on a registry this deployment controls.
func upstreamsOver(t *testing.T, db *sql.DB, dialect runstore.Dialect, prefix string, insecure ...string) plugins.Upstreams {
	t.Helper()
	sigs := plugins.NewSignatures(db, dialect)
	ups := plugins.NewUpstreams(db, dialect, sigs, prefix, plugins.WithInsecureUpstreams(insecure...))
	t.Cleanup(func() { require.NoError(t, ups.Close()) })
	return ups
}

// newUpstreams is the SQLite store: a fresh file per test.
func newUpstreams(t *testing.T, prefix string, insecure ...string) plugins.Upstreams {
	t.Helper()
	ups, _ := newUpstreamsAndSignatures(t, prefix, insecure...)
	return ups
}

// newUpstreamsAndSignatures hands back the signature store too, for the tests
// that need to withdraw a signature behind the mirror's back.
func newUpstreamsAndSignatures(t *testing.T, prefix string, insecure ...string) (plugins.Upstreams, plugins.Signatures) {
	t.Helper()
	db, err := runstore.OpenSQLite(filepath.Join(t.TempDir(), "dhole.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return upstreamsOver(t, db, runstore.DialectSQLite, prefix, insecure...), plugins.NewSignatures(db, runstore.DialectSQLite)
}

// postgresUpstreams is the same store on the tuned target, or a skip. The SQL
// here rebinds placeholders and upserts, and neither is proven by SQLite: the
// four tables that once reached SQLite and silently never reached Postgres are
// why every store in this package is held to both dialects.
func postgresUpstreams(t *testing.T, prefix string, insecure ...string) plugins.Upstreams {
	t.Helper()
	dsn := os.Getenv("DHOLE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DHOLE_TEST_POSTGRES_DSN not set: this test needs a live Postgres")
	}
	db, err := runstore.OpenPostgres(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return upstreamsOver(t, db, runstore.DialectPostgres, prefix, insecure...)
}

// mirrorPrefix is a repository prefix nothing else writes to, so a run's
// assertions never rest on another run's tags in a registry that outlives it.
func mirrorPrefix(t *testing.T, host string) string {
	t.Helper()
	return fmt.Sprintf("%s/%s", host, uniqueRepo(t))
}

func upstreamAt(host, namespace string, allowed []string, policy plugins.MirrorPolicy) plugins.Upstream {
	return plugins.Upstream{
		Namespace:         namespace,
		URL:               "oci://" + host + "/plugins",
		AllowedIdentities: allowed,
		MirrorPolicy:      policy,
	}
}

// TestNamespacingPreventsCollisionBetweenUpstreams is why a federated upstream
// is registered under a namespace rather than merged into one flat name space.
//
// Two organisations both publish `docker-build`. Without a namespace the second
// registration silently shadows the first, and every pipeline referring to
// `docker-build` starts running somebody else's code the day a second
// federation is added — with no error anywhere.
func TestNamespacingPreventsCollisionBetweenUpstreams(t *testing.T) {
	local := registryHost(t)
	ctx := context.Background()

	hostA, _ := startUpstreamRegistry(t)
	hostB, _ := startUpstreamRegistry(t)

	payloadA := []byte("docker-build, as federation a builds it")
	payloadB := []byte("docker-build, as federation b builds it")
	pushSignedImage(t, hostA, "plugins/docker-build", "latest", payloadA, identityFedA, testIssuer)
	pushSignedImage(t, hostB, "plugins/docker-build", "latest", payloadB, identityFedB, testIssuer)

	tenant := uniqueTenant(t)
	ups := newUpstreams(t, mirrorPrefix(t, local), hostA, hostB, local)

	require.NoError(t, ups.Add(ctx, tenant,
		upstreamAt(hostA, "a", allowOnly(testIssuer, identityFedA), plugins.MirrorAlways)))
	require.NoError(t, ups.Add(ctx, tenant,
		upstreamAt(hostB, "b", allowOnly(testIssuer, identityFedB), plugins.MirrorAlways)))

	nA, err := ups.Sync(ctx, tenant, "a")
	require.NoError(t, err)
	require.Equal(t, 1, nA)
	nB, err := ups.Sync(ctx, tenant, "b")
	require.NoError(t, err)
	require.Equal(t, 1, nB)

	artA, err := ups.Resolve(ctx, tenant, "a/docker-build")
	require.NoError(t, err)
	artB, err := ups.Resolve(ctx, tenant, "b/docker-build")
	require.NoError(t, err)
	require.NotEqual(t, artA.Digest.GetHex(), artB.Digest.GetHex(),
		"two upstreams' docker-build are different artifacts and must resolve to different digests")

	rcA, err := ups.Dispatch(ctx, tenant, "a/docker-build")
	require.NoError(t, err)
	require.Equal(t, payloadA, readAll(t, rcA))

	rcB, err := ups.Dispatch(ctx, tenant, "b/docker-build")
	require.NoError(t, err)
	require.Equal(t, payloadB, readAll(t, rcB),
		"b/docker-build must serve b's bytes, not whichever upstream was synced last")

	// An unqualified name is not a plugin reference here: it names one of two
	// different artifacts, and picking either would be a guess.
	_, err = ups.Resolve(ctx, tenant, "docker-build")
	require.Error(t, err)
}

// TestSyncMirrorsArtifactsLocallyAndSurvivesUpstreamOutage is the whole reason
// mirroring exists.
//
// An upstream consulted at dispatch time is both an availability dependency and
// a supply-chain one. Mirroring moves both to sync time, where a human is
// watching. The upstream registry here is genuinely stopped — its socket is
// closed and the test proves it refuses connections — so a resolve or a fetch
// that reached for it would fail rather than quietly pass.
func TestSyncMirrorsArtifactsLocallyAndSurvivesUpstreamOutage(t *testing.T) {
	local := registryHost(t)
	ctx := context.Background()

	hostA, stopA := startUpstreamRegistry(t)
	payload := []byte("the plugin bytes the mirror must keep serving")
	pushSignedImage(t, hostA, "plugins/docker-build", "latest", payload, identityFedA, testIssuer)

	tenant := uniqueTenant(t)
	ups := newUpstreams(t, mirrorPrefix(t, local), hostA, local)
	require.NoError(t, ups.Add(ctx, tenant,
		upstreamAt(hostA, "a", allowOnly(testIssuer, identityFedA), plugins.MirrorAlways)))

	n, err := ups.Sync(ctx, tenant, "a")
	require.NoError(t, err)
	require.Equal(t, 1, n, "the count is what this sync newly mirrored")

	// Sync is safe to run twice: nothing upstream changed, so nothing is newly
	// mirrored and the count says so.
	again, err := ups.Sync(ctx, tenant, "a")
	require.NoError(t, err)
	require.Equal(t, 0, again, "a second sync of an unchanged upstream mirrors nothing new")

	stopA()
	requireUnreachable(t, hostA)

	art, err := ups.Resolve(ctx, tenant, "a/docker-build")
	require.NoError(t, err, "resolve must not need the upstream once the artifact is mirrored")
	require.Contains(t, art.Ref, local, "the resolved reference must name the local mirror")
	require.NotContains(t, art.Ref, hostA, "a reference naming the upstream is an availability dependency")

	rc, err := ups.Dispatch(ctx, tenant, "a/docker-build")
	require.NoError(t, err, "dispatch must be served entirely from the mirror")
	require.Equal(t, payload, readAll(t, rc))
}

// TestUnmirroredPluginBlocksDispatchWithClearDiagnostic. The refusal is not the
// interesting part — what it SAYS is. An operator reading this at 3am must not
// have to guess which of a dozen upstreams is down, so the error names both the
// plugin and the upstream it would have come from.
func TestUnmirroredPluginBlocksDispatchWithClearDiagnostic(t *testing.T) {
	local := registryHost(t)
	ctx := context.Background()

	hostB, stopB := startUpstreamRegistry(t)
	pushSignedImage(t, hostB, "plugins/docker-build", "latest",
		[]byte("published upstream, never mirrored"), identityFedB, testIssuer)

	tenant := uniqueTenant(t)
	ups := newUpstreams(t, mirrorPrefix(t, local), hostB, local)
	// on_demand: the policy that is ALLOWED to pull at save time. Even so,
	// dispatch must never pull — that is the property under test.
	require.NoError(t, ups.Add(ctx, tenant,
		upstreamAt(hostB, "b", allowOnly(testIssuer, identityFedB), plugins.MirrorOnDemand)))

	stopB()
	requireUnreachable(t, hostB)

	_, err := ups.Dispatch(ctx, tenant, "b/docker-build")
	require.ErrorIs(t, err, plugins.ErrNotMirrored)
	require.Contains(t, err.Error(), "b/docker-build", "the diagnostic must name the plugin")
	require.Contains(t, err.Error(), hostB, "the diagnostic must name the upstream that is down")

	// Save-time resolution may reach for the upstream under on_demand, and when
	// that fails it owes the operator the same two facts.
	_, err = ups.Resolve(ctx, tenant, "b/docker-build")
	require.Error(t, err)
	require.Contains(t, err.Error(), "b/docker-build")
	require.Contains(t, err.Error(), hostB)
}

// TestPerUpstreamAllowedIdentitiesAreEnforced. Trust is per-upstream: a signer
// this tenant vouched for inside federation `a` is not thereby vouched for
// inside federation `b`. Pooling the two lists would mean adding one upstream
// silently widened the trust of every other.
func TestPerUpstreamAllowedIdentitiesAreEnforced(t *testing.T) {
	local := registryHost(t)
	ctx := context.Background()

	hostA, _ := startUpstreamRegistry(t)
	hostB, _ := startUpstreamRegistry(t)

	// Both artifacts are signed by federation a's release bot. Different bytes,
	// so the two are genuinely different artifacts and nothing here passes by
	// sharing a digest with something already mirrored.
	pushSignedImage(t, hostA, "plugins/docker-build", "latest",
		[]byte("a's docker-build"), identityFedA, testIssuer)
	pushSignedImage(t, hostB, "plugins/docker-build", "latest",
		[]byte("b's docker-build, signed by a's bot"), identityFedA, testIssuer)

	tenant := uniqueTenant(t)
	ups := newUpstreams(t, mirrorPrefix(t, local), hostA, hostB, local)
	require.NoError(t, ups.Add(ctx, tenant,
		upstreamAt(hostA, "a", allowOnly(testIssuer, identityFedA), plugins.MirrorAlways)))
	require.NoError(t, ups.Add(ctx, tenant,
		upstreamAt(hostB, "b", allowOnly(testIssuer, identityFedB), plugins.MirrorAlways)))

	n, err := ups.Sync(ctx, tenant, "a")
	require.NoError(t, err)
	require.Equal(t, 1, n)

	n, err = ups.Sync(ctx, tenant, "b")
	require.ErrorIs(t, err, plugins.ErrVerificationFailed,
		"b does not vouch for a's release bot, so b's copy must not be mirrored")
	require.Contains(t, err.Error(), "b/docker-build")
	require.Contains(t, err.Error(), hostB)
	require.Equal(t, 0, n, "a refused artifact is not counted as mirrored")

	_, err = ups.Dispatch(ctx, tenant, "b/docker-build")
	require.ErrorIs(t, err, plugins.ErrNotMirrored)

	// And a, whose signer it does allow, is unaffected.
	rc, err := ups.Dispatch(ctx, tenant, "a/docker-build")
	require.NoError(t, err)
	require.Equal(t, []byte("a's docker-build"), readAll(t, rc))
}

// TestUpstreamRegistrationIsScopedAndDeliberate covers the registration rules
// that need no registry: every call carries a tenant, a namespace is claimed
// once, and a namespace that would shadow local plugins is refused.
func TestUpstreamRegistrationIsScopedAndDeliberate(t *testing.T) {
	registrationContract(t, newUpstreams(t, "registry.invalid/mirror"))
}

// TestPostgresUpstreamRegistrationIsScopedAndDeliberate holds the same
// contract on the tuned target, where the placeholders and the upsert differ.
func TestPostgresUpstreamRegistrationIsScopedAndDeliberate(t *testing.T) {
	registrationContract(t, postgresUpstreams(t, "registry.invalid/mirror"))
}

func registrationContract(t *testing.T, ups plugins.Upstreams) {
	t.Helper()
	ctx := context.Background()
	u := upstreamAt("registry.invalid", "a", allowOnly(testIssuer, identityFedA), plugins.MirrorAlways)

	t.Run("AnUnscopedCallIsRefused", func(t *testing.T) {
		require.ErrorIs(t, ups.Add(ctx, "", u), plugins.ErrTenantRequired)
		require.ErrorContains(t, ups.Add(ctx, "", u), "tenant scope required")
		_, err := ups.Sync(ctx, "", "a")
		require.ErrorIs(t, err, plugins.ErrTenantRequired)
		_, err = ups.Resolve(ctx, "", "a/docker-build")
		require.ErrorIs(t, err, plugins.ErrTenantRequired)
	})

	t.Run("ANamespaceIsClaimedOnce", func(t *testing.T) {
		tenant := uniqueTenant(t)
		require.NoError(t, ups.Add(ctx, tenant, u))
		// Re-registering the identical upstream is a no-op: registration is
		// retried by deploy tooling and must not fail on a second apply.
		require.NoError(t, ups.Add(ctx, tenant, u))

		moved := u
		moved.URL = "oci://elsewhere.invalid/plugins"
		require.ErrorIs(t, ups.Add(ctx, tenant, moved), plugins.ErrNamespaceExists,
			"repointing a namespace at different bytes must be deliberate, not a silent overwrite")

		widened := u
		widened.AllowedIdentities = append(allowOnly(testIssuer, identityFedA),
			testIssuer+" "+identityFedB)
		require.ErrorIs(t, ups.Add(ctx, tenant, widened), plugins.ErrNamespaceExists,
			"widening who a namespace trusts must not happen by re-applying a config")
	})

	t.Run("ANamespaceIsScopedToItsTenant", func(t *testing.T) {
		mine, theirs := uniqueTenant(t), uniqueTenant(t)
		require.NoError(t, ups.Add(ctx, mine, u))
		// Another tenant's registration of the same namespace is unrelated, and
		// mine is invisible to them until they make it themselves.
		_, err := ups.Sync(ctx, theirs, "a")
		require.ErrorIs(t, err, plugins.ErrUnknownUpstream)
		require.NoError(t, ups.Add(ctx, theirs, u))
	})

	t.Run("AReservedNamespaceIsRefused", func(t *testing.T) {
		tenant := uniqueTenant(t)
		shadow := u
		shadow.Namespace = "local"
		err := ups.Add(ctx, tenant, shadow)
		require.ErrorIs(t, err, plugins.ErrReservedNamespace)
		require.Contains(t, err.Error(), "local",
			"a federated upstream must not be able to shadow the tenant's own plugins")
	})

	t.Run("AMalformedRegistrationIsRefused", func(t *testing.T) {
		tenant := uniqueTenant(t)
		for name, bad := range map[string]func(plugins.Upstream) plugins.Upstream{
			"no namespace":   func(x plugins.Upstream) plugins.Upstream { x.Namespace = ""; return x },
			"namespace path": func(x plugins.Upstream) plugins.Upstream { x.Namespace = "a/b"; return x },
			"uppercase":      func(x plugins.Upstream) plugins.Upstream { x.Namespace = "Prod"; return x },
			"no url":         func(x plugins.Upstream) plugins.Upstream { x.URL = ""; return x },
			"cas url":        func(x plugins.Upstream) plugins.Upstream { x.URL = "cas://sha256:abc"; return x },
			"no policy":      func(x plugins.Upstream) plugins.Upstream { x.MirrorPolicy = ""; return x },
			"no identities": func(x plugins.Upstream) plugins.Upstream {
				x.AllowedIdentities = nil
				return x
			},
		} {
			t.Run(name, func(t *testing.T) {
				require.Error(t, ups.Add(ctx, tenant, bad(u)))
			})
		}
	})
}

// TestPostgresMirrorsAndDispatchesOnTheTunedTarget runs one whole federation —
// register, sync, re-sync, dispatch — against Postgres, because the mirror
// index is written with an upsert and read with rebound placeholders, and
// neither is exercised by the SQLite path.
func TestPostgresMirrorsAndDispatchesOnTheTunedTarget(t *testing.T) {
	local := registryHost(t)
	ctx := context.Background()

	hostA, _ := startUpstreamRegistry(t)
	payload := []byte("mirrored through postgres")
	pushSignedImage(t, hostA, "plugins/docker-build", "latest", payload, identityFedA, testIssuer)

	tenant := uniqueTenant(t)
	ups := postgresUpstreams(t, mirrorPrefix(t, local), hostA, local)
	require.NoError(t, ups.Add(ctx, tenant,
		upstreamAt(hostA, "a", allowOnly(testIssuer, identityFedA), plugins.MirrorAlways)))

	n, err := ups.Sync(ctx, tenant, "a")
	require.NoError(t, err)
	require.Equal(t, 1, n)

	again, err := ups.Sync(ctx, tenant, "a")
	require.NoError(t, err)
	require.Equal(t, 0, again, "the upsert must update the coordinate in place, not re-mirror it")

	rc, err := ups.Dispatch(ctx, tenant, "a/docker-build")
	require.NoError(t, err)
	require.Equal(t, payload, readAll(t, rc))
}

// TestARevokedSignatureStopsMirroredDispatchImmediately. Mirroring vouches for
// an artifact at SYNC time, and it is tempting to read a mirror row as standing
// proof of trust — the bytes are here, somebody checked them once. They are not.
// A signature withdrawn after a key compromise must stop the artifact
// dispatching now, not at the next sync, so trust is re-checked on every
// dispatch and never remembered.
func TestARevokedSignatureStopsMirroredDispatchImmediately(t *testing.T) {
	local := registryHost(t)
	ctx := context.Background()

	hostA, _ := startUpstreamRegistry(t)
	d := pushSignedImage(t, hostA, "plugins/docker-build", "latest",
		[]byte("signed today, revoked tomorrow"), identityFedA, testIssuer)

	tenant := uniqueTenant(t)
	ups, sigs := newUpstreamsAndSignatures(t, mirrorPrefix(t, local), hostA, local)
	require.NoError(t, ups.Add(ctx, tenant,
		upstreamAt(hostA, "a", allowOnly(testIssuer, identityFedA), plugins.MirrorAlways)))

	n, err := ups.Sync(ctx, tenant, "a")
	require.NoError(t, err)
	require.Equal(t, 1, n)

	rc, err := ups.Dispatch(ctx, tenant, "a/docker-build")
	require.NoError(t, err)
	require.NoError(t, rc.Close())

	require.NoError(t, sigs.Remove(ctx, tenant, d))

	_, err = ups.Dispatch(ctx, tenant, "a/docker-build")
	require.ErrorIs(t, err, plugins.ErrVerificationFailed,
		"a mirrored artifact whose signature was withdrawn must stop dispatching")
	require.Contains(t, err.Error(), "a/docker-build")
	require.Contains(t, err.Error(), hostA)
}
