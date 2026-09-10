package containerd_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/executor"
	ctrdexec "github.com/azrtydxb/dhole/internal/executor/containerd"
	"github.com/azrtydxb/dhole/internal/executor/executortest"
)

// executorContract is the conformance suite every executor backend must pass.
func executorContract(t *testing.T, e executor.Executor) {
	t.Helper()
	executortest.Contract(t, e)
}

// socket returns the path of a live containerd socket, or skips naming exactly
// what is missing. There is deliberately no fake client behind this: a mock
// containerd never pulls an image, never starts a shim and never reaps a
// process, so nothing these tests assert about exec, signal propagation or
// exit codes would be real. The Kubernetes backend found two defects — an SPDY
// stream that hung forever and a probe that added five seconds to every
// signalled exec — that a fake would have hidden.
func socket(t *testing.T) string {
	t.Helper()
	sock := os.Getenv("DHOLE_TEST_CONTAINERD_SOCK")
	if sock == "" {
		t.Skip("DHOLE_TEST_CONTAINERD_SOCK is unset: point it at a live containerd socket " +
			"(/run/containerd/containerd.sock, or /run/k3s/containerd/containerd.sock on k3s) " +
			"to run the containerd executor tests; the process running them needs read/write " +
			"access to that socket and to the FIFO directory containerd creates for exec streams")
	}
	return sock
}

func newExecutor(t *testing.T, mutate func(*ctrdexec.Config)) executor.Executor {
	t.Helper()
	cfg := ctrdexec.Config{Address: socket(t), Namespace: "dhole-exec-test"}
	if mutate != nil {
		mutate(&cfg)
	}
	e, err := ctrdexec.New(cfg)
	require.NoError(t, err)
	return e
}

// TestContainerdExecutorContract holds the containerd engine to exactly the
// contract the process and Kubernetes engines pass. ADR 0006's claim is that
// the executor interface was not shaped around any one backend; a third
// backend passing the same suite unchanged is where that claim is tested.
func TestContainerdExecutorContract(t *testing.T) {
	executorContract(t, newExecutor(t, nil))
}

// pushImage publishes a single-layer image carrying payload at repo:tag and
// returns its manifest digest, the same way internal/plugins' tests do: the
// image is built in-process, so no container runtime is needed to produce
// content a registry will serve.
func pushImage(t *testing.T, ref name.Tag, payload []byte) v1.Hash {
	t.Helper()
	img, err := mutate.AppendLayers(empty.Image, static.NewLayer(payload, types.OCILayer))
	require.NoError(t, err)
	img = mutate.MediaType(img, types.OCIManifestSchema1)
	img = mutate.ConfigMediaType(img, types.OCIConfigJSON)
	require.NoError(t, remote.Write(ref, img))
	h, err := img.Digest()
	require.NoError(t, err)
	return h
}

// TestEnvironmentIdentityIsImageDigestNotTag. A tag is not an identity: the
// same tag serves different content over time, so a cache keyed on one returns
// yesterday's answer for today's image (ADR 0021 — the identity must be stable
// for identical environments, different for different ones, and absent rather
// than invented).
//
// The registry here is go-containerregistry's own implementation running in
// this process, not a stub of ours: the manifest is pushed and the digest is
// read back over HTTP. That is the whole of what this test exercises —
// resolution — so it needs no containerd, and it runs everywhere the rest of
// the suite skips.
func TestEnvironmentIdentityIsImageDigestNotTag(t *testing.T) {
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "http://")

	ref, err := name.NewTag(fmt.Sprintf("%s/dhole-test/env:latest", host), name.Insecure)
	require.NoError(t, err)
	first := pushImage(t, ref, []byte("the environment as it was"))

	identityOf := func() string {
		e, err := ctrdexec.New(ctrdexec.Config{
			Address:            "/nonexistent/containerd.sock",
			Image:              ref.Name(),
			InsecureRegistries: []string{host},
		})
		require.NoError(t, err)
		id, err := e.EnvironmentIdentity()
		require.NoError(t, err)
		return id
	}

	before := identityOf()
	require.Contains(t, before, "@sha256:", "the identity must be a content digest")
	require.NotContains(t, before, ":latest", "a mutable tag must not appear in a cache key")
	require.Contains(t, before, first.String())

	// The same tag, different bytes: exactly the case a tag-keyed cache gets
	// wrong, and the reason ADR 0021 asks engines for a digest.
	second := pushImage(t, ref, []byte("the environment as it is now"))
	require.NotEqual(t, first, second, "the fixture must actually have changed the image")
	require.NotEqual(t, before, identityOf(),
		"the identity did not change when the content under the tag did: it is keyed on the tag")
}

// TestEnvironmentIdentityWithoutAResolvableImageIsNoStableIdentity. Honesty in
// the other direction: an identity that cannot be resolved is absent, never
// invented, and internal/cache then declines to cache rather than caching
// against a key that quietly means nothing.
func TestEnvironmentIdentityWithoutAResolvableImageIsNoStableIdentity(t *testing.T) {
	e, err := ctrdexec.New(ctrdexec.Config{
		Address: "/nonexistent/containerd.sock",
		Image:   "dhole.invalid/nothing/here:v1",
	})
	require.NoError(t, err)
	id, err := e.EnvironmentIdentity()
	require.Empty(t, id)
	require.ErrorIs(t, err, executor.ErrNoStableIdentity)
}

// TestPrivilegedIsRefusedUnlessCapabilityAdvertised. A backend that accepts
// work whose isolation requirements it cannot meet runs that work with the
// wrong isolation — the failure is silent, and the step believes it got what
// it asked for.
//
// The socket here does not exist, on purpose: the refusal must come from the
// capability check and not from a daemon that happened to be unreachable, so
// the assertion is that Acquire says "capability not advertised" rather than
// anything about containerd.
func TestPrivilegedIsRefusedUnlessCapabilityAdvertised(t *testing.T) {
	e, err := ctrdexec.New(ctrdexec.Config{Address: "/nonexistent/containerd.sock"})
	require.NoError(t, err)
	require.NotContains(t, e.Capabilities(), dholev1.Capability_CAPABILITY_PRIVILEGED,
		"a default containerd sandbox is unprivileged, and must not claim otherwise")

	sb, err := e.Acquire(t.Context(), executor.Spec{
		Requirements: executor.Requirements{
			Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED},
		},
	})
	require.Nil(t, sb)
	require.Error(t, err)
	require.Contains(t, err.Error(), "capability not advertised")
}

// TestRootlessByDefault. A step that gets root in its sandbox gets root over
// every host resource that sandbox is given, and nothing in a passing build
// would show it. Privilege is granted only where it was configured and
// advertised, so the default has to be provably unprivileged.
func TestRootlessByDefault(t *testing.T) {
	var out strings.Builder
	sb, err := newExecutor(t, nil).Acquire(t.Context(), executor.Spec{Lease: executor.LeaseStep})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sb.Release(context.WithoutCancel(t.Context()))) })

	code, err := sb.Exec(t.Context(), executor.Cmd{Args: []string{"sh", "-c", "id -u"}, Stdout: &out})
	require.NoError(t, err)
	require.Equal(t, int32(0), code)
	require.NotEqual(t, "0", strings.TrimSpace(out.String()),
		"the default sandbox ran as root: it must run as an unprivileged uid unless PRIVILEGED was granted")
}

// TestPrivilegedGrantsRootWhenItIsAdvertised is the other half: a backend
// configured for privilege must actually deliver it, or a step that legitimately
// needs it fails in a way that looks like its own bug.
func TestPrivilegedGrantsRootWhenItIsAdvertised(t *testing.T) {
	e := newExecutor(t, func(c *ctrdexec.Config) { c.Privileged = true })
	require.Contains(t, e.Capabilities(), dholev1.Capability_CAPABILITY_PRIVILEGED)

	sb, err := e.Acquire(t.Context(), executor.Spec{
		Lease: executor.LeaseStep,
		Requirements: executor.Requirements{
			Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sb.Release(context.WithoutCancel(t.Context()))) })

	var out strings.Builder
	code, err := sb.Exec(t.Context(), executor.Cmd{Args: []string{"sh", "-c", "id -u"}, Stdout: &out})
	require.NoError(t, err)
	require.Equal(t, int32(0), code)
	require.Equal(t, "0", strings.TrimSpace(out.String()),
		"an executor advertising PRIVILEGED must actually grant it")
}
