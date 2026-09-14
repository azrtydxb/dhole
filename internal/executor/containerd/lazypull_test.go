package containerd_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	client "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/containerd"
)

// captureLog returns a logger writing into buf, so a test can assert that the
// fallback is ANNOUNCED. A silent fallback is the failure mode this whole
// mechanism has: the image pull quietly costs what it always did, and nobody
// looking at a slow pipeline has anything to read.
func captureLog() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

func TestLazyPullUsesStargzWhenTheSnapshotterIsPresent(t *testing.T) {
	log, buf := captureLog()
	mode := containerd.SelectPullMode(t.Context(), func(context.Context) ([]string, error) {
		return []string{"overlayfs", "native", containerd.StargzSnapshotter}, nil
	}, log)

	require.Equal(t, containerd.StargzSnapshotter, mode.Snapshotter)
	require.True(t, mode.Lazy, "with the stargz snapshotter present the pull is lazy")
	require.NotContains(t, buf.String(), "level=WARN", "nothing to warn about")
}

func TestLazyPullFallsBackToAFullPullWithALoggedWarning(t *testing.T) {
	log, buf := captureLog()
	mode := containerd.SelectPullMode(t.Context(), func(context.Context) ([]string, error) {
		return []string{"overlayfs", "native"}, nil
	}, log)

	require.Equal(t, containerd.DefaultSnapshotter, mode.Snapshotter)
	require.False(t, mode.Lazy)
	require.Contains(t, buf.String(), "level=WARN")
	require.Contains(t, buf.String(), "falling back to a full image pull")
	require.Contains(t, buf.String(), containerd.StargzSnapshotter,
		"the warning must name what is missing, or it cannot be acted on")
}

func TestLazyPullFallsBackWhenSnapshottersCannotBeListed(t *testing.T) {
	log, buf := captureLog()
	mode := containerd.SelectPullMode(t.Context(), func(context.Context) ([]string, error) {
		return nil, errors.New("containerd is not reachable")
	}, log)

	require.Equal(t, containerd.DefaultSnapshotter, mode.Snapshotter)
	require.False(t, mode.Lazy, "an unknown daemon is not assumed to be lazy-capable")
	require.Contains(t, buf.String(), "level=WARN")
	require.Contains(t, buf.String(), "falling back to a full image pull")
	require.Contains(t, buf.String(), "containerd is not reachable")
}

// TestLazyPullFetchesFewerBytesThanFullImage is the property that actually
// matters: with stargz on, starting a sandbox on a 500MB image and running
// `true` in it must pull far less than 500MB.
//
// Nothing here is simulated. The daemon is a real containerd with a real
// stargz snapshotter, the image is a real eStargz image in a real registry,
// and the bytes are counted on the wire between the two: the test sits a
// counting reverse proxy in front of the registry and hands the executor the
// PROXY's address, so every byte either containerd or the snapshotter fetches
// passes through it. A byte count asserted against a mock registry would prove
// the mock, which is why this test refuses to run without the real thing.
//
// A counter that counted nothing would pass the lazy half on its own, so the
// same image is then pulled whole through the same proxy and that count must
// cover every layer. Only a proxy that measured the full pull correctly is
// believed about the lazy one.
func TestLazyPullFetchesFewerBytesThanFullImage(t *testing.T) {
	sock := os.Getenv("DHOLE_TEST_CONTAINERD_SOCK")
	if sock == "" {
		t.Skip("needs a containerd socket in DHOLE_TEST_CONTAINERD_SOCK with a WORKING stargz " +
			"snapshotter, plus an eStargz image in DHOLE_TEST_ESTARGZ_IMAGE large enough for the " +
			"byte count to mean something; a mock registry would prove nothing about how many " +
			"bytes a real pull reads (docs/executors/containerd.md sets both up)")
	}
	image := os.Getenv("DHOLE_TEST_ESTARGZ_IMAGE")
	if image == "" {
		t.Skip("DHOLE_TEST_CONTAINERD_SOCK is set but DHOLE_TEST_ESTARGZ_IMAGE is not: name an " +
			"eStargz image of at least 256MiB of layers (hack/lazy-pull/build-fixture.sh builds one)")
	}

	proxy := newCountingRegistryProxy(t, image)
	layers := estargzLayerBytes(t, proxy.ref)
	require.GreaterOrEqual(t, layers, int64(minLazyFixtureBytes),
		"%s has only %d bytes of layers; a byte count on an image that small cannot tell a lazy pull from a full one",
		image, layers)

	// The lazy half, with the executor left to choose: it must pick stargz on
	// its own, and must not demote it on the way.
	log, buf := captureLog()
	lazy := proxy.measure(t, sock, "dhole-lazypull-lazy", containerd.Config{Logger: log})
	require.Contains(t, buf.String(), "lazy image pull enabled",
		"the executor did not choose the stargz snapshotter; log:\n%s", buf.String())
	require.NotContains(t, buf.String(), "level=WARN",
		"the executor fell back to a full pull, so there is no lazy pull to measure; log:\n%s", buf.String())

	// The control: the same image, pulled whole, through the same counter.
	full := proxy.measure(t, sock, "dhole-lazypull-full", containerd.Config{Snapshotter: containerd.DefaultSnapshotter})
	t.Logf("layers %d bytes; lazy pull fetched %d blob bytes; full pull fetched %d blob bytes", layers, lazy, full)

	require.GreaterOrEqual(t, full, layers,
		"the full pull fetched %d bytes of %d: either the counter misses traffic or the daemon already held "+
			"the layers, and either way it cannot be trusted to measure the lazy pull", full, layers)
	require.Less(t, lazy, layers/lazyFractionDenominator,
		"a lazy pull running `true` fetched %d of %d layer bytes; that is not lazy", lazy, layers)
}

const (
	// minLazyFixtureBytes is the smallest image the byte comparison means
	// anything on. The TOCs and the shell a sandbox starts are a few MB.
	minLazyFixtureBytes = 256 << 20
	// lazyFractionDenominator: a lazy start must fetch under a tenth of the
	// image. The real figure is a small fraction of that; a tenth is the line
	// past which "lazy" no longer describes what happened.
	lazyFractionDenominator = 10
)

// countingRegistryProxy forwards to a real registry and counts the blob bytes
// it hands back.
type countingRegistryProxy struct {
	ref       string // the image, addressed through the proxy
	host      string // the proxy's host:port
	blobBytes atomic.Int64
}

func newCountingRegistryProxy(t *testing.T, image string) *countingRegistryProxy {
	t.Helper()
	upstream, err := name.ParseReference(image)
	require.NoError(t, err)
	registry := upstream.Context().RegistryStr()
	scheme := "https"
	if h, _, err := net.SplitHostPort(registry); err == nil {
		if ip := net.ParseIP(h); (ip != nil && ip.IsLoopback()) || h == "localhost" {
			scheme = "http"
		}
	}
	target := &url.URL{Scheme: scheme, Host: registry}

	p := &countingRegistryProxy{}
	rp := httputil.NewSingleHostReverseProxy(target)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/blobs/") {
			w = &countingWriter{ResponseWriter: w, n: &p.blobBytes}
		}
		r.Host = target.Host
		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	p.host = strings.TrimPrefix(srv.URL, "http://")

	sep := ":"
	if _, ok := upstream.(name.Digest); ok {
		sep = "@"
	}
	p.ref = p.host + "/" + upstream.Context().RepositoryStr() + sep + upstream.Identifier()
	return p
}

// measure starts a sandbox on the image in a containerd namespace of its own,
// runs `true`, and returns the blob bytes fetched to get that far. The
// namespace's images are deleted before, and again as soon as the sandbox is
// released, synchronously: containerd shares content between namespaces, so a
// layer one half pulled and left behind is a layer the other half's pull
// silently skips.
func (p *countingRegistryProxy) measure(t *testing.T, sock, ns string, cfg containerd.Config) int64 {
	t.Helper()
	purgeImages(t, sock, ns)
	t.Cleanup(func() { purgeImages(t, sock, ns) })

	cfg.Address, cfg.Namespace, cfg.Image = sock, ns, p.ref
	cfg.InsecureRegistries = []string{p.host}
	e, err := containerd.New(cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	p.blobBytes.Store(0)
	sb, err := e.Acquire(ctx, executor.Spec{})
	require.NoError(t, err)
	code, err := sb.Exec(ctx, executor.Cmd{Args: []string{"true"}})
	fetched := p.blobBytes.Load()
	require.NoError(t, errors.Join(err, sb.Release(context.WithoutCancel(ctx))))
	require.Equal(t, int32(0), code)
	purgeImages(t, sock, ns)
	return fetched
}

// estargzLayerBytes is the total compressed size of the image's layers, and
// refuses an image whose layers are not eStargz: a plain gzip image pulled
// "lazily" is a full pull, and the failure would read as a broken snapshotter.
func estargzLayerBytes(t *testing.T, ref string) int64 {
	t.Helper()
	r, err := name.ParseReference(ref, name.Insecure)
	require.NoError(t, err)
	img, err := remote.Image(r)
	require.NoError(t, err)
	m, err := img.Manifest()
	require.NoError(t, err)
	var total int64
	for _, l := range m.Layers {
		require.Contains(t, l.Annotations, "containerd.io/snapshot/stargz/toc.digest",
			"layer %s of %s is not eStargz", l.Digest, ref)
		total += l.Size
	}
	return total
}

func purgeImages(t *testing.T, sock, ns string) {
	t.Helper()
	c, err := client.New(sock, client.WithDefaultNamespace(ns))
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	ctx := namespaces.WithNamespace(context.WithoutCancel(t.Context()), ns)
	imgs, err := c.ImageService().List(ctx)
	require.NoError(t, err)
	for _, img := range imgs {
		require.NoError(t, c.ImageService().Delete(ctx, img.Name, images.SynchronousDelete()))
	}
}

type countingWriter struct {
	http.ResponseWriter
	n *atomic.Int64
}

func (w *countingWriter) Write(b []byte) (int, error) {
	n, err := w.ResponseWriter.Write(b)
	w.n.Add(int64(n))
	return n, err
}

func (w *countingWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
