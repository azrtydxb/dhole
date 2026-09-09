package containerd_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

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
// matters: with stargz on, running `true` in a 500MB image must pull far less
// than 500MB, measured from the registry's own request log.
//
// It cannot run here, and it is deliberately NOT written against a fake
// registry: a byte count asserted against a mock proves the mock, not the
// pull. Running it needs three things this machine does not have — a reachable
// containerd (Task 36 is recorded as blocked for exactly that reason: no
// Docker daemon, and k3s's containerd is reachable only from a privileged pod
// on a node), the stargz snapshotter plugged into that containerd, and Task
// 36's containerd.New to drive the pull through it. A real registry IS
// available at DHOLE_TEST_OCI_REGISTRY, so the missing half is the runtime.
func TestLazyPullFetchesFewerBytesThanFullImage(t *testing.T) {
	if os.Getenv("DHOLE_TEST_CONTAINERD_SOCK") == "" {
		t.Skip("needs a containerd socket in DHOLE_TEST_CONTAINERD_SOCK with the stargz " +
			"snapshotter plugged in, plus Task 36's containerd executor to drive the pull; " +
			"a mock registry would prove nothing about how many bytes a real pull reads")
	}
	t.Skip("containerd executor (Task 36) is not implemented yet, so there is nothing to " +
		"pull the image with; unskip together with Task 36")
}
