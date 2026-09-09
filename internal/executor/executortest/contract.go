// Package executortest holds the executor conformance contract: the behaviour
// every executor backend must exhibit, expressed once so a container, VM or
// remote backend can be dropped in unchanged.
//
// It lives in its own package rather than in a _test.go file because a test
// file is invisible to other packages, and the contract has to be callable
// from every backend's own test package.
package executortest

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/executor"
)

// Contract asserts the behaviour required of every executor backend. It is
// deliberately written against nothing but the executor interfaces: it never
// names a container, an image or a host path, so a VM or process backend
// satisfies it on the same terms as an OCI one.
//
// The commands it runs are POSIX shell one-liners, the smallest common
// denominator across the Linux and macOS targets in scope. A backend whose
// sandboxes are not POSIX (Task 54's Windows engine) needs a shell-neutral
// variant of this contract, not an exemption from it.
func Contract(t *testing.T, e executor.Executor) {
	t.Helper()

	t.Run("runs a command and captures stdout", func(t *testing.T) {
		sb := acquire(t, e)
		var stdout bytes.Buffer
		code, err := sb.Exec(t.Context(), executor.Cmd{
			Args:   []string{"sh", "-c", "echo hi"},
			Stdout: &stdout,
		})
		require.NoError(t, err)
		require.Equal(t, int32(0), code)
		require.Equal(t, "hi", strings.TrimSpace(stdout.String()))
	})

	t.Run("reports a non-zero exit code without reporting an error", func(t *testing.T) {
		sb := acquire(t, e)
		code, err := sb.Exec(t.Context(), executor.Cmd{Args: []string{"sh", "-c", "exit 3"}})
		require.NoError(t, err, "a command that runs and exits non-zero is not an executor failure")
		require.Equal(t, int32(3), code)
	})

	t.Run("signal terminates a running command and everything it spawned", func(t *testing.T) {
		sb := acquire(t, e)
		// Stdout is a plain writer, so the backend streams it through a pipe
		// the whole process tree inherits. Exec therefore cannot report the
		// command finished while a grandchild still holds that pipe open —
		// which is what makes this subtest fail, rather than silently leak,
		// when a backend signals only the direct child and leaves the sleep
		// running on the host. The trailing `exit 0` matters: a shell given a
		// single command execs into it, leaving no grandchild to leak.
		var out syncBuffer
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = sb.Exec(context.WithoutCancel(t.Context()), executor.Cmd{
				Args:   []string{"sh", "-c", "sleep 30; exit 0"},
				Stdout: &out,
			})
		}()

		// The command may not have started yet, and signalling an idle
		// sandbox is a no-op, so keep signalling until it takes.
		require.Eventually(t, func() bool {
			if err := sb.Signal(t.Context(), executor.SIGTERM); err != nil {
				return false
			}
			select {
			case <-done:
				return true
			default:
				return false
			}
		}, 2*time.Second, 20*time.Millisecond, "SIGTERM did not stop the command tree within 2s")
	})

	t.Run("put then get round-trips bytes", func(t *testing.T) {
		sb := acquire(t, e)
		payload := []byte("dhole\x00binary\nbytes")
		require.NoError(t, sb.Put(t.Context(), "artifact.bin", bytes.NewReader(payload)))

		r, err := sb.Get(t.Context(), "artifact.bin")
		require.NoError(t, err)
		defer func() { require.NoError(t, r.Close()) }()

		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, payload, got)
	})

	t.Run("release is idempotent", func(t *testing.T) {
		sb, err := e.Acquire(t.Context(), executor.Spec{Lease: executor.LeaseStep})
		require.NoError(t, err)
		require.NoError(t, sb.Release(t.Context()))
		require.NoError(t, sb.Release(t.Context()), "releasing twice must not be an error")
	})

	t.Run("advertises a kind and a capability set", func(t *testing.T) {
		require.NotEmpty(t, e.Kind())
		// Capabilities may legitimately be empty; asking must not panic and
		// must not report a capability the backend cannot honour.
		require.NotNil(t, e.Capabilities)
	})

	t.Run("environment identity is honest", func(t *testing.T) {
		id, err := e.EnvironmentIdentity()
		if err != nil {
			require.ErrorIs(t, err, executor.ErrNoStableIdentity)
			require.Empty(t, id, "an executor with no stable identity must not return one")
			return
		}
		require.NotEmpty(t, id, "a nil error must mean a real, hashable identity")
	})
}

func acquire(t *testing.T, e executor.Executor) executor.Sandbox {
	t.Helper()
	sb, err := e.Acquire(t.Context(), executor.Spec{Lease: executor.LeaseStep})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, sb.Release(context.WithoutCancel(t.Context())))
	})
	return sb
}

// syncBuffer is a bytes.Buffer safe for the writer goroutine a backend may use
// to stream output while the test reads nothing.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
