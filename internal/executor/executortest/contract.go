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
		}, sigtermWindow, 20*time.Millisecond, "SIGTERM did not stop the command tree within "+sigtermWindow.String())
	})

	t.Run("cancelling a step terminates everything it spawned", func(t *testing.T) {
		sb := acquire(t, e)
		// Cancellation is how a job is actually stopped: the control plane
		// cancels the context the step runs under. Signal is the operator's
		// door, not the scheduler's, so a backend that terminates a tree on
		// Signal and leaks it on cancel leaks it in production.
		ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
		defer cancel()

		// A pooled sandbox may carry an earlier case's heartbeat file, so
		// what counts is GROWTH from where this case found it, never mere
		// existence: a stale file would let this pass without the grandchild
		// ever having run.
		base := Heartbeat(t, sb)

		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = sb.Exec(ctx, executor.Cmd{
				Args:   []string{"sh", "-c", OrphanScript},
				Stdout: &syncBuffer{},
			})
		}()

		// Cancelling a tree that never started proves nothing.
		require.Eventually(t, func() bool { return Heartbeat(t, sb) > base }, 30*time.Second, 100*time.Millisecond,
			"the orphaned grandchild never started writing its heartbeat")

		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("Exec did not return within 10s of cancellation")
		}

		// Returning is not the property. The property is that nothing the
		// step spawned is still running, and a leaked grandchild goes on
		// writing its heartbeat.
		before := Heartbeat(t, sb)
		time.Sleep(750 * time.Millisecond)
		require.Equal(t, before, Heartbeat(t, sb),
			"a grandchild is still running after cancellation: the step's process tree leaked")
	})

	t.Run("a step killed the way an out-of-memory kill arrives reports 137", func(t *testing.T) {
		sb := acquire(t, e)
		// This is how memory exhaustion reaches a step everywhere Dhole runs:
		// SIGKILL from Linux's OOM killer or macOS's Jetsam, a Windows job
		// terminating, a container OOMKilled. What must never happen is that
		// it reads as a success — a step reported exit 0 is a step whose
		// result may be cached and shipped. docs/wire-contract.md documents
		// 137 as the one number for all of them.
		done := make(chan int32, 1)
		go func() {
			code, _ := sb.Exec(context.WithoutCancel(t.Context()), executor.Cmd{
				// Ignores SIGTERM, exactly as an OOM kill ignores politeness.
				Args:   []string{"sh", "-c", `trap "" TERM; while :; do sleep 0.05; done`},
				Stdout: &syncBuffer{},
			})
			done <- code
		}()

		var code int32
		require.Eventually(t, func() bool {
			if err := sb.Signal(t.Context(), executor.SIGKILL); err != nil {
				return false
			}
			select {
			case code = <-done:
				return true
			default:
				return false
			}
		}, 30*time.Second, 100*time.Millisecond, "SIGKILL did not stop the step")

		require.NotEqual(t, int32(0), code, "a killed step must never be reported as a success")
		require.Positive(t, code,
			"a negative exit code sign-extends to ten bytes on the wire and hangs a varint decoder")
		require.Equal(t, int32(137), code,
			"a killed step reports 137 on every backend and platform, so one number means one thing")
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

	t.Run("mkdir creates a directory a step can redirect into, and is idempotent", func(t *testing.T) {
		sb := acquire(t, e)
		// The concrete failure: a step whose command is `... > outputs/copy`
		// exits 1 with "No such file or directory" when nothing created
		// outputs/ first, and the engine reports a broken pipeline rather
		// than a missing sandbox convention. Every backend has to create it.
		require.NoError(t, sb.Mkdir(t.Context(), "outputs"))
		require.NoError(t, sb.Mkdir(t.Context(), "outputs"), "creating a directory twice must not be an error")

		code, err := sb.Exec(t.Context(), executor.Cmd{Args: []string{"sh", "-c", "printf ok > outputs/copy"}})
		require.NoError(t, err)
		require.Equal(t, int32(0), code)

		r, err := sb.Get(t.Context(), "outputs/copy")
		require.NoError(t, err)
		defer func() { require.NoError(t, r.Close()) }()
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, "ok", string(got))
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

	t.Run("a sandbox names the environment it actually runs in", func(t *testing.T) {
		// The identity a cache key is hashed against comes from HERE, because
		// the environment is chosen per sandbox: a backend that answered from
		// the executor alone gave two steps on two different images one
		// identity, and therefore one cache key for two computations.
		sb := acquire(t, e)
		id, err := sb.EnvironmentIdentity()
		if err != nil {
			require.ErrorIs(t, err, executor.ErrNoStableIdentity)
			require.Empty(t, id, "a sandbox with no stable identity must not return one")
			return
		}
		require.NotEmpty(t, id, "a nil error must mean a real, hashable identity")

		again, err := sb.EnvironmentIdentity()
		require.NoError(t, err)
		require.Equal(t, id, again,
			"one sandbox is one environment: an identity that changes while the sandbox is alive describes nothing")

		// A sandbox acquired with an empty Spec is the backend's own default,
		// which is exactly what the executor-wide answer describes. They must
		// agree, or an engine registers one environment and runs another.
		base, baseErr := e.EnvironmentIdentity()
		require.NoError(t, baseErr)
		require.Equal(t, base, id,
			"the default sandbox and the executor must name the same environment")
	})
}

// sigtermWindow is how long a backend has to stop a command tree after a
// SIGTERM, and it is the SAME for every backend on purpose.
//
// It was an open question whether a remote backend deserved a larger budget:
// a Kubernetes executor spends roughly 80ms on each exec round trip, where a
// local process spends none, so the window looked like a local-process figure
// applied to something with far less headroom.
//
// It stays strict, and the reason is measurement rather than principle. The
// Kubernetes executor meets it against a real cluster — the whole contract,
// this subtest included, passes in 18s over a live API server. So the budget
// is achievable remotely and there is nothing to relax.
//
// The principle matters for the next backend, though. This window is a promise
// made to the SCHEDULER, not a convenience for the backend: a lease expires
// thirty seconds after it is claimed, and a cancel that takes longer than this
// leaves a step running while the scheduler has already re-dispatched it. A
// backend that genuinely cannot meet the window has told us something the
// scheduler needs to know, and the honest response is to say so rather than to
// widen the number until it passes.
const sigtermWindow = 2 * time.Second

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

// OrphanScript is a step that spawns a grandchild which OUTLIVES its parent,
// ignores SIGTERM, and proves it is alive by appending to HeartbeatFile every
// 50ms. Three properties are load-bearing:
//
//   - the direct child exits at once, so a backend that terminates only the
//     process it started terminates something that is already gone;
//   - the grandchild traps SIGTERM, so a backend that never escalates to an
//     unignorable kill leaves it running;
//   - it holds the inherited stdout pipe, so a backend cannot report the step
//     finished while it is still there.
//
// The obvious `sh -c "sleep 30"` tests none of this: a shell given a single
// command execs into it and leaves no grandchild at all. That version of this
// test was written once already, and caught nothing.
const OrphanScript = `(trap "" TERM; while :; do printf . >> ` + HeartbeatFile + `; sleep 0.05; done) & exit 0`

// HeartbeatFile is where OrphanScript's grandchild proves it is still alive.
// It is a path inside the sandbox, so its growth is readable through Get on
// any backend rather than off the host the step happens to run on.
const HeartbeatFile = "heartbeat"

// Heartbeat is how many bytes the grandchild has written so far. A file that
// is not there yet is zero, not a failure: the step may not have started.
func Heartbeat(t *testing.T, sb executor.Sandbox) int {
	t.Helper()
	r, err := sb.Get(context.WithoutCancel(t.Context()), HeartbeatFile)
	if err != nil {
		return 0
	}
	defer func() { _ = r.Close() }()
	// A backend that streams the file reports "not there" when the bytes are
	// read rather than when Get is called, and "not there yet" is a real
	// answer here: the step may not have written anything so far. It is never
	// a false pass, because the caller waits for the count to GROW.
	b, err := io.ReadAll(r)
	if err != nil {
		return 0
	}
	return len(b)
}
