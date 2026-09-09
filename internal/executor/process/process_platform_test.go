package process_test

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/executortest"
	"github.com/azrtydxb/dhole/internal/executor/process"
)

// oomChildEnv makes this test binary re-exec itself as a step that asks for
// more memory than any machine can give. It is an env variable rather than a
// separate fixture program so the "step" is a real, ordinary process the
// executor starts and reaps, on every platform, with no toolchain assumptions.
const oomChildEnv = "DHOLE_TEST_OOM_CHILD"

// TestMain is the other half of that re-exec: when the variable is set this
// binary is not a test runner at all, it is the memory-exhausting step.
func TestMain(m *testing.M) {
	if os.Getenv(oomChildEnv) != "" {
		exhaustMemory()
		// Unreachable: the allocation above must not return. Exiting 0 here
		// would be the very "silent success" this file exists to forbid, so
		// it exits non-zero and distinctly instead.
		os.Exit(99)
	}
	os.Exit(m.Run())
}

// exhaustMemory asks for more address space than the runtime can ever map. It
// fails immediately and without touching a page, so it exercises the
// allocation-refused path without putting the host under real memory pressure
// — which a test that genuinely filled RAM would do to whatever else is on the
// machine, including the rest of this suite.
func exhaustMemory() {
	n := int(^uint(0) >> 2) // half the address space; beyond any heap
	sink := make([]byte, n)
	// Keep the allocation live so a compiler cannot elide it.
	os.Exit(len(sink) & 1)
}

// shellPath is the POSIX shell the process-tree cases need. On Windows the
// runners carry Git's sh; a host without one cannot express "spawn a child
// that outlives its parent" in a portable command, so the test says why it
// skipped instead of pretending to have checked.
func shellPath(t *testing.T) string {
	t.Helper()
	sh, err := exec.LookPath("sh")
	if err != nil {
		t.Skipf("no POSIX shell on PATH (%v): the process-tree cases need one to spawn a grandchild", err)
	}
	return sh
}

// TestProcessTreeIsKilledOnCancelOnEveryPlatform is the shared contract's
// cancellation case aimed straight at this backend, on whichever platform the
// test binary was built for. It uses executortest.OrphanScript so the process
// engine and every other backend are held to the same command: a grandchild
// that outlives its parent, traps SIGTERM, and reports its own liveness.
func TestProcessTreeIsKilledOnCancelOnEveryPlatform(t *testing.T) {
	sh := shellPath(t)
	sb := acquireSandbox(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan int32, 1)
	go func() {
		code, _ := sb.Exec(ctx, executor.Cmd{
			Args: []string{sh, "-c", executortest.OrphanScript},
			// A plain writer, so os/exec streams through a pipe the whole
			// tree inherits: Exec cannot return while a grandchild holds it.
			Stdout: &discardWriter{},
		})
		done <- code
	}()

	// The grandchild has to be genuinely running before cancelling proves
	// anything: cancelling a tree that never started passes trivially.
	require.Eventually(t, func() bool { return executortest.Heartbeat(t, sb) > 0 }, 10*time.Second, 50*time.Millisecond,
		"the orphaned grandchild never started writing its heartbeat")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Exec did not return within 2s of cancellation: the process tree still holds its output pipe")
	}

	// Returning is not the property. The property is that nothing is left
	// running on the host, and a leaked grandchild goes on writing.
	before := executortest.Heartbeat(t, sb)
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, before, executortest.Heartbeat(t, sb),
		"a grandchild is still running after cancellation: the process tree leaked onto the host (%s)", runtime.GOOS)
}

// TestExitCodeOnOOMIsReportedConsistently pins what a memory-exhausted step
// reports, in both directions memory exhaustion arrives: the kernel killing
// the step (Linux's OOM killer, macOS's Jetsam, a Windows job limit) and the
// step's own allocation being refused. Neither may look like success — a step
// reported as exit 0 is a step whose result may be cached and shipped.
//
// The codes asserted here are the ones docs/wire-contract.md documents.
func TestExitCodeOnOOMIsReportedConsistently(t *testing.T) {
	t.Run("a step killed the way an OOM kill arrives reports 137", func(t *testing.T) {
		sh := shellPath(t)
		sb := acquireSandbox(t)

		done := make(chan int32, 1)
		go func() {
			code, _ := sb.Exec(context.Background(), executor.Cmd{
				// Ignores SIGTERM, exactly as an OOM kill ignores politeness:
				// only the SIGKILL the kernel would send ends it.
				Args:   []string{sh, "-c", `trap "" TERM; while :; do sleep 0.05; done`},
				Stdout: &discardWriter{},
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
		}, 5*time.Second, 50*time.Millisecond, "SIGKILL did not stop the step")

		require.Equal(t, int32(137), code,
			"a step killed by SIGKILL must report 137 on every platform, so an operator reads one number for an OOM kill")
	})

	t.Run("a step whose allocation is refused reports its own non-zero status", func(t *testing.T) {
		self, err := os.Executable()
		require.NoError(t, err)
		sb := acquireSandbox(t)

		code, err := sb.Exec(t.Context(), executor.Cmd{
			Args:   []string{self, "-test.run=TestExitCodeOnOOMIsReportedConsistently"},
			Env:    map[string]string{oomChildEnv: "1"},
			Stdout: &discardWriter{},
			Stderr: &discardWriter{},
		})
		require.NoError(t, err, "a step that runs and dies out of memory is not an executor failure")
		require.NotEqual(t, int32(0), code, "a step that ran out of memory must never be reported as a success")
		require.Positive(t, code,
			"a negative exit code sign-extends to ten bytes on the wire and hangs a varint decoder (docs/wire-contract.md)")
		require.NotEqual(t, int32(99), code, "the child returned from an allocation that must not return")
	})
}

func acquireSandbox(t *testing.T) executor.Sandbox {
	t.Helper()
	sb, err := process.New().Acquire(t.Context(), executor.Spec{Lease: executor.LeaseStep})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sb.Release(context.WithoutCancel(t.Context()))) })
	return sb
}

// discardWriter is a writer that is not an *os.File, which is what forces
// os/exec to give the command a pipe the whole tree inherits.
type discardWriter struct{}

func (*discardWriter) Write(p []byte) (int, error) { return len(p), nil }
