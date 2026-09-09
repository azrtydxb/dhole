//go:build darwin

package process

import (
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/azrtydxb/dhole/internal/executor"
)

// processTree is the macOS process tree: one process group led by the command
// this sandbox started. Every process the step spawns inherits the group,
// which is what lets one signal reach the whole tree rather than only the
// shell at its root.
//
// This is a separate file from process_unix.go rather than a shared `unix`
// build tag, and the duplication is deliberate. From Task 54 macOS is a
// supported engine platform, not a unix that happens to compile, and the two
// places where it differs from Linux are both invisible in the system calls:
//
//   - There is no OOM killer. Linux picks a victim and sends SIGKILL when the
//     cgroup or the machine is out of memory; macOS refuses the allocation and
//     lets the program fail, and only under real system-wide pressure does
//     Jetsam kill a process — with SIGKILL, which arrives here as 137, the
//     same number Linux's OOM killer produces. A memory-exhausted step on
//     macOS therefore usually reports the program's OWN non-zero status, and
//     only sometimes 137. docs/wire-contract.md documents both.
//   - Resource accounting differs: ru_maxrss is bytes here and kilobytes on
//     Linux (see usage_unix.go).
//
// Sharing one file would mean macOS silently inheriting Linux's answers to
// those questions, with nothing in the tree to notice when one of them stops
// being true.
type processTree struct {
	mu sync.Mutex
	// pgid is zero until the command has started: there is no group before
	// there is a leader.
	pgid       int
	terminated bool
}

// terminationGrace is how long a cancelled step gets to handle SIGTERM before
// its tree is killed outright.
const terminationGrace = 500 * time.Millisecond

// newProcessTree configures c to lead its own process group. It runs before
// Start, because a group cannot be joined retroactively by children that have
// already been forked.
func newProcessTree(c *exec.Cmd) *processTree {
	if c.SysProcAttr == nil {
		c.SysProcAttr = &syscall.SysProcAttr{}
	}
	c.SysProcAttr.Setpgid = true
	return &processTree{}
}

// adopt records the started command's group. The leader's pid is the group id,
// which is only knowable after Start.
func (t *processTree) adopt(c *exec.Cmd) error {
	t.mu.Lock()
	t.pgid = c.Process.Pid
	pending := t.terminated
	t.mu.Unlock()
	if pending {
		// Cancellation landed between Start and here, when there was no group
		// to signal yet. Without this the step would run on, cancelled in
		// name only.
		return t.terminate()
	}
	return nil
}

// signal delivers sig to the whole group.
func (t *processTree) signal(sig executor.Signal) error {
	s, err := unixSignal(sig)
	if err != nil {
		return err
	}
	return t.kill(s)
}

// terminate ends the tree: SIGTERM first so a step can shut down cleanly, then
// SIGKILL once the grace period is up.
func (t *processTree) terminate() error {
	t.mu.Lock()
	t.terminated = true
	pgid := t.pgid
	t.mu.Unlock()
	if pgid == 0 {
		return nil
	}
	if err := t.kill(syscall.SIGTERM); err != nil {
		return err
	}
	deadline := time.Now().Add(terminationGrace)
	for time.Now().Before(deadline) {
		if !t.alive() {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	// SIGTERM is a request. A step that traps it, or that wedged before it
	// could run its handler, still has to be gone: without this escalation
	// the tree outlives the run and the host collects one more of them per
	// cancelled build.
	return t.kill(syscall.SIGKILL)
}

// close releases whatever the tree holds. A process group is not a resource,
// so there is nothing to release here.
func (t *processTree) close() {}

func (t *processTree) kill(s syscall.Signal) error {
	t.mu.Lock()
	pgid := t.pgid
	t.mu.Unlock()
	if pgid == 0 {
		return nil
	}
	// The NEGATIVE pid is the group. Signalling pgid alone reaches only the
	// process this sandbox started, and orphans everything it spawned onto
	// the host — which is how a CI agent leaks processes until the machine
	// dies.
	if err := syscall.Kill(-pgid, s); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("process executor: signal group %d: %w", pgid, err)
	}
	return nil
}

// alive reports whether the group still has any member. Signal 0 performs the
// permission and existence checks without delivering anything.
func (t *processTree) alive() bool {
	t.mu.Lock()
	pgid := t.pgid
	t.mu.Unlock()
	return pgid != 0 && syscall.Kill(-pgid, 0) == nil
}

func unixSignal(sig executor.Signal) (syscall.Signal, error) {
	switch sig {
	case executor.SIGINT:
		return syscall.SIGINT, nil
	case executor.SIGTERM:
		return syscall.SIGTERM, nil
	case executor.SIGKILL:
		return syscall.SIGKILL, nil
	default:
		return 0, fmt.Errorf("process executor: unknown signal %q", sig)
	}
}
