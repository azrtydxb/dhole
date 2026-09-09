//go:build unix

package process

import (
	"fmt"
	"os/exec"
	"syscall"

	"github.com/azrtydxb/dhole/internal/executor"
)

// setProcessGroup puts the command in its own process group, making it the
// group leader. Every process it spawns joins that group, which is what lets
// signalProcessGroup reach the whole tree rather than just the shell at its
// root.
func setProcessGroup(c *exec.Cmd) {
	if c.SysProcAttr == nil {
		c.SysProcAttr = &syscall.SysProcAttr{}
	}
	c.SysProcAttr.Setpgid = true
}

// signalProcessGroup signals the group led by pid. The negative pid is what
// makes it the group: signalling pid alone leaves grandchildren — the `sleep`
// inside a `sh -c` — running on the host after the step is gone.
func signalProcessGroup(pid int, sig executor.Signal) error {
	s, err := unixSignal(sig)
	if err != nil {
		return err
	}
	if err := syscall.Kill(-pid, s); err != nil {
		return fmt.Errorf("process executor: signal group %d: %w", pid, err)
	}
	return nil
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
