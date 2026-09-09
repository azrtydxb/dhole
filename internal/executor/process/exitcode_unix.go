//go:build unix

package process

import (
	"os/exec"
	"syscall"
)

// exitCode reports the exit status of a finished command. A process killed by
// a signal has no exit status of its own, so it is reported by the POSIX
// convention of 128 plus the signal number rather than as -1, which would be
// indistinguishable from "no status" downstream.
func exitCode(err *exec.ExitError) int32 {
	if status, ok := err.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return narrowExit(128 + int(status.Signal()))
	}
	return narrowExit(err.ExitCode())
}
