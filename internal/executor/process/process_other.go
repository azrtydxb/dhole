//go:build !unix

package process

import (
	"errors"
	"fmt"
	"os/exec"

	"github.com/azrtydxb/dhole/internal/executor"
)

// setProcessGroup is a no-op where process groups do not exist. Task 54 brings
// the Windows engine, which kills a job object instead; until then this file
// exists only so the package still compiles off unix.
var errUnsupportedPlatform = errors.New("process groups are not supported on this platform")

func setProcessGroup(*exec.Cmd) {}

func signalProcessGroup(_ int, sig executor.Signal) error {
	return fmt.Errorf("process executor: signal %q: %w", sig, errUnsupportedPlatform)
}
