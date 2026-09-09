//go:build !unix && !windows

package process

import (
	"errors"
	"fmt"
	"os/exec"

	"github.com/azrtydxb/dhole/internal/executor"
)

// errUnsupportedPlatform is what a platform with neither process groups nor
// job objects — js/wasm, plan9 — can honestly say. The package still compiles
// and still runs commands there; what it refuses to do is pretend it can
// terminate a tree it has no way to reach.
var errUnsupportedPlatform = errors.New("process trees are not supported on this platform")

type processTree struct{}

func newProcessTree(*exec.Cmd) *processTree { return &processTree{} }

func (*processTree) adopt(*exec.Cmd) error { return nil }

func (*processTree) signal(sig executor.Signal) error {
	return fmt.Errorf("process executor: signal %q: %w", sig, errUnsupportedPlatform)
}

func (*processTree) terminate() error {
	return fmt.Errorf("process executor: terminate: %w", errUnsupportedPlatform)
}

func (*processTree) close() {}
