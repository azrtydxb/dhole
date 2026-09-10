package vm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
)

// errHypervisorExited marks the one failure that must not be retried. Waiting
// for a guest agent is normally a matter of polling through a few tens of
// milliseconds of connection refusals, so the dial loop retries by default —
// but a hypervisor that has already exited will never start listening, and
// spending the whole boot budget discovering that turns a one-line
// configuration error into a thirty-second timeout with no cause in it.
var errHypervisorExited = errors.New("the hypervisor exited")

// hypervisorProcess is the part of a machine that is the same for Firecracker
// and QEMU: a child process, its guest console on disk, and a single place
// that knows whether it is still alive.
type hypervisorProcess struct {
	cmd         *exec.Cmd
	console     *os.File
	consolePath string

	done  chan struct{}
	state *os.ProcessState

	stopOnce sync.Once
}

func startHypervisor(ctx context.Context, bin string, args []string, consolePath string) (*hypervisorProcess, error) {
	console, err := os.Create(consolePath) // #nosec G304 -- a path this process just made
	if err != nil {
		return nil, fmt.Errorf("vm executor: create the guest console log: %w", err)
	}
	cmd := exec.CommandContext(ctx, bin, args...) // #nosec G204 -- the hypervisor path is operator configuration
	// The guest's serial console is the hypervisor's stdout. It is the only
	// evidence of a kernel that panicked before the agent ever ran, so it goes
	// to a file that outlives the boot rather than to a discarded pipe.
	cmd.Stdout, cmd.Stderr = console, console
	if err := cmd.Start(); err != nil {
		_ = console.Close()
		return nil, fmt.Errorf("vm executor: start %s: %w", bin, err)
	}
	p := &hypervisorProcess{cmd: cmd, console: console, consolePath: consolePath, done: make(chan struct{})}
	// Reaped here and nowhere else. A hypervisor that dies on its own is a
	// zombie until somebody waits for it, and the sandbox that owns it may not
	// be released for hours.
	go func() {
		_ = cmd.Wait()
		p.state = cmd.ProcessState
		close(p.done)
	}()
	return p, nil
}

// exited reports the hypervisor's death, or nil while it is running.
func (p *hypervisorProcess) exited() error {
	select {
	case <-p.done:
		return fmt.Errorf("%w: %s (guest console: %s)", errHypervisorExited, p.state, p.consolePath)
	default:
		return nil
	}
}

func (p *hypervisorProcess) diagnostics() string { return p.consolePath }

func (p *hypervisorProcess) stop() error {
	p.stopOnce.Do(func() {
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		<-p.done
		_ = p.console.Close()
	})
	return nil
}
