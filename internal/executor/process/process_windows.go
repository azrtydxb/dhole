//go:build windows

package process

import (
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/azrtydxb/dhole/internal/executor"
)

// processTree is one Job Object holding the command and everything it spawns.
//
// Windows has no process groups a signal can be delivered to, and — the part
// that bites — killing a process does NOT kill its descendants. A step that
// starts a compiler that starts a linker leaves the linker running when the
// step is cancelled, and nothing in a passing build ever shows it. The kernel
// object that does contain a tree is a Job: a process assigned to a job cannot
// leave it, every process it creates joins it, and terminating the job
// terminates all of them at once.
type processTree struct {
	mu sync.Mutex
	// job is zero until the command has started and been assigned.
	job        windows.Handle
	terminated bool
	closed     bool
}

// killExitCode is what a step reports when Dhole terminated it. On unix it is
// the POSIX 128+SIGKILL; here it is chosen to match, so an operator reads one
// number for a killed step whatever the engine runs on, and
// docs/wire-contract.md documents that single number.
const killExitCode = 137

// newProcessTree puts the command in its own console process group, so that a
// Ctrl+C delivered to the engine's console is not delivered to every step it
// is running as well.
func newProcessTree(c *exec.Cmd) *processTree {
	if c.SysProcAttr == nil {
		c.SysProcAttr = &syscall.SysProcAttr{}
	}
	c.SysProcAttr.CreationFlags |= windows.CREATE_NEW_PROCESS_GROUP
	return &processTree{}
}

// adopt creates the job and puts the started process in it.
//
// The assignment happens after Start rather than to a suspended process,
// because os/exec does not hand out the initial thread handle needed to resume
// one. The window between Start and the assignment is microseconds wide, and a
// grandchild created inside it would escape the job; a step that forks that
// fast, that early, would have to do it before its own entry point runs.
// Failing to assign is reported rather than ignored: a command Dhole cannot
// terminate is worse than a command that did not start.
func (t *processTree) adopt(c *exec.Cmd) error {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return fmt.Errorf("process executor: create job object: %w", err)
	}
	// KILL_ON_JOB_CLOSE is the backstop: if the engine dies, its handles close
	// and the kernel kills the tree. Without it a crashed engine leaves every
	// step it was running on the host.
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{
		BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{
			LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
		},
	}
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	); err != nil {
		_ = windows.CloseHandle(job)
		return fmt.Errorf("process executor: configure job object: %w", err)
	}

	// #nosec G115 -- a pid is a 32-bit DWORD on Windows; the conversion back
	// is exact.
	proc, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(c.Process.Pid))
	if err != nil {
		_ = windows.CloseHandle(job)
		return fmt.Errorf("process executor: open process %d: %w", c.Process.Pid, err)
	}
	defer func() { _ = windows.CloseHandle(proc) }()

	if err := windows.AssignProcessToJobObject(job, proc); err != nil {
		_ = windows.CloseHandle(job)
		return fmt.Errorf("process executor: assign process %d to job: %w", c.Process.Pid, err)
	}

	t.mu.Lock()
	t.job = job
	pending := t.terminated
	t.mu.Unlock()
	if pending {
		// Cancellation landed between Start and here, when there was no job
		// to terminate yet. Without this the step would run on, cancelled in
		// name only.
		return t.terminate()
	}
	return nil
}

// signal ends the tree. Windows has no signal a running child can be asked to
// handle — there is no SIGTERM to trap and no graceful equivalent for a
// process that is not attached to this console — so all three of Dhole's
// signals terminate the job. An engine author must not expect a step on
// Windows to get a chance to shut down cleanly; docs/wire-contract.md says so.
func (t *processTree) signal(sig executor.Signal) error {
	switch sig {
	case executor.SIGINT, executor.SIGTERM, executor.SIGKILL:
		return t.terminate()
	default:
		return fmt.Errorf("process executor: unknown signal %q", sig)
	}
}

// terminate kills the job, and with it every process the step created. This is
// the whole reason a job exists here: TerminateProcess on the command alone
// would leave its descendants running.
func (t *processTree) terminate() error {
	t.mu.Lock()
	t.terminated = true
	job := t.job
	closed := t.closed
	t.mu.Unlock()
	if job == 0 || closed {
		return nil
	}
	if err := windows.TerminateJobObject(job, killExitCode); err != nil {
		return fmt.Errorf("process executor: terminate job: %w", err)
	}
	return nil
}

// close releases the job handle. It runs only once the command has been
// waited for, so the KILL_ON_JOB_CLOSE limit cannot cut a live step short.
func (t *processTree) close() {
	t.mu.Lock()
	job := t.job
	t.closed = true
	t.job = 0
	t.mu.Unlock()
	if job != 0 {
		_ = windows.CloseHandle(job)
	}
}
