// Package process runs steps as bare host processes.
//
// It is the simplest executor backend and the reason the executor interface is
// not shaped around containers: there is no image, no namespace and no daemon
// here, yet every method of executor.Sandbox still means something. What this
// backend cannot honestly promise — privilege separation, controlled host
// mounts — it declines to advertise, so the scheduler never hands it work that
// assumes isolation it does not have.
package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/executor"
)

// Kind is this backend's stable identifier in configuration.
const Kind = "process"

// Executor hands out sandboxes that are temporary directories on the host.
type Executor struct{}

// New returns the local process executor. It holds no state: a sandbox is a
// directory and the processes currently running in it.
func New() *Executor { return &Executor{} }

// Kind implements executor.Executor.
func (*Executor) Kind() string { return Kind }

// Capabilities implements executor.Executor. The set is empty on purpose. A
// bare process shares the host's user, filesystem and network; it cannot grant
// privilege as a controlled capability, and it cannot bound a host mount,
// because every path on the host is already reachable. Advertising either
// would be a promise this backend cannot keep.
func (*Executor) Capabilities() []dholev1.Capability { return []dholev1.Capability{} }

// EnvironmentIdentity implements executor.Executor. A host process runs
// against whatever compilers, libraries and tools the host happens to carry,
// and that cannot be digested. Returning a fabricated identity would let the
// cache serve results from an environment that has since changed, so this
// backend reports the truth and its steps stay non-cacheable.
func (*Executor) EnvironmentIdentity() (string, error) {
	return "", executor.ErrNoStableIdentity
}

// Acquire implements executor.Executor. spec.Image is ignored: this backend
// has no images, and asking for one is not an error.
func (*Executor) Acquire(_ context.Context, spec Spec) (executor.Sandbox, error) {
	root, err := os.MkdirTemp("", "dhole-sandbox-")
	if err != nil {
		return nil, fmt.Errorf("process executor: create sandbox directory: %w", err)
	}
	workDir := root
	if spec.WorkDir != "" {
		workDir, err = resolve(root, spec.WorkDir)
		if err != nil {
			return nil, errors.Join(err, os.RemoveAll(root))
		}
		if err := os.MkdirAll(workDir, 0o750); err != nil {
			return nil, errors.Join(
				fmt.Errorf("process executor: create work directory: %w", err),
				os.RemoveAll(root),
			)
		}
	}
	return &sandbox{root: root, workDir: workDir, env: spec.Env, lease: spec.Lease}, nil
}

// Spec is executor.Spec; aliased so this file reads without the package
// qualifier on every field.
type Spec = executor.Spec

type sandbox struct {
	root    string
	workDir string
	env     map[string]string
	lease   executor.LeaseScope

	mu       sync.Mutex
	released bool
	running  map[*processTree]struct{} // the trees of the commands running now
	// usage of the most recently finished command, when the platform reports
	// it. Kept here because it exists only on the ProcessState of a reaped
	// process: nothing can ask for it afterwards.
	usage    processUsage
	hasUsage bool
}

// EnvironmentIdentity implements executor.Sandbox. A temporary directory on a
// host is no more nameable than the host itself, so a process sandbox answers
// exactly what its executor does: nothing, honestly.
func (*sandbox) EnvironmentIdentity() (string, error) {
	return "", executor.ErrNoStableIdentity
}

// processUsage is what one finished command consumed.
type processUsage struct {
	cpuSeconds  float64
	maxRSSBytes int64
}

// LastUsage reports the resources of the most recently finished command, and
// false when this platform does not report them. It satisfies the optional
// interface telemetry looks for, without this package depending on telemetry.
func (s *sandbox) LastUsage() (cpuSeconds float64, maxRSSBytes int64, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usage.cpuSeconds, s.usage.maxRSSBytes, s.hasUsage
}

func (s *sandbox) recordUsage(state *os.ProcessState) {
	cpuSeconds, maxRSSBytes, ok := usageOf(state)
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage = processUsage{cpuSeconds: cpuSeconds, maxRSSBytes: maxRSSBytes}
	s.hasUsage = true
}

// Exec runs a command to completion. A non-zero exit is reported as an exit
// code, not an error: only failing to run the command at all is an error.
// cancelDrainDelay bounds how long Wait may spend draining a cancelled
// command's output after the group has been killed.
const cancelDrainDelay = 5 * time.Second

func (s *sandbox) Exec(ctx context.Context, cmd executor.Cmd) (int32, error) {
	if len(cmd.Args) == 0 {
		return 0, errors.New("process executor: exec with no command")
	}

	dir := s.workDir
	if cmd.Dir != "" {
		var err error
		if dir, err = resolve(s.workDir, cmd.Dir); err != nil {
			return 0, err
		}
	}

	// Running a caller-supplied command is this backend's entire purpose. The
	// argument vector goes to execve directly, with no shell in between, so
	// there is no string for an argument to break out of; what a step may run
	// at all is a policy decision taken before it ever reaches an executor.
	// #nosec G204 -- see above.
	// nosemgrep: dangerous-exec-command
	c := exec.CommandContext(ctx, cmd.Args[0], cmd.Args[1:]...)
	c.Dir = dir
	c.Env = environ(s.env, cmd.Env)
	c.Stdin = cmd.Stdin
	c.Stdout = cmd.Stdout
	c.Stderr = cmd.Stderr
	// The command and everything it spawns are confined to one tree — a
	// process group on unix, a job object on Windows — so that terminating
	// the step terminates all of it. Without this, cancelling a shell leaves
	// its children running on the host forever.
	tree := newProcessTree(c)
	// os/exec's default cancellation kills the started process and nothing
	// else, which is no cancellation at all for a step that spawned anything:
	// the child it kills may already have exited, while the grandchild that
	// holds the work runs on — still holding the pipes this command's output
	// is read through, so Wait blocks draining a pipe nobody will close and a
	// cancelled step never returns. Cancellation is the ONLY way a job is
	// stopped in production (internal/engine cancels the context; nothing
	// calls Signal), so this is the path that has to reach the whole tree.
	c.Cancel = tree.terminate
	// And a bound on the drain even so. A process that escaped the tree, or
	// one ignoring SIGKILL while stuck in the kernel, must not turn a
	// cancellation into a hang: after this, Wait returns and the pipes are
	// closed under it.
	c.WaitDelay = cancelDrainDelay

	if err := c.Start(); err != nil {
		tree.close()
		return 0, fmt.Errorf("process executor: start %q: %w", cmd.Args[0], err)
	}
	if err := tree.adopt(c); err != nil {
		// A command that cannot be confined is a command that cannot be
		// stopped. Killing it now is better than running a step that would
		// outlive its run.
		_ = c.Process.Kill()
		_ = c.Wait()
		tree.close()
		return 0, fmt.Errorf("process executor: confine %q: %w", cmd.Args[0], err)
	}
	s.track(tree)

	// os/exec's own context watcher retires the moment the started process is
	// reaped, so it never fires for the case that matters: a step whose direct
	// child exits and leaves a grandchild holding the work — and the output
	// pipe. Watching the context here covers both, and it is the reason Exec
	// returns at all in that case rather than blocking forever on a pipe held
	// by a process nothing is going to stop.
	watched := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			_ = tree.terminate()
		case <-watched:
		}
	}()

	err := c.Wait()
	close(watched)
	// Wait for the watcher before releasing the tree: a terminate still in
	// flight must not race the handle it is terminating through.
	<-watcherDone
	s.untrack(tree)
	tree.close()
	s.recordUsage(c.ProcessState)

	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return narrowExit(c.ProcessState.ExitCode()), nil
	case errors.As(err, &exitErr):
		return exitCode(exitErr), nil
	default:
		return 0, fmt.Errorf("process executor: run %q: %w", cmd.Args[0], err)
	}
}

// Signal delivers sig to the process group of every command the sandbox is
// currently running. Signalling an idle sandbox is not an error.
func (s *sandbox) Signal(_ context.Context, sig executor.Signal) error {
	s.mu.Lock()
	trees := make([]*processTree, 0, len(s.running))
	for tree := range s.running {
		trees = append(trees, tree)
	}
	s.mu.Unlock()

	var errs []error
	for _, tree := range trees {
		// The tree, not the process: a step is a tree, and killing only its
		// root orphans the rest onto the host.
		if err := tree.signal(sig); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Put writes a file into the sandbox. name is relative to the sandbox root and
// cannot escape it.
func (s *sandbox) Put(_ context.Context, name string, r io.Reader) error {
	path, err := resolve(s.root, name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("process executor: put %q: %w", name, err)
	}
	// #nosec G304 -- resolve has already confined path to the sandbox root.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("process executor: put %q: %w", name, err)
	}
	if _, err := io.Copy(f, r); err != nil {
		return errors.Join(fmt.Errorf("process executor: put %q: %w", name, err), f.Close())
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("process executor: put %q: %w", name, err)
	}
	return nil
}

// Mkdir creates a directory in the sandbox, with its parents. name is relative
// to the sandbox root and cannot escape it.
func (s *sandbox) Mkdir(_ context.Context, name string) error {
	path, err := resolve(s.root, name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(path, 0o750); err != nil {
		return fmt.Errorf("process executor: mkdir %q: %w", name, err)
	}
	return nil
}

// Get reads a file back out of the sandbox. The caller closes the reader.
func (s *sandbox) Get(_ context.Context, name string) (io.ReadCloser, error) {
	path, err := resolve(s.root, name)
	if err != nil {
		return nil, err
	}
	// #nosec G304 -- resolve has already confined path to the sandbox root.
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("process executor: get %q: %w", name, err)
	}
	return f, nil
}

// Release removes the sandbox directory. It is idempotent so cleanup paths can
// run unconditionally.
func (s *sandbox) Release(_ context.Context) error {
	s.mu.Lock()
	if s.released {
		s.mu.Unlock()
		return nil
	}
	s.released = true
	s.mu.Unlock()

	if err := os.RemoveAll(s.root); err != nil {
		return fmt.Errorf("process executor: release sandbox: %w", err)
	}
	return nil
}

func (s *sandbox) track(tree *processTree) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running == nil {
		s.running = make(map[*processTree]struct{})
	}
	s.running[tree] = struct{}{}
}

func (s *sandbox) untrack(tree *processTree) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.running, tree)
}

// environ merges the sandbox environment with a command's overrides. The host
// environment is deliberately not inherited: a step that depends on an ambient
// variable is a step whose inputs are not declared.
func environ(base, overrides map[string]string) []string {
	merged := make(map[string]string, len(base)+len(overrides))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range overrides {
		merged[k] = v
	}
	env := make([]string, 0, len(merged))
	for k, v := range merged {
		env = append(env, k+"="+v)
	}
	return env
}

// resolve joins name onto root and refuses anything that leaves it, so a step
// cannot read or write the host through a crafted artifact name.
func resolve(root, name string) (string, error) {
	if name == "" {
		return "", errors.New("process executor: empty path")
	}
	if filepath.IsAbs(name) {
		return "", fmt.Errorf("process executor: %q is absolute, sandbox paths are relative", name)
	}
	path := filepath.Join(root, filepath.Clean(name))
	if path != root && !strings.HasPrefix(path, root+string(filepath.Separator)) {
		return "", fmt.Errorf("process executor: %q escapes the sandbox", name)
	}
	return path, nil
}

// narrowExit converts an exit status to the wire's int32. A POSIX exit status
// is a byte and a signal-derived status is 128 plus a signal number, so the
// clamp never fires in practice; it exists so the conversion cannot wrap into
// a different exit code on a platform that reports something wider. -1 is
// preserved: it means "no status of its own".
func narrowExit(code int) int32 {
	switch {
	case code < 0:
		return -1
	case code > 255:
		return 255
	default:
		return int32(code)
	}
}
