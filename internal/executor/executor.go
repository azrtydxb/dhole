// Package executor defines the boundary between the control plane and the
// place a step actually runs.
//
// The interface is deliberately not shaped around containers. A bare host
// process and a virtual machine are first-class backends: nothing here names
// an image layer, a mount, a namespace or a daemon. Spec.Image is a hint an
// executor is free to ignore — the process backend has no image at all — and
// anything a backend cannot do is a capability it declines to advertise, never
// a special case in the core (ADR 0006).
//
// Sandbox lifetime is explicit rather than implicit per backend: a LeaseScope
// says how long the sandbox outlives the step that acquired it. That is what
// makes reuse safe to reason about — a pooled sandbox carries state from
// earlier runs that cannot be hashed, so steps that run in one are not
// cacheable.
package executor

import (
	"context"
	"errors"
	"io"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
)

// ErrNoStableIdentity is returned by EnvironmentIdentity when a backend has no
// reproducible environment to hash. A host-process backend runs against
// whatever toolchain the host happens to carry, and no honest digest describes
// that; the scheduler treats such steps as non-cacheable rather than caching
// them against an identity that quietly changes.
var ErrNoStableIdentity = errors.New("executor: no stable environment identity")

// LeaseScope is how long a sandbox lives. It is chosen by the caller, not
// inferred from the backend, because it decides both cleanup and cacheability.
type LeaseScope string

const (
	// LeaseStep releases the sandbox when the step finishes. The default, and
	// the only scope with no unhashable carried state.
	LeaseStep LeaseScope = "step"
	// LeaseJob keeps the sandbox for every step of one job.
	LeaseJob LeaseScope = "job"
	// LeasePipeline keeps the sandbox for the whole pipeline run.
	LeasePipeline LeaseScope = "pipeline"
	// LeasePool takes a warm sandbox reused across runs. Faster, and
	// explicitly dirty: its prior state is an input that cannot be hashed, so
	// steps that run in one are not cacheable.
	LeasePool LeaseScope = "pool"
	// LeaseService is a sidecar sandbox bounded by the pipeline that started
	// it, released when that pipeline ends.
	LeaseService LeaseScope = "service"
)

// Requirements are what a step needs of the place it runs. The scheduler
// matches these against what each executor advertises.
type Requirements struct {
	// OS and Arch name the target platform, in Go's GOOS/GOARCH vocabulary.
	// Empty means the step does not care.
	OS   string
	Arch string
	// Capabilities the step needs the sandbox to grant. An executor that does
	// not advertise one cannot be given the step.
	Capabilities []dholev1.Capability
}

// Spec describes the sandbox a caller wants.
type Spec struct {
	// Image is a hint. Backends that have a notion of an image resolve it;
	// those that do not — the process backend, a bare VM — ignore it. It is
	// never an error to ask for one a backend cannot use.
	Image string
	// Env is the base environment every command in the sandbox inherits. A
	// Cmd may add to or override it.
	Env map[string]string
	// WorkDir is the directory commands start in, interpreted by the backend.
	// Empty means the sandbox root.
	WorkDir string
	// Lease is how long the sandbox lives. The zero value means LeaseStep.
	Lease LeaseScope
	// Requirements the sandbox must satisfy.
	Requirements Requirements
}

// Signal is a process signal expressed backend-neutrally. A backend maps it to
// whatever its platform offers — a POSIX signal, a Windows console control
// event, an ACPI button on a VM — so callers never speak syscall numbers.
type Signal string

const (
	// SIGINT asks the running command to stop, as an interactive interrupt.
	SIGINT Signal = "INT"
	// SIGTERM asks the running command to stop and clean up. The signal
	// cancellation uses first.
	SIGTERM Signal = "TERM"
	// SIGKILL stops the running command without giving it a chance to react.
	SIGKILL Signal = "KILL"
)

// Cmd is one command to run inside a sandbox.
type Cmd struct {
	// Args is the full argument vector; Args[0] is the program. There is no
	// shell in between: a caller that wants shell semantics asks for a shell.
	Args []string
	// Env adds to, and overrides, the sandbox environment for this command.
	Env map[string]string
	// Dir is relative to the sandbox working directory. Empty means that
	// directory itself.
	Dir string
	// Stdin, Stdout and Stderr are optional. A nil Stdin reads EOF; nil
	// outputs are discarded.
	Stdin          io.Reader
	Stdout, Stderr io.Writer
}

// Sandbox is one acquired execution environment. Every method is meaningful
// for a process, a container and a VM alike; a method only one of them could
// implement does not belong here.
type Sandbox interface {
	// Exec runs a command to completion and returns its exit code. A command
	// that runs and exits non-zero is not an error: err is reserved for the
	// sandbox failing to run it at all.
	Exec(ctx context.Context, cmd Cmd) (exitCode int32, err error)
	// Put writes a file into the sandbox under name, a path relative to the
	// sandbox root. It never escapes that root.
	Put(ctx context.Context, name string, r io.Reader) error
	// Get reads a file back out. The caller closes the reader.
	Get(ctx context.Context, name string) (io.ReadCloser, error)
	// Signal delivers sig to everything the sandbox is currently running. It
	// is not an error to signal an idle sandbox.
	Signal(ctx context.Context, sig Signal) error
	// Release tears the sandbox down. It is idempotent: releasing an already
	// released sandbox is not an error, so cleanup paths can be unconditional.
	Release(ctx context.Context) error
}

// Executor is one backend: a place sandboxes come from.
type Executor interface {
	// Acquire returns a sandbox matching spec, ready to run commands.
	Acquire(ctx context.Context, spec Spec) (Sandbox, error)
	// Capabilities is what this backend can honestly grant. A backend that
	// cannot enforce a capability must not advertise it.
	Capabilities() []dholev1.Capability
	// Kind is the backend's stable identifier, as configuration names it.
	Kind() string
	// EnvironmentIdentity is a digest of the environment steps run in —
	// an image digest, a VM snapshot id — used as a cache key input. A
	// backend with no reproducible environment returns ErrNoStableIdentity
	// and an empty string.
	EnvironmentIdentity() (string, error)
}
