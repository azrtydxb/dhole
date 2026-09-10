// Package vm runs steps in microVMs: the strong-isolation tier, where the
// boundary between a step and everything else is a guest kernel rather than a
// set of namespaces.
//
// Two hypervisors sit behind one backend. Firecracker is the one worth having
// — a microVM boots in tens of milliseconds and restores from a snapshot in
// fewer — and it exists only for linux/amd64 and linux/arm64 with /dev/kvm.
// QEMU is the portable fallback for every other host, and it is held to
// exactly the same conformance contract, because a fallback nobody tested is a
// fallback that behaves differently on the day it is used.
//
// Nothing in the executor interface had to change to admit this backend, which
// is the claim ADR 0006 made: there is no image, no mount and no daemon in
// Acquire's vocabulary, so a guest kernel fits where a container did. What the
// VM does need — a kernel and a rootfs — arrives as configuration, and
// Spec.Image, which a container backend reads as an image reference, is read
// here as the rootfs snapshot a particular step should boot from.
package vm

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/yamux"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/vm/vmwire"
)

// Kind is this backend's stable identifier in configuration.
const Kind = "vm"

// Backend names a hypervisor.
type Backend string

const (
	// Firecracker is the microVM monitor: no PCI, no BIOS, no legacy devices,
	// and a boot measured in tens of milliseconds.
	Firecracker Backend = "firecracker"
	// QEMU is the portable fallback. Slower to boot and far larger, but it
	// exists everywhere Firecracker does not.
	QEMU Backend = "qemu"
)

// Defaults chosen for a per-step microVM rather than for a workstation: the
// memory size is also the size of a snapshot's memory file, so it is paid
// again on every restore.
const (
	defaultVCPUs  = 1
	defaultMemMiB = 256
	// bootTimeout bounds waiting for the guest agent. A cold Firecracker boot
	// is tens of milliseconds and a cold QEMU boot a second or two; anything
	// past this is a kernel that panicked or a rootfs with no agent in it, and
	// the console log named in the error is where that is visible.
	bootTimeout = 30 * time.Second
	// cancelGrace is how long the host waits for the guest to finish tearing
	// down a cancelled command tree before giving up on an orderly answer. It
	// is longer than the guest's own escalation and shorter than the contract's
	// ten seconds, so a guest that is working gets to finish and a guest that
	// is wedged does not hold the step open.
	cancelGrace = 5 * time.Second
)

// Config is what a VM backend needs of its host.
type Config struct {
	// Backend selects the hypervisor. Empty means Firecracker.
	Backend Backend
	// BinaryPath is the hypervisor executable. Empty means look it up on PATH.
	BinaryPath string
	// KernelImage is the guest kernel every sandbox boots.
	KernelImage string
	// RootfsImage is the default rootfs snapshot: an initramfs archive
	// carrying the guest agent as its init. Its content digest is this
	// backend's environment identity.
	RootfsImage string
	// SnapshotDir is where snapshots are written. Empty disables snapshotting.
	SnapshotDir string
	// VCPUs and MemMiB size the guest. Zero means the defaults above.
	VCPUs  int
	MemMiB int
	// KernelArgs replaces the default kernel command line. Empty means the
	// backend's own, which is what the guest agent expects.
	KernelArgs string
}

// Executor is the VM backend.
type Executor struct {
	cfg    Config
	launch launcher

	identity   identityCache
	nestedVirt bool
}

// launcher is the hypervisor-specific half: everything that differs between
// Firecracker and QEMU lives behind these two calls, and nothing above this
// line knows which one it has.
type launcher interface {
	// boot starts a guest from rootfs, with dir for its sockets and logs, and
	// returns a handle once the hypervisor is running. It does NOT wait for
	// the guest agent — that is dial's job, retried by the caller.
	boot(ctx context.Context, cfg Config, rootfs, dir string) (machine, error)
}

// machine is one running guest.
type machine interface {
	// dial opens a fresh connection to the guest agent, or fails because the
	// guest is not listening yet.
	dial(ctx context.Context) (io.ReadWriteCloser, error)
	// stop tears the guest down. It is idempotent.
	stop() error
	// diagnostics names where the guest console was written, so a boot that
	// never reached the agent can be explained rather than guessed at.
	diagnostics() string
}

// New builds a VM executor. It does not touch the hypervisor: a backend that
// refused to construct without one could not answer EnvironmentIdentity or
// Capabilities, and the control plane asks both of those before it ever
// dispatches a step.
func New(cfg Config) (executor.Executor, error) {
	if cfg.Backend == "" {
		cfg.Backend = Firecracker
	}
	if cfg.VCPUs == 0 {
		cfg.VCPUs = defaultVCPUs
	}
	if cfg.MemMiB == 0 {
		cfg.MemMiB = defaultMemMiB
	}
	var launch launcher
	switch cfg.Backend {
	case Firecracker:
		launch = firecrackerLauncher{}
	case QEMU:
		launch = qemuLauncher{}
	default:
		return nil, fmt.Errorf("vm executor: backend must be %q or %q, got %q", Firecracker, QEMU, cfg.Backend)
	}
	return &Executor{cfg: cfg, launch: launch, nestedVirt: nestedVirtAvailable()}, nil
}

// Kind implements executor.Executor.
func (*Executor) Kind() string { return Kind }

// Capabilities implements executor.Executor. A microVM grants less than a
// container does, not more, and saying so is the point: a capability this
// backend advertises is one the scheduler will send it work for.
//
// There is no NETWORK here because a Dhole microVM is booted with no network
// device at all — a guest with no NIC cannot reach anything, and advertising
// otherwise would route every step that needs the internet to the one backend
// that cannot provide it. HOST_MOUNT is absent for the same reason in the
// other direction: the guest sees a rootfs and nothing of the host, which is
// the property the tier is selected for.
//
// PRIVILEGED is advertised, and it is honest: commands run as uid 0 inside the
// guest. That is safe HERE and nowhere else in Dhole — root in a microVM is
// root over a kernel that owns nothing, where root in a container is root over
// a kernel that owns the node.
func (e *Executor) Capabilities() []dholev1.Capability {
	caps := []dholev1.Capability{dholev1.Capability_CAPABILITY_PRIVILEGED}
	if e.nestedVirt {
		caps = append(caps, dholev1.Capability_CAPABILITY_NESTED_VIRT)
	}
	return caps
}

// EnvironmentIdentity implements executor.Executor: the content digest of the
// rootfs snapshot a sandbox acquired with an EMPTY Spec would boot from.
//
// The snapshot's CONTENT and not its path. A rootfs is rebuilt in place far
// more often than a container image tag is republished — that is what a build
// pipeline for a golden image does — so keying on the filename would serve a
// step compiled against yesterday's toolchain as today's answer, which is the
// stale hit ADR 0021 exists to prevent. A rootfs that cannot be read yields
// ErrNoStableIdentity, and its steps stay uncached, which is the safe
// direction to be wrong in.
func (e *Executor) EnvironmentIdentity() (string, error) {
	return e.identity.of(e.cfg.RootfsImage)
}

// rootfs is the snapshot a sandbox boots: the spec's if the step named one,
// else this backend's default.
func (e *Executor) rootfs(specImage string) string {
	if specImage != "" {
		return specImage
	}
	return e.cfg.RootfsImage
}

// Acquire boots a microVM and waits for its guest agent to answer.
func (e *Executor) Acquire(ctx context.Context, spec executor.Spec) (executor.Sandbox, error) {
	if err := e.grants(spec.Requirements.Capabilities); err != nil {
		return nil, err
	}
	rootfs := e.rootfs(spec.Image)
	if rootfs == "" {
		return nil, errors.New("vm executor: no rootfs image configured and the step named none")
	}
	dir, err := os.MkdirTemp("", "dhole-vm-")
	if err != nil {
		return nil, fmt.Errorf("vm executor: create the sandbox directory: %w", err)
	}
	sb, err := e.start(ctx, dir, rootfs, spec)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return sb, nil
}

func (e *Executor) start(ctx context.Context, dir, rootfs string, spec executor.Spec) (*sandbox, error) {
	m, err := e.launch.boot(ctx, e.cfg, rootfs, dir)
	if err != nil {
		return nil, err
	}
	session, err := connect(ctx, m)
	if err != nil {
		_ = m.stop()
		return nil, err
	}
	return &sandbox{
		machine:  m,
		session:  session,
		dir:      dir,
		rootfs:   rootfs,
		env:      spec.Env,
		workDir:  spec.WorkDir,
		identity: &e.identity,
	}, nil
}

// grants refuses a step whose requirements this backend does not advertise. A
// backend that accepted them anyway would run the step with the wrong
// isolation and the step would never know.
func (e *Executor) grants(want []dholev1.Capability) error {
	have := e.Capabilities()
	for _, w := range want {
		if w == dholev1.Capability_CAPABILITY_UNSPECIFIED {
			continue
		}
		found := false
		for _, h := range have {
			if h == w {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("vm executor: %s: capability not advertised by this backend (it advertises %v)",
				w, have)
		}
	}
	return nil
}

// connect dials the guest agent until it answers or the boot budget runs out.
// Retrying is not a workaround: the hypervisor returns as soon as the VM is
// created, and the guest kernel has not finished booting yet, so "connection
// refused" is the expected answer for the first tens of milliseconds.
func connect(ctx context.Context, m machine) (*yamux.Session, error) {
	deadline := time.Now().Add(bootTimeout)
	var last error
	for {
		conn, err := m.dial(ctx)
		if err == nil {
			cfg := yamux.DefaultConfig()
			cfg.LogOutput = io.Discard
			session, serr := yamux.Client(conn, cfg)
			if serr == nil {
				return session, nil
			}
			_ = conn.Close()
			last = serr
		} else {
			last = err
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("vm executor: waiting for the guest agent: %w", ctx.Err())
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("vm executor: the guest agent did not answer within %s (guest console: %s): %w",
				bootTimeout, m.diagnostics(), last)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// sandbox is one running microVM.
type sandbox struct {
	machine  machine
	session  *yamux.Session
	dir      string
	rootfs   string
	env      map[string]string
	workDir  string
	identity *identityCache

	mu       sync.Mutex
	released bool
}

// EnvironmentIdentity implements executor.Sandbox: the digest of the rootfs
// snapshot THIS guest booted from.
//
// It lives here because this is where the environment is. One executor serves
// as many environments as there are snapshots its steps name, and answering
// from the executor's configured default gave two steps on two different
// rootfs images one identity — after which a content-addressed cache hands one
// step's outputs back as the other's result.
func (s *sandbox) EnvironmentIdentity() (string, error) {
	return s.identity.of(s.rootfs)
}

// Exec runs a command inside the guest and returns its exit code.
func (s *sandbox) Exec(ctx context.Context, cmd executor.Cmd) (int32, error) {
	if len(cmd.Args) == 0 {
		return 0, errors.New("vm executor: exec with no command")
	}
	stream, err := s.session.OpenStream()
	if err != nil {
		return 0, fmt.Errorf("vm executor: open an exec stream: %w", err)
	}
	defer func() { _ = stream.Close() }()
	c := vmwire.NewConn(stream)

	env := map[string]string{}
	for k, v := range s.env {
		env[k] = v
	}
	for k, v := range cmd.Env {
		env[k] = v
	}
	if err := c.WriteJSON(vmwire.FrameHeader, vmwire.Request{
		Op:   vmwire.OpExec,
		Args: cmd.Args,
		Env:  env,
		Dir:  path.Join(s.workDir, cmd.Dir),
	}); err != nil {
		return 0, fmt.Errorf("vm executor: send the exec request: %w", err)
	}

	done := make(chan struct{})
	defer close(done)
	go feedStdin(c, cmd.Stdin)
	// Cancellation travels as one frame and the GUEST does the escalation.
	// Closing the stream instead would return here immediately and leave the
	// tree dying in the background — which is precisely the leak the contract
	// checks for, since the scheduler re-dispatches a step it has cancelled.
	go func() {
		select {
		case <-ctx.Done():
			_ = c.Write(vmwire.FrameSignal, []byte(vmwire.SignalCancel))
			select {
			case <-done:
			case <-time.After(cancelGrace):
				// The guest is not answering; stop waiting for an orderly
				// exit and let the read below fail.
				_ = stream.Close()
			}
		case <-done:
		}
	}()

	var code int32
	for {
		t, payload, err := c.Read()
		if err != nil {
			if ctx.Err() != nil {
				return code, fmt.Errorf("vm executor: exec cancelled: %w", ctx.Err())
			}
			return code, fmt.Errorf("vm executor: the guest stopped answering mid-command: %w", err)
		}
		switch t {
		case vmwire.FrameStdout:
			writeTo(cmd.Stdout, payload)
		case vmwire.FrameStderr:
			writeTo(cmd.Stderr, payload)
		case vmwire.FrameExit:
			if len(payload) == 4 {
				code = int32(binary.BigEndian.Uint32(payload)) // #nosec G115 -- the guest never sends a negative code
			}
		case vmwire.FrameStatus:
			if len(payload) > 0 {
				return code, fmt.Errorf("vm executor: %s", payload)
			}
			if ctx.Err() != nil {
				return code, fmt.Errorf("vm executor: exec cancelled: %w", ctx.Err())
			}
			return code, nil
		}
	}
}

func feedStdin(c *vmwire.Conn, r io.Reader) {
	if r != nil {
		buf := make([]byte, 32*1024)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				if werr := c.Write(vmwire.FrameData, buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				break
			}
		}
	}
	_ = c.Write(vmwire.FrameEOF, nil)
}

func writeTo(w io.Writer, p []byte) {
	if w == nil {
		return
	}
	_, _ = w.Write(p)
}

// Put writes a file into the guest.
func (s *sandbox) Put(ctx context.Context, name string, r io.Reader) error {
	return s.request(ctx, vmwire.Request{Op: vmwire.OpPut, Name: name}, func(c *vmwire.Conn) error {
		buf := make([]byte, 32*1024)
		for {
			n, err := r.Read(buf)
			if n > 0 {
				if werr := c.Write(vmwire.FrameData, buf[:n]); werr != nil {
					return werr
				}
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
		}
		return c.Write(vmwire.FrameEOF, nil)
	})
}

// Get reads a file back out of the guest. The content is streamed through a
// pipe rather than buffered: an artifact is whatever size the step made it,
// and the host has no say in that.
func (s *sandbox) Get(_ context.Context, name string) (io.ReadCloser, error) {
	stream, err := s.session.OpenStream()
	if err != nil {
		return nil, fmt.Errorf("vm executor: open a get stream: %w", err)
	}
	c := vmwire.NewConn(stream)
	if err := c.WriteJSON(vmwire.FrameHeader, vmwire.Request{Op: vmwire.OpGet, Name: name}); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("vm executor: send the get request: %w", err)
	}
	if err := c.Write(vmwire.FrameEOF, nil); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("vm executor: send the get request: %w", err)
	}
	pr, pw := io.Pipe()
	go func() {
		for {
			t, payload, err := c.Read()
			if err != nil {
				_ = pw.CloseWithError(fmt.Errorf("vm executor: reading %q from the guest: %w", name, err))
				_ = stream.Close()
				return
			}
			switch t {
			case vmwire.FrameData:
				if _, werr := pw.Write(payload); werr != nil {
					_ = stream.Close()
					return
				}
			case vmwire.FrameStatus:
				if len(payload) > 0 {
					_ = pw.CloseWithError(fmt.Errorf("vm executor: %s", payload))
				} else {
					_ = pw.Close()
				}
				_ = stream.Close()
				return
			}
		}
	}()
	return pr, nil
}

// Mkdir creates a directory and its parents inside the guest.
func (s *sandbox) Mkdir(ctx context.Context, name string) error {
	return s.request(ctx, vmwire.Request{Op: vmwire.OpMkdir, Name: name}, nil)
}

// Signal delivers sig to every command the guest is running, and delivers
// exactly what was asked. Signalling an idle sandbox is a no-op.
func (s *sandbox) Signal(ctx context.Context, sig executor.Signal) error {
	return s.request(ctx, vmwire.Request{Op: vmwire.OpSignal, Signal: string(sig)}, nil)
}

// request runs one short operation on its own stream: header, optional body,
// then the guest's status.
func (s *sandbox) request(ctx context.Context, req vmwire.Request, body func(*vmwire.Conn) error) error {
	stream, err := s.session.OpenStream()
	if err != nil {
		return fmt.Errorf("vm executor: open a %s stream: %w", req.Op, err)
	}
	defer func() { _ = stream.Close() }()
	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetDeadline(deadline)
	}
	c := vmwire.NewConn(stream)
	if err := c.WriteJSON(vmwire.FrameHeader, req); err != nil {
		return fmt.Errorf("vm executor: send the %s request: %w", req.Op, err)
	}
	if body != nil {
		if err := body(c); err != nil {
			return fmt.Errorf("vm executor: %s: %w", req.Op, err)
		}
	} else if err := c.Write(vmwire.FrameEOF, nil); err != nil {
		return fmt.Errorf("vm executor: send the %s request: %w", req.Op, err)
	}
	for {
		t, payload, err := c.Read()
		if err != nil {
			return fmt.Errorf("vm executor: %s: the guest stopped answering: %w", req.Op, err)
		}
		if t == vmwire.FrameStatus {
			if len(payload) > 0 {
				return fmt.Errorf("vm executor: %s: %s", req.Op, payload)
			}
			return nil
		}
	}
}

// Release destroys the microVM. It is idempotent, so cleanup paths can be
// unconditional.
func (s *sandbox) Release(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.released {
		return nil
	}
	s.released = true
	if s.session != nil {
		_ = s.session.Close()
	}
	err := s.machine.stop()
	if s.dir != "" {
		_ = os.RemoveAll(s.dir)
	}
	return err
}

// identityCache remembers the digest of each rootfs it has hashed. A rootfs
// image is hundreds of megabytes and the identity is asked for on every cache
// lookup, so hashing it per question would make the cache slower than the work
// it saves.
//
// The cache is keyed on the path AND the file's size and modification time, so
// a rootfs rebuilt in place under a running engine is re-hashed rather than
// answered from memory. An engine that kept serving the old digest would hand
// out cache hits from an environment that no longer exists.
type identityCache struct {
	mu      sync.Mutex
	entries map[string]identityEntry
}

type identityEntry struct {
	size    int64
	modTime time.Time
	digest  string
}

func (c *identityCache) of(rootfs string) (string, error) {
	noIdentity := func(err error) (string, error) {
		return "", fmt.Errorf("vm executor: rootfs %q has no readable digest: %w: %w",
			rootfs, err, executor.ErrNoStableIdentity)
	}
	if strings.TrimSpace(rootfs) == "" {
		return noIdentity(errors.New("no rootfs image is configured"))
	}
	info, err := os.Stat(rootfs)
	if err != nil {
		return noIdentity(err)
	}
	c.mu.Lock()
	if e, ok := c.entries[rootfs]; ok && e.size == info.Size() && e.modTime.Equal(info.ModTime()) {
		c.mu.Unlock()
		return e.digest, nil
	}
	c.mu.Unlock()

	f, err := os.Open(rootfs) // #nosec G304 -- the rootfs path is operator configuration
	if err != nil {
		return noIdentity(err)
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return noIdentity(err)
	}
	digest := "sha256:" + hex.EncodeToString(h.Sum(nil))

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[string]identityEntry{}
	}
	c.entries[rootfs] = identityEntry{size: info.Size(), modTime: info.ModTime(), digest: digest}
	return digest, nil
}

// nestedVirtAvailable reports whether this host can give a GUEST a working
// hypervisor.
//
// Two things have to be true and only the first is obvious. /dev/kvm must be
// there, or there is no virtualisation at all. But a host that virtualises
// perfectly well may still refuse to nest, and on every architecture that
// refusal is spelled in the same place: the kvm module's `nested` parameter.
// Advertising the capability on the strength of /dev/kvm alone is the mistake
// this function exists to avoid — the step would be scheduled here, open
// /dev/kvm inside the guest, find nothing, and fail in a way that reads as its
// own bug.
func nestedVirtAvailable() bool {
	if _, err := os.Stat("/dev/kvm"); err != nil {
		return false
	}
	for _, param := range []string{
		"/sys/module/kvm_intel/parameters/nested",
		"/sys/module/kvm_amd/parameters/nested",
		"/sys/module/kvm/parameters/nested",
	} {
		b, err := os.ReadFile(filepath.Clean(param))
		if err != nil {
			continue
		}
		switch strings.TrimSpace(string(b)) {
		case "Y", "y", "1":
			return true
		}
	}
	return false
}
