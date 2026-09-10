//go:build linux

// Command dhole-vm-agent is the guest half of the VM executor: the init
// process of a Dhole microVM.
//
// It is a separate program from the engine on purpose. The executor and the
// step are on opposite sides of a kernel boundary — that boundary is the whole
// product of the VM backend (ADR 0006's strong-isolation tier) — so the only
// thing that crosses it is this protocol. The agent is statically linked and
// baked into the rootfs image, and the digest of that image is the sandbox's
// environment identity, which means changing this program changes the cache
// key of every step that runs under it. That is correct: it is part of the
// environment.
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/yamux"
	"golang.org/x/sys/unix"

	"github.com/azrtydxb/dhole/internal/executor/vm/vmwire"
)

// guestPath is where programs live in a Dhole guest. It is both the agent's
// own PATH and the default a command inherits, because those are two different
// lookups and a step needs both to work.
const guestPath = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

func main() {
	if os.Getpid() == 1 {
		// Nothing else will: an initramfs starts with /init and an empty
		// namespace, so a step running `id` or `sleep` finds no /proc and no
		// /dev unless this happens first.
		mountGuestFilesystems()
	}
	// The agent's OWN environment, not the command's. os/exec resolves a bare
	// program name against the PATH of the process doing the exec, and an
	// init started by the kernel has no environment at all — so every command
	// in the contract failed with `exec: "sh": executable file not found in
	// $PATH` while /bin/sh sat there in the rootfs.
	if err := os.Setenv("PATH", guestPath); err != nil {
		fatal(fmt.Errorf("set the guest PATH: %w", err))
	}
	if err := os.MkdirAll(vmwire.SandboxRoot, 0o755); err != nil {
		fatal(fmt.Errorf("create the sandbox root: %w", err))
	}
	if err := serve(); err != nil {
		fatal(err)
	}
}

// fatal reports and then parks rather than exiting. A PID 1 that exits panics
// the guest kernel, and "Kernel panic - not syncing: Attempted to kill init"
// on a serial log is a far worse diagnostic than the error that caused it.
func fatal(err error) {
	fmt.Fprintf(os.Stderr, "dhole-vm-agent: %v\n", err)
	if os.Getpid() != 1 {
		os.Exit(1)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func mountGuestFilesystems() {
	type mount struct{ source, target, fstype string }
	for _, m := range []mount{
		{"proc", "/proc", "proc"},
		{"sysfs", "/sys", "sysfs"},
		{"devtmpfs", "/dev", "devtmpfs"},
		{"tmpfs", "/tmp", "tmpfs"},
	} {
		if err := os.MkdirAll(m.target, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "dhole-vm-agent: mkdir %s: %v\n", m.target, err)
			continue
		}
		// EBUSY means the kernel mounted it for us, which is a success with a
		// different spelling.
		if err := unix.Mount(m.source, m.target, m.fstype, 0, ""); err != nil && !errors.Is(err, unix.EBUSY) {
			fmt.Fprintf(os.Stderr, "dhole-vm-agent: mount %s on %s: %v\n", m.fstype, m.target, err)
		}
	}
}

// serve accepts host connections on vsock forever.
//
// Forever, and not once: a snapshot restore resumes a guest whose previous
// host connection died with the firecracker process that took the snapshot.
// An agent that served one connection would come back from a restore deaf,
// and the restore would look like a boot timeout.
func serve() error {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return fmt.Errorf("vsock socket: %w", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrVM{CID: unix.VMADDR_CID_ANY, Port: vmwire.Port}); err != nil {
		return fmt.Errorf("bind vsock port %d: %w", vmwire.Port, err)
	}
	if err := unix.Listen(fd, 16); err != nil {
		return fmt.Errorf("listen on vsock: %w", err)
	}
	for {
		nfd, _, err := unix.Accept(fd)
		if err != nil {
			if errors.Is(err, unix.EINTR) {
				continue
			}
			return fmt.Errorf("accept on vsock: %w", err)
		}
		go func() {
			if err := session(nfd); err != nil {
				fmt.Fprintf(os.Stderr, "dhole-vm-agent: session: %v\n", err)
			}
		}()
	}
}

// session serves one host connection. Every request is its own multiplexed
// stream, because the host reads files out of the sandbox while a command is
// still running in it.
func session(fd int) error {
	// Non-blocking, so *os.File hands the socket to the runtime poller
	// instead of parking an OS thread per concurrent stream.
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("set the vsock connection non-blocking: %w", err)
	}
	conn := os.NewFile(uintptr(fd), "vsock")
	defer func() { _ = conn.Close() }()

	cfg := yamux.DefaultConfig()
	cfg.LogOutput = io.Discard
	sess, err := yamux.Server(conn, cfg)
	if err != nil {
		return fmt.Errorf("start the multiplexer: %w", err)
	}
	defer func() { _ = sess.Close() }()

	a := &agent{running: map[int]struct{}{}}
	for {
		stream, err := sess.Accept()
		if err != nil {
			return nil // the host went away; the next connection is a new session
		}
		go a.handle(vmwire.NewConn(stream))
	}
}

type agent struct {
	mu sync.Mutex
	// running holds the process-group id of every command in flight, so a
	// sandbox-wide signal reaches all of them. A signal aimed at one pid would
	// miss everything the command spawned, which is the case the executor
	// contract exists to catch.
	running map[int]struct{}
}

func (a *agent) handle(c *vmwire.Conn) {
	defer func() { _ = c.Close() }()
	req, err := c.ReadRequest()
	if err != nil {
		return
	}
	switch req.Op {
	case vmwire.OpExec:
		err = a.execute(c, req)
	case vmwire.OpPut:
		err = a.put(c, req)
	case vmwire.OpGet:
		err = a.get(c, req)
	case vmwire.OpMkdir:
		err = a.mkdir(req)
	case vmwire.OpSignal:
		err = a.signalAll(req.Signal)
	default:
		err = fmt.Errorf("unknown operation %q", req.Op)
	}
	_ = c.WriteStatus(err)
}

func (a *agent) execute(c *vmwire.Conn, req vmwire.Request) error {
	if len(req.Args) == 0 {
		return errors.New("exec with no command")
	}
	dir, err := resolve(req.Dir)
	if err != nil {
		return err
	}
	cmd := exec.Command(req.Args[0], req.Args[1:]...) // #nosec G204 -- running the caller's command IS the job
	cmd.Dir = dir
	cmd.Env = environ(req.Env)
	// Its own process group, so everything the command spawns can be signalled
	// as a unit. Without it a backend signals the shell and leaves the
	// grandchild running on the far side of a step the scheduler believes is
	// over.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("stdin pipe: %w", err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdinR, outW, errW

	if err := cmd.Start(); err != nil {
		for _, f := range []*os.File{stdinR, stdinW, outR, outW, errR, errW} {
			_ = f.Close()
		}
		return fmt.Errorf("start %q: %w", req.Args[0], err)
	}
	// The write ends belong to the child now. Holding a copy here would keep
	// the pipes open forever and the output pump would never see EOF.
	for _, f := range []*os.File{stdinR, outW, errW} {
		_ = f.Close()
	}

	pgid := cmd.Process.Pid
	a.track(pgid)
	defer a.untrack(pgid)

	var pumps sync.WaitGroup
	pumps.Add(2)
	go pump(&pumps, c, vmwire.FrameStdout, outR)
	go pump(&pumps, c, vmwire.FrameStderr, errR)

	done := make(chan struct{})
	go a.control(c, pgid, stdinW, done)

	waitErr := cmd.Wait()
	// Not before the pumps drain. A grandchild that outlived its parent still
	// holds the inherited pipe, and reporting the step finished while it is
	// still writing is exactly the leak the contract looks for.
	pumps.Wait()
	close(done)
	_ = stdinW.Close()

	var code [4]byte
	binary.BigEndian.PutUint32(code[:], uint32(exitCode(cmd.ProcessState, waitErr))) // #nosec G115 -- exit codes are small and non-negative
	return c.Write(vmwire.FrameExit, code[:])
}

// control carries stdin, signals and cancellation towards the command.
//
// The stream closing IS cancellation: the host closes it when the step's
// context is cancelled, and that has to kill the tree rather than merely
// return, because the scheduler re-dispatches a cancelled step and two copies
// of one step running at once is the failure this prevents.
func (a *agent) control(c *vmwire.Conn, pgid int, stdin *os.File, done <-chan struct{}) {
	for {
		t, payload, err := c.Read()
		if err != nil {
			select {
			case <-done:
				// The command finished on its own; the stream is closing
				// behind it and there is nothing left to kill.
			default:
				terminateGroup(pgid)
			}
			return
		}
		switch t {
		case vmwire.FrameData:
			_, _ = stdin.Write(payload)
		case vmwire.FrameEOF:
			_ = stdin.Close()
		case vmwire.FrameSignal:
			// Cancellation is not a signal the caller chose, so it does not
			// map to one: it means "this step is over", and the guest decides
			// how to make that true. Doing the escalation HERE rather than as
			// two host round trips is what keeps a cancelled step inside the
			// scheduler's window.
			if string(payload) == vmwire.SignalCancel {
				terminateGroup(pgid)
				continue
			}
			signalGroup(pgid, string(payload))
		}
	}
}

func pump(wg *sync.WaitGroup, c *vmwire.Conn, t vmwire.FrameType, r *os.File) {
	defer wg.Done()
	defer func() { _ = r.Close() }()
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if werr := c.Write(t, buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (a *agent) put(c *vmwire.Conn, req vmwire.Request) error {
	path, err := resolve(req.Name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create the parents of %q: %w", req.Name, err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644) // #nosec G304 -- resolve() confined it to the sandbox root
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	for {
		t, payload, err := c.Read()
		if err != nil {
			return fmt.Errorf("read the content of %q: %w", req.Name, err)
		}
		switch t {
		case vmwire.FrameData:
			if _, err := f.Write(payload); err != nil {
				return err
			}
		case vmwire.FrameEOF:
			return f.Close()
		}
	}
}

func (a *agent) get(c *vmwire.Conn, req vmwire.Request) error {
	path, err := resolve(req.Name)
	if err != nil {
		return err
	}
	f, err := os.Open(path) // #nosec G304 -- resolve() confined it to the sandbox root
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, 32*1024)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if werr := c.Write(vmwire.FrameData, buf[:n]); werr != nil {
				return werr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (a *agent) mkdir(req vmwire.Request) error {
	path, err := resolve(req.Name)
	if err != nil {
		return err
	}
	return os.MkdirAll(path, 0o755)
}

func (a *agent) signalAll(name string) error {
	a.mu.Lock()
	groups := make([]int, 0, len(a.running))
	for pgid := range a.running {
		groups = append(groups, pgid)
	}
	a.mu.Unlock()
	for _, pgid := range groups {
		signalGroup(pgid, name)
	}
	return nil
}

func (a *agent) track(pgid int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.running[pgid] = struct{}{}
}

func (a *agent) untrack(pgid int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.running, pgid)
}

// signalGroup delivers exactly what was asked to the whole process group.
// Exactly: a sandbox-wide Signal is the operator's door, and an operator who
// asked for TERM and got KILL was lied to.
func signalGroup(pgid int, name string) {
	var sig syscall.Signal
	switch name {
	case "INT":
		sig = unix.SIGINT
	case "TERM":
		sig = unix.SIGTERM
	case "KILL":
		sig = unix.SIGKILL
	default:
		return
	}
	_ = unix.Kill(-pgid, sig)
}

// terminateGroup is what cancellation does: ask, then insist.
//
// Asking alone is not enough — the contract's orphan traps SIGTERM precisely
// because a real build tool does — and insisting alone would deny every step
// the chance to clean up. The grace period is short because it is spent inside
// the guest, where a round trip costs nothing, and the whole cancellation has
// to fit in the scheduler's window.
func terminateGroup(pgid int) {
	_ = unix.Kill(-pgid, unix.SIGTERM)
	time.Sleep(250 * time.Millisecond)
	_ = unix.Kill(-pgid, unix.SIGKILL)
}

// exitCode collapses every way a command can end into the one number
// docs/wire-contract.md documents. A signalled command reports 128+signal, so
// a SIGKILL — an OOM kill, a cancellation, an operator — is 137 here exactly
// as it is on every other backend.
func exitCode(state *os.ProcessState, waitErr error) int32 {
	if state == nil {
		if waitErr != nil {
			return 1
		}
		return 0
	}
	if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return int32(128 + status.Signal()) // #nosec G115 -- signal numbers are below 64
	}
	code := state.ExitCode()
	if code < 0 {
		// Never negative on the wire: a negative int32 sign-extends to ten
		// bytes as a varint and hangs the decoder on the far side.
		return 1
	}
	return int32(code)
}

// resolve turns a sandbox-relative name into an absolute guest path that
// cannot escape the sandbox root. The step is the untrusted party here, and
// `../../etc/passwd` is the first thing it will try.
func resolve(name string) (string, error) {
	clean := filepath.Clean("/" + strings.TrimSpace(name))
	path := filepath.Join(vmwire.SandboxRoot, clean)
	if path != vmwire.SandboxRoot && !strings.HasPrefix(path, vmwire.SandboxRoot+string(filepath.Separator)) {
		return "", fmt.Errorf("%q escapes the sandbox root", name)
	}
	return path, nil
}

// environ merges the request's variables over a minimal base. The base exists
// because an initramfs gives a process no environment at all, and `sh -c` with
// no PATH cannot find `sleep`.
func environ(extra map[string]string) []string {
	env := map[string]string{
		"PATH": guestPath,
		"HOME": "/root",
	}
	for k, v := range extra {
		env[k] = v
	}
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	return out
}
