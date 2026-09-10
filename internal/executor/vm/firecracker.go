package vm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/vm/vmwire"
)

// guestCID is the vsock context id every Dhole guest is given. It is fixed
// because host-to-guest traffic goes through the unix socket Firecracker
// creates, not through the host's vsock namespace, so the number is private to
// one microVM and nothing collides.
const guestCID = 3

// firecrackerBootArgs is the kernel command line a Dhole guest boots with.
// Every entry is there to remove something: no PCI bus to probe, no PS/2
// controller to time out on, and a panic that reboots into the hypervisor's
// arms instead of hanging with the sandbox still leased.
const firecrackerBootArgs = "console=ttyS0 reboot=k panic=1 pci=off i8042.noaux i8042.nomux i8042.nopnp i8042.dumbkbd"

type firecrackerLauncher struct{}

func (firecrackerLauncher) boot(ctx context.Context, cfg Config, rootfs, dir string) (machine, error) {
	m, err := startFirecracker(ctx, cfg, dir, "")
	if err != nil {
		return nil, err
	}
	boot := func() error {
		if err := m.api.put(ctx, "/boot-source", map[string]any{
			"kernel_image_path": cfg.KernelImage,
			"initrd_path":       rootfs,
			"boot_args":         bootArgs(cfg),
		}); err != nil {
			return err
		}
		if err := m.api.put(ctx, "/machine-config", map[string]any{
			"vcpu_count":   cfg.VCPUs,
			"mem_size_mib": cfg.MemMiB,
			"smt":          false,
		}); err != nil {
			return err
		}
		if err := m.api.put(ctx, "/vsock", map[string]any{
			"guest_cid": guestCID,
			"uds_path":  m.vsockPath,
		}); err != nil {
			return err
		}
		return m.api.put(ctx, "/actions", map[string]any{"action_type": "InstanceStart"})
	}
	if err := boot(); err != nil {
		_ = m.stop()
		return nil, fmt.Errorf("vm executor: firecracker (guest console: %s): %w", m.diagnostics(), err)
	}
	return m, nil
}

func bootArgs(cfg Config) string {
	if cfg.KernelArgs != "" {
		return cfg.KernelArgs
	}
	return firecrackerBootArgs
}

// firecrackerVM is one Firecracker process and the two sockets it owns: the
// API socket the host configures it through, and the vsock socket the guest
// agent is reached through.
type firecrackerVM struct {
	*hypervisorProcess
	api       *fcAPI
	apiPath   string
	vsockPath string
}

// sunPathMax is how long a unix socket path may be, minus the terminator.
// Exceeding it is not a graceful failure anywhere in the stack: the kernel
// truncates, the bind refers to something else, and Firecracker exits 1 with
// nothing on its console — which is what a snapshot restore nested three
// temporary directories deep actually did.
const sunPathMax = 107

// startFirecracker launches one hypervisor process. tag distinguishes the API
// socket and console of several processes sharing a directory, which restores
// from one snapshot must do: the vsock socket's path is recorded INSIDE the
// snapshot, so a restore has to recreate it exactly where the snapshotted
// guest had it and cannot be given a directory of its own.
func startFirecracker(ctx context.Context, cfg Config, dir, tag string) (*firecrackerVM, error) {
	bin := cfg.BinaryPath
	if bin == "" {
		bin = string(Firecracker)
	}
	apiPath := filepath.Join(dir, tag+"api.sock")
	vsockPath := filepath.Join(dir, "v.sock")
	for _, p := range []string{apiPath, vsockPath} {
		if len(p) > sunPathMax {
			return nil, fmt.Errorf("vm executor: %q is %d bytes, past the %d a unix socket path may be: "+
				"put the sandbox directory somewhere shorter", p, len(p), sunPathMax)
		}
	}
	proc, err := startHypervisor(ctx, bin,
		[]string{"--api-sock", apiPath, "--level", "Warn", "--id", "dhole-vm"},
		filepath.Join(dir, tag+"console.log"))
	if err != nil {
		return nil, err
	}
	m := &firecrackerVM{
		hypervisorProcess: proc,
		apiPath:           apiPath,
		vsockPath:         vsockPath,
		api:               newFCAPI(apiPath),
	}
	if err := m.waitForAPI(ctx); err != nil {
		_ = m.stop()
		return nil, fmt.Errorf("vm executor: firecracker: %w", err)
	}
	return m, nil
}

// waitForAPI blocks until the hypervisor has created its API socket, or until
// it has exited without doing so. Both are needed: polling alone would spend
// the whole boot budget on a process that died in the first millisecond.
func (m *firecrackerVM) waitForAPI(ctx context.Context) error {
	deadline := time.Now().Add(bootTimeout)
	for {
		if _, err := os.Stat(m.apiPath); err == nil {
			return nil
		}
		if err := m.exited(); err != nil {
			return fmt.Errorf("before creating %s: %w", m.apiPath, err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the hypervisor never created %s (guest console: %s)", m.apiPath, m.consolePath)
		}
		time.Sleep(200 * time.Microsecond)
	}
}

// dial reaches the guest agent through Firecracker's host-initiated vsock
// tunnel: connect to the unix socket, name the guest port, and read the
// acknowledgement. Anything else on that first line is a refusal, and it has
// to be treated as one — a tunnel that was not established but is used anyway
// feeds the handshake text to the multiplexer as if it were a frame.
func (m *firecrackerVM) dial(ctx context.Context) (io.ReadWriteCloser, error) {
	if err := m.exited(); err != nil {
		return nil, err
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", m.vsockPath)
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(conn, "CONNECT %d\n", vmwire.Port); err != nil {
		_ = conn.Close()
		return nil, err
	}
	line, err := readLine(conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if !strings.HasPrefix(line, "OK") {
		_ = conn.Close()
		return nil, fmt.Errorf("the guest refused a vsock connection on port %d: %q", vmwire.Port, line)
	}
	return conn, nil
}

// readLine reads one byte at a time on purpose. The handshake reply is
// followed immediately by the guest agent's own bytes, and a buffered reader
// would swallow the first frames of the multiplexed session into a buffer the
// caller never sees.
func readLine(r io.Reader) (string, error) {
	var line []byte
	var b [1]byte
	for {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return string(line), err
		}
		if b[0] == '\n' {
			return string(line), nil
		}
		line = append(line, b[0])
		if len(line) > 256 {
			return string(line), errors.New("the vsock handshake reply has no newline")
		}
	}
}

func (m *firecrackerVM) stop() error {
	err := m.hypervisorProcess.stop()
	// The sockets are what a restore into this directory would collide with,
	// so they go with the process that owned them.
	_ = os.Remove(m.apiPath)
	_ = os.Remove(m.vsockPath)
	return err
}

// fcAPI is Firecracker's REST API over its unix socket.
type fcAPI struct{ hc *http.Client }

func newFCAPI(sock string) *fcAPI {
	return &fcAPI{hc: &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", sock)
			},
		},
	}}
}

func (a *fcAPI) put(ctx context.Context, path string, body any) error {
	return a.do(ctx, http.MethodPut, path, body)
}

func (a *fcAPI) patch(ctx context.Context, path string, body any) error {
	return a.do(ctx, http.MethodPatch, path, body)
}

func (a *fcAPI) do(ctx context.Context, method, path string, body any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://localhost"+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.hc.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		// The API's error body names the field it rejected, and without it the
		// only symptom is a guest that never boots.
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(detail)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// Snapshotter is implemented by backends that can freeze a booted guest and
// bring it back. It is what makes a microVM per step affordable: a cold boot
// costs more than a short step, so a strong-isolation tier that could only
// cold-boot is a tier nobody would select.
type Snapshotter interface {
	// Snapshot boots a guest, waits for its agent, and freezes it.
	Snapshot(ctx context.Context) (Snapshot, error)
}

// Snapshot is a frozen guest that can be restored any number of times.
type Snapshot interface {
	// Restore brings up a guest from the snapshot and returns it as a
	// sandbox, ready to run commands.
	Restore(ctx context.Context) (executor.Sandbox, error)
	// Close removes the snapshot's files.
	Close() error
}

// Snapshot implements Snapshotter.
func (e *Executor) Snapshot(ctx context.Context) (Snapshot, error) {
	if e.cfg.Backend != Firecracker {
		return nil, fmt.Errorf("vm executor: the %s backend cannot snapshot; only %s can", e.cfg.Backend, Firecracker)
	}
	if e.cfg.SnapshotDir == "" {
		return nil, errors.New("vm executor: no SnapshotDir is configured, so there is nowhere to write a snapshot")
	}
	dir, err := os.MkdirTemp(e.cfg.SnapshotDir, "s")
	if err != nil {
		return nil, fmt.Errorf("vm executor: create the snapshot directory: %w", err)
	}
	snap := &fcSnapshot{
		exec:      e,
		dir:       dir,
		statePath: filepath.Join(dir, "state"),
		memPath:   filepath.Join(dir, "mem"),
	}
	if err := snap.create(ctx); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return snap, nil
}

type fcSnapshot struct {
	exec      *Executor
	dir       string
	statePath string
	memPath   string

	mu       sync.Mutex
	restores int
}

// create boots one guest, waits until its agent answers, and freezes it there.
// Waiting matters: a snapshot taken before the agent is listening restores to
// a guest that is still booting, which is the cold boot the snapshot was
// supposed to replace.
func (s *fcSnapshot) create(ctx context.Context) error {
	m, err := firecrackerLauncher{}.boot(ctx, s.exec.cfg, s.exec.cfg.RootfsImage, s.dir)
	if err != nil {
		return err
	}
	defer func() { _ = m.stop() }()
	session, err := connect(ctx, m)
	if err != nil {
		return err
	}
	_ = session.Close()

	fc, ok := m.(*firecrackerVM)
	if !ok {
		return errors.New("vm executor: snapshot needs a firecracker guest")
	}
	if err := fc.api.patch(ctx, "/vm", map[string]any{"state": "Paused"}); err != nil {
		return fmt.Errorf("vm executor: pause the guest: %w", err)
	}
	// Exactly these three fields. Firecracker's API rejects an unknown key
	// outright rather than ignoring it — `unknown field \`enable_diff_sn\`` —
	// so a body carrying a plausible extra is a 400, not a snapshot.
	if err := fc.api.put(ctx, "/snapshot/create", map[string]any{
		"snapshot_type": "Full",
		"snapshot_path": s.statePath,
		"mem_file_path": s.memPath,
	}); err != nil {
		return fmt.Errorf("vm executor: create the snapshot: %w", err)
	}
	return nil
}

// Restore implements Snapshot.
//
// One restore at a time. The vsock unix socket's path is part of the
// snapshotted machine state, so every restore of this snapshot binds the SAME
// path, and two of them at once is one hypervisor exiting 1 with an empty
// console. Restores are cheap enough that serialising them is not the cost
// that matters here.
func (s *fcSnapshot) Restore(ctx context.Context) (executor.Sandbox, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tag := strconv.Itoa(s.restores) + "-"
	s.restores++
	// A leftover from the previous restore would make the bind fail, and the
	// process that owned it is gone.
	_ = os.Remove(filepath.Join(s.dir, "v.sock"))

	m, err := startFirecracker(ctx, s.exec.cfg, s.dir, tag)
	if err != nil {
		return nil, err
	}
	if err := m.api.put(ctx, "/snapshot/load", map[string]any{
		"snapshot_path": s.statePath,
		"mem_backend": map[string]any{
			"backend_path": s.memPath,
			"backend_type": "File",
		},
		"resume_vm": true,
	}); err != nil {
		_ = m.stop()
		return nil, fmt.Errorf("vm executor: load the snapshot (guest console: %s): %w", m.diagnostics(), err)
	}
	session, err := connect(ctx, m)
	if err != nil {
		_ = m.stop()
		return nil, err
	}
	return &sandbox{
		machine:  m,
		session:  session,
		rootfs:   s.exec.cfg.RootfsImage,
		identity: &s.exec.identity,
	}, nil
}

// Close implements Snapshot.
func (s *fcSnapshot) Close() error { return os.RemoveAll(s.dir) }
