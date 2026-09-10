package vm

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"

	"github.com/azrtydxb/dhole/internal/executor/vm/vmwire"
)

// QEMU is the fallback, and it is a fallback rather than the default for two
// reasons that both show up per step: it boots a full machine model where
// Firecracker boots four devices, and it cannot snapshot into the sub-second
// restore the strong-isolation tier is priced around. What it has is reach —
// it runs wherever KVM does, on architectures and kernels Firecracker has
// never supported.
//
// The guest is identical either way. Same kernel, same rootfs, same agent,
// same protocol; only the machine model and the path to the vsock differ, and
// that is what lets one conformance run cover both.
type qemuLauncher struct{}

// cidSpace is the range guest context ids are drawn from. 0, 1 and 2 are
// reserved by the vsock address family for the hypervisor, a loopback and the
// host; everything above is a free-for-all shared with every other process on
// the machine, which is why allocation is a retry loop and not a counter.
const (
	cidMin = 3
	cidMax = 1 << 30
)

// cidAttempts is how many times a boot re-rolls its context id before giving
// up. Collisions are rare and independent, so a handful of attempts turns a
// visible flake into an invisible one.
const cidAttempts = 5

// takenCIDs stops this process from colliding with itself. Two sandboxes
// acquired at the same moment picked the same id often enough to matter once
// the pool executor started warming several at once, and the second one failed
// with a QEMU error message that named neither sandbox.
var takenCIDs = struct {
	mu sync.Mutex
	m  map[uint32]struct{}
}{m: map[uint32]struct{}{}}

func claimCID() (uint32, error) {
	for range cidAttempts * 4 {
		n, err := rand.Int(rand.Reader, big.NewInt(cidMax-cidMin))
		if err != nil {
			return 0, err
		}
		cid := uint32(n.Int64()) + cidMin // #nosec G115 -- cidMax is 2^30
		takenCIDs.mu.Lock()
		_, clash := takenCIDs.m[cid]
		if !clash {
			takenCIDs.m[cid] = struct{}{}
		}
		takenCIDs.mu.Unlock()
		if !clash {
			return cid, nil
		}
	}
	return 0, fmt.Errorf("vm executor: no free vsock context id after %d attempts", cidAttempts*4)
}

func releaseCID(cid uint32) {
	takenCIDs.mu.Lock()
	defer takenCIDs.mu.Unlock()
	delete(takenCIDs.m, cid)
}

func (qemuLauncher) boot(ctx context.Context, cfg Config, rootfs, dir string) (machine, error) {
	bin := cfg.BinaryPath
	if bin == "" {
		bin = "qemu-system-" + qemuArch()
	}
	var last error
	for range cidAttempts {
		cid, err := claimCID()
		if err != nil {
			return nil, err
		}
		proc, err := startHypervisor(ctx, bin, qemuArgs(cfg, rootfs, cid), filepath.Join(dir, "console.log"))
		if err != nil {
			releaseCID(cid)
			return nil, err
		}
		m := &qemuVM{hypervisorProcess: proc, cid: cid}
		// A context id already in use by another process on this host is the
		// one startup failure worth retrying, and QEMU reports it by exiting
		// rather than by refusing the device, so the only way to see it is to
		// look at whether the process is still there.
		if err := m.settle(ctx); err != nil {
			_ = m.stop()
			last = err
			continue
		}
		return m, nil
	}
	return nil, fmt.Errorf("vm executor: qemu did not start after %d attempts: %w", cidAttempts, last)
}

// qemuArch maps Go's architecture names onto QEMU's.
func qemuArch() string {
	switch runtime.GOARCH {
	case "arm64":
		return "aarch64"
	case "amd64":
		return "x86_64"
	default:
		return runtime.GOARCH
	}
}

// qemuArgs builds the command line for one guest.
//
// Every device is a virtio-mmio one — the `-device x-device` spellings rather
// than `x-pci`. That is not a style choice: the guest kernel is the same image
// Firecracker boots, and a Firecracker kernel is configured for virtio over
// MMIO with the PCI bus turned off. Asking for PCI devices here would boot a
// kernel that cannot see them, and the symptom would be a guest agent that
// never answers rather than an error.
func qemuArgs(cfg Config, rootfs string, cid uint32) []string {
	args := []string{
		"-machine", qemuMachine(),
		"-cpu", "host",
		"-smp", strconv.Itoa(cfg.VCPUs),
		"-m", strconv.Itoa(cfg.MemMiB),
		"-nodefaults",
		"-no-reboot",
		"-display", "none",
		"-kernel", cfg.KernelImage,
		"-initrd", rootfs,
		"-append", qemuBootArgs(cfg),
		"-device", fmt.Sprintf("vhost-vsock-device,guest-cid=%d", cid),
	}
	// The serial console goes to the hypervisor's stdout, which
	// startHypervisor has already pointed at the console log — the same place
	// Firecracker writes it, so a boot failure is diagnosed the same way on
	// both backends.
	return append(args, "-serial", "stdio")
}

func qemuMachine() string {
	if runtime.GOARCH == "amd64" {
		// microvm is QEMU's answer to Firecracker: no PCI, no ACPI tables to
		// parse, virtio over MMIO.
		return "microvm,accel=kvm"
	}
	return "virt,accel=kvm"
}

func qemuBootArgs(cfg Config) string {
	if cfg.KernelArgs != "" {
		return cfg.KernelArgs
	}
	if runtime.GOARCH == "amd64" {
		return firecrackerBootArgs
	}
	// The virt machine's serial port is a PL011, not an 8250.
	return "console=ttyAMA0 reboot=k panic=1 pci=off"
}

// qemuVM is one QEMU process. Unlike Firecracker there is no API socket: the
// machine is fully described by its command line, and the only channel to it
// is the guest's vsock.
type qemuVM struct {
	*hypervisorProcess
	cid      uint32
	stopOnce sync.Once
}

// settle gives QEMU long enough to fail. It has no readiness signal short of
// the guest agent answering, so the only thing worth waiting for here is the
// process NOT being gone.
func (m *qemuVM) settle(ctx context.Context) error {
	select {
	case <-m.done:
		return m.exited()
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (m *qemuVM) dial(ctx context.Context) (io.ReadWriteCloser, error) {
	if err := m.exited(); err != nil {
		return nil, err
	}
	return dialVsock(ctx, m.cid, vmwire.Port)
}

func (m *qemuVM) stop() error {
	err := m.hypervisorProcess.stop()
	m.stopOnce.Do(func() { releaseCID(m.cid) })
	return err
}
