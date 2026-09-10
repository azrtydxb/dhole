package vm

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQEMUIsLaunchedAsAnMMIOMachineWithNoPCIBus.
//
// This is the one thing about the QEMU command line that is not obvious and
// not forgiving. The guest kernel is the same image Firecracker boots, and a
// Firecracker kernel is configured for virtio over MMIO with the PCI bus
// turned off — so `vhost-vsock-pci`, which is the spelling every QEMU example
// on the internet uses, produces a guest that boots fine and never sees its
// vsock device. The symptom is a guest agent that never answers, thirty
// seconds later, with nothing in the console log to say why.
//
// It is a unit test rather than a boot because it is an assertion about a
// command line. What it CANNOT stand in for is the boot itself: see the note
// on TestQEMUExecutorContract.
func TestQEMUIsLaunchedAsAnMMIOMachineWithNoPCIBus(t *testing.T) {
	args := qemuArgs(Config{VCPUs: 2, MemMiB: 512, KernelImage: "/k/vmlinux"}, "/k/rootfs.cpio.gz", 4242)
	line := strings.Join(args, " ")

	require.Contains(t, line, "vhost-vsock-device,guest-cid=4242",
		"the vsock device must be the virtio-mmio one; the -pci spelling is invisible to a Firecracker kernel")
	require.NotContains(t, line, "vhost-vsock-pci")
	require.Contains(t, line, "accel=kvm", "the VM backend is the strong-isolation tier, not an emulator")
	require.Contains(t, line, "-kernel /k/vmlinux")
	require.Contains(t, line, "-initrd /k/rootfs.cpio.gz")
	require.Contains(t, line, "-smp 2")
	require.Contains(t, line, "-m 512")
}

// TestQEMUKernelArgsAreReplacedWholeWhenConfigured. An operator who supplies a
// kernel command line gets THAT command line: a backend that appended its own
// defaults would silently re-add the console the operator moved.
func TestQEMUKernelArgsAreReplacedWholeWhenConfigured(t *testing.T) {
	args := qemuArgs(Config{KernelArgs: "console=hvc0 quiet"}, "rootfs", 3)
	require.Contains(t, args, "console=hvc0 quiet")
	for _, a := range args {
		require.NotContains(t, a, "ttyAMA0")
	}
}

// TestAContextIdIsNeverHandedOutTwice. Two sandboxes acquired at the same
// moment picked the same context id often enough to matter once anything
// warmed several at once, and the loser failed with a QEMU error naming
// neither sandbox.
func TestAContextIdIsNeverHandedOutTwice(t *testing.T) {
	seen := map[uint32]struct{}{}
	for range 200 {
		cid, err := claimCID()
		require.NoError(t, err)
		require.GreaterOrEqual(t, cid, uint32(cidMin), "0, 1 and 2 are reserved by the address family")
		_, dup := seen[cid]
		require.False(t, dup, "context id %d was handed out twice", cid)
		seen[cid] = struct{}{}
	}
	for cid := range seen {
		releaseCID(cid)
	}
	// Released ids come back, or a long-lived engine runs out of them.
	cid, err := claimCID()
	require.NoError(t, err)
	releaseCID(cid)
}
