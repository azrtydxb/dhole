package vm_test

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	dholev1 "github.com/azrtydxb/dhole/gen/dhole/v1"
	"github.com/azrtydxb/dhole/internal/executor"
	"github.com/azrtydxb/dhole/internal/executor/executortest"
	"github.com/azrtydxb/dhole/internal/executor/vm"
)

// executorContract is the conformance suite every executor backend must pass.
func executorContract(t *testing.T, e executor.Executor) {
	t.Helper()
	executortest.Contract(t, e)
}

// hypervisor returns the configuration for a real microVM, or skips naming
// exactly what is missing.
//
// There is deliberately no emulated stand-in behind this. A fake hypervisor
// never boots a kernel, never crosses the guest boundary and never reaps a
// guest process, so nothing this contract asserts about exec, signal
// propagation, cancellation or exit codes would be true of a microVM. The
// whole claim of ADR 0006 — that the executor interface fits a VM as a
// first-class citizen rather than as a container-shaped fake — is only tested
// against a hypervisor with hardware acceleration.
func hypervisor(t *testing.T, backend vm.Backend, binEnv string) vm.Config {
	t.Helper()
	bin := os.Getenv(binEnv)
	if bin == "" {
		t.Skipf("%s is unset: point it at a %s binary on a Linux host with /dev/kvm "+
			"to run the VM executor tests, together with DHOLE_TEST_VM_KERNEL and "+
			"DHOLE_TEST_VM_ROOTFS (see docs/executors/vm.md for how the two are built)",
			binEnv, backend)
	}
	kernel := os.Getenv("DHOLE_TEST_VM_KERNEL")
	rootfs := os.Getenv("DHOLE_TEST_VM_ROOTFS")
	if kernel == "" || rootfs == "" {
		t.Skipf("%s is set but DHOLE_TEST_VM_KERNEL (%q) and DHOLE_TEST_VM_ROOTFS (%q) are not: "+
			"a microVM needs a kernel and a rootfs carrying the guest agent", binEnv, kernel, rootfs)
	}
	if _, err := os.Stat("/dev/kvm"); err != nil {
		t.Skipf("/dev/kvm is not available (%v): this suite refuses to run under emulation, "+
			"because a TCG guest measures nothing the scheduler relies on", err)
	}
	return vm.Config{
		Backend:     backend,
		BinaryPath:  bin,
		KernelImage: kernel,
		RootfsImage: rootfs,
		SnapshotDir: t.TempDir(),
	}
}

func newExecutor(t *testing.T, cfg vm.Config) executor.Executor {
	t.Helper()
	e, err := vm.New(cfg)
	require.NoError(t, err)
	return e
}

// TestVMExecutorContract holds the microVM engine to exactly the contract the
// process, containerd and Kubernetes engines pass, against a real hypervisor.
//
// ADR 0006's claim is that the executor interface was not shaped around
// containers — that a VM is a first-class backend rather than a container
// pretending. A fourth backend passing the same suite unchanged, with a kernel
// boundary between the executor and the step, is where that claim is either
// true or is not.
func TestVMExecutorContract(t *testing.T) {
	executorContract(t, newExecutor(t, hypervisor(t, vm.Firecracker, "DHOLE_TEST_FIRECRACKER_BIN")))
}

// TestQEMUExecutorContract is the same contract against the portable fallback.
// Firecracker exists on linux/amd64 and linux/arm64 and nowhere else; QEMU is
// what a host without it runs, and a fallback nobody holds to the contract is
// a fallback that silently behaves differently on the day it is used.
//
// It has NOT yet been made to pass, and the reason is the guest image rather
// than the backend. QEMU launches from these arguments and the guest resets
// before printing a line — the Firecracker CI kernel does not boot on QEMU's
// machine models, with or without KVM, with or without earlycon. A stock
// distribution kernel boots the same initramfs and runs the agent, and then
// the agent cannot open a vsock socket because that kernel ships vsock as a
// loadable module the initramfs does not carry. What this backend needs is a
// guest kernel with virtio-vsock built IN, and building one was out of scope
// for the task that wrote this. Until there is one, setting
// DHOLE_TEST_QEMU_BIN produces a real failure rather than a real pass, and CI
// deliberately leaves it unset.
func TestQEMUExecutorContract(t *testing.T) {
	executorContract(t, newExecutor(t, hypervisor(t, vm.QEMU, "DHOLE_TEST_QEMU_BIN")))
}

// TestEnvironmentIdentityIsSnapshotDigest. A microVM's environment is the
// rootfs snapshot it booted from, so that snapshot's content digest is the
// only honest cache key input: two steps on different snapshots ran two
// different computations and must not share a key, and two steps on the same
// snapshot ran the same one (ADR 0021).
//
// It needs no hypervisor, because resolution is all it exercises — the same
// reason the containerd backend's digest test runs everywhere the rest of its
// suite skips.
func TestEnvironmentIdentityIsSnapshotDigest(t *testing.T) {
	dir := t.TempDir()
	rootfs := filepath.Join(dir, "rootfs.img")
	require.NoError(t, os.WriteFile(rootfs, []byte("the environment as it was"), 0o600))

	identityOf := func(path string) string {
		e := newExecutor(t, vm.Config{
			Backend:     vm.Firecracker,
			BinaryPath:  "/nonexistent/firecracker",
			KernelImage: filepath.Join(dir, "vmlinux"),
			RootfsImage: path,
		})
		id, err := e.EnvironmentIdentity()
		require.NoError(t, err)
		return id
	}

	before := identityOf(rootfs)
	require.Contains(t, before, "sha256:", "the identity must be a content digest")
	require.Equal(t, before, identityOf(rootfs),
		"an identity that changes while the snapshot does not describes nothing")

	// The same path, different bytes: exactly the case a path-keyed cache gets
	// wrong. A rootfs is rebuilt in place far more often than a container
	// image tag is republished, so keying on the name would serve a step built
	// against yesterday's toolchain as today's result.
	require.NoError(t, os.WriteFile(rootfs, []byte("the environment as it is now"), 0o600))
	require.NotEqual(t, before, identityOf(rootfs),
		"the identity did not change when the snapshot content did: it is keyed on the path")

	other := filepath.Join(dir, "other.img")
	require.NoError(t, os.WriteFile(other, []byte("a third environment"), 0o600))
	require.NotEqual(t, identityOf(rootfs), identityOf(other),
		"two different snapshots must not share a cache key")
}

// TestEnvironmentIdentityWithoutAReadableRootfsIsNoStableIdentity. Honesty in
// the other direction: an identity that cannot be computed is absent, never
// invented, and internal/cache then declines to cache rather than caching
// against a key that quietly means nothing.
func TestEnvironmentIdentityWithoutAReadableRootfsIsNoStableIdentity(t *testing.T) {
	e := newExecutor(t, vm.Config{
		Backend:     vm.Firecracker,
		BinaryPath:  "/nonexistent/firecracker",
		KernelImage: "/nonexistent/vmlinux",
		RootfsImage: "/nonexistent/rootfs.img",
	})
	id, err := e.EnvironmentIdentity()
	require.Empty(t, id)
	require.ErrorIs(t, err, executor.ErrNoStableIdentity)
}

// TestNestedVirtCapabilityIsAdvertisedOnlyWhenAvailable. A backend that
// advertises a capability it cannot enforce gets sent the work that needs it,
// and that work fails inside the sandbox where it reads as the step's own bug.
// Nested virtualisation is the sharpest case for a VM backend, because it is
// the one capability a microVM looks like it ought to have: it is running on a
// hypervisor already, so the promise is plausible right up until the guest
// opens /dev/kvm and finds nothing there.
func TestNestedVirtCapabilityIsAdvertisedOnlyWhenAvailable(t *testing.T) {
	e := newExecutor(t, vm.Config{
		Backend:     vm.Firecracker,
		BinaryPath:  "/nonexistent/firecracker",
		KernelImage: "/nonexistent/vmlinux",
		RootfsImage: "/nonexistent/rootfs.img",
	})
	caps := e.Capabilities()

	if _, err := os.Stat("/dev/kvm"); err != nil {
		require.NotContains(t, caps, dholev1.Capability_CAPABILITY_NESTED_VIRT,
			"a host with no /dev/kvm (%v) cannot grant nested virtualisation to a guest", err)
		return
	}
	if !contains(caps, dholev1.Capability_CAPABILITY_NESTED_VIRT) {
		return
	}
	// Advertised: then the thing it is a promise about has to be there. A
	// guest cannot be given a hypervisor the host does not expose.
	f, err := os.Open("/dev/kvm")
	require.NoError(t, err, "NESTED_VIRT is advertised on a host whose /dev/kvm cannot be opened")
	require.NoError(t, f.Close())
}

// TestACapabilityTheBackendDoesNotAdvertiseIsRefusedBeforeAnyVMBoots. A
// backend that accepts work whose requirements it cannot meet runs that work
// with the wrong isolation, and the failure is silent — the step believes it
// got what it asked for.
//
// The hypervisor path here does not exist on purpose: the refusal must come
// from the capability check and not from a binary that happened to be missing,
// so the assertion is that Acquire says "capability not advertised" rather
// than anything about firecracker.
func TestACapabilityTheBackendDoesNotAdvertiseIsRefusedBeforeAnyVMBoots(t *testing.T) {
	e := newExecutor(t, vm.Config{
		Backend:     vm.Firecracker,
		BinaryPath:  "/nonexistent/firecracker",
		KernelImage: "/nonexistent/vmlinux",
		RootfsImage: "/nonexistent/rootfs.img",
	})
	require.NotContains(t, e.Capabilities(), dholev1.Capability_CAPABILITY_HOST_MOUNT,
		"a microVM sees no host filesystem, and must not claim otherwise")

	sb, err := e.Acquire(t.Context(), executor.Spec{
		Requirements: executor.Requirements{
			Capabilities: []dholev1.Capability{dholev1.Capability_CAPABILITY_HOST_MOUNT},
		},
	})
	require.Nil(t, sb)
	require.Error(t, err)
	require.Contains(t, err.Error(), "capability not advertised")
}

// TestASandboxOnASecondRootfsNamesThatRootfsAndNotTheDefault. The identity a
// cache key is hashed against comes from the SANDBOX, because a backend serves
// more than one environment: one executor asked about two steps booted from
// two different snapshots answered with its configured default both times, and
// a content-addressed cache that collides hands one step's outputs back as
// another step's result (ADR 0021).
//
// The second rootfs is the default with an extra cpio member concatenated on
// to it — different bytes, and still a bootable initramfs, because the kernel
// concatenates cpio archives. That matters: an identity the guest could not
// actually boot would be a label rather than a key, so the test runs a command
// in the sandbox it asked about.
func TestASandboxOnASecondRootfsNamesThatRootfsAndNotTheDefault(t *testing.T) {
	cfg := hypervisor(t, vm.Firecracker, "DHOLE_TEST_FIRECRACKER_BIN")
	e := newExecutor(t, cfg)

	second := filepath.Join(t.TempDir(), "rootfs.cpio.gz")
	require.NoError(t, os.WriteFile(second, appendCPIOTrailer(t, cfg.RootfsImage), 0o600))

	sb, err := e.Acquire(t.Context(), executor.Spec{Lease: executor.LeaseStep, Image: second})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sb.Release(t.Context())) })

	id, err := sb.EnvironmentIdentity()
	require.NoError(t, err)
	base, err := e.EnvironmentIdentity()
	require.NoError(t, err)
	require.NotEqual(t, base, id,
		"the sandbox reported the executor's default rootfs, not the one it booted: "+
			"two steps on two snapshots would share one cache key")

	// And the sandbox must actually be running that rootfs, not merely
	// reporting it: an identity nothing booted is a label, not a key.
	code, err := sb.Exec(t.Context(), executor.Cmd{Args: []string{"sh", "-c", "exit 0"}})
	require.NoError(t, err)
	require.Equal(t, int32(0), code)
}

// appendCPIOTrailer returns the initramfs at path with a second, empty cpio
// archive gzipped on to the end of it. The Linux initramfs loader accepts
// concatenated archives and concatenated gzip streams, so the result boots
// identically and hashes differently — which is exactly the fixture an
// identity test needs and a plain byte-for-byte copy cannot be.
func appendCPIOTrailer(t *testing.T, path string) []byte {
	t.Helper()
	original, err := os.ReadFile(path) //nolint:gosec // a path this test was handed
	require.NoError(t, err)

	// A newc trailer record: a 110-byte ASCII header, the name TRAILER!!! and
	// its NUL, padded to a four-byte boundary.
	const trailerName = "TRAILER!!!\x00"
	// Magic, then eleven zeroed fields (ino, mode, uid, gid, nlink, mtime,
	// filesize, devmajor, devminor, rdevmajor, rdevminor), then the name
	// length and an unused checksum.
	record := []byte("070701" + strings.Repeat("00000000", 11) + "0000000B" + "00000000" + trailerName)
	for len(record)%4 != 0 {
		record = append(record, 0)
	}

	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	_, err = w.Write(record)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return append(original, gz.Bytes()...)
}

// TestSnapshotRestoreBootsUnderOneHundredMilliseconds. Snapshot restore is the
// entire reason a microVM is affordable per step: a cold boot costs more than
// the step it isolates for anything short-running, and a strong-isolation tier
// nobody can afford is a tier nobody selects.
//
// Twenty iterations rather than one, because a single restore that happened to
// land while the page cache was warm proves nothing about the twentieth.
func TestSnapshotRestoreBootsUnderOneHundredMilliseconds(t *testing.T) {
	e := newExecutor(t, hypervisor(t, vm.Firecracker, "DHOLE_TEST_FIRECRACKER_BIN"))
	restorer, ok := e.(vm.Snapshotter)
	require.True(t, ok, "the firecracker backend must expose snapshot restore")

	snap, err := restorer.Snapshot(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, snap.Close()) })

	for i := range 20 {
		start := time.Now()
		sb, err := snap.Restore(t.Context())
		require.NoError(t, err, "restore %d", i)
		elapsed := time.Since(start)
		require.NoError(t, sb.Release(t.Context()))
		require.Less(t, elapsed, 100*time.Millisecond,
			"restore %d reached the guest agent in %s: a per-step microVM has to be cheaper than the step", i, elapsed)
	}
}

func contains(caps []dholev1.Capability, want dholev1.Capability) bool {
	for _, c := range caps {
		if c == want {
			return true
		}
	}
	return false
}
