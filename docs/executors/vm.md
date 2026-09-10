# The `vm` executor

`kind: vm` — steps run in a microVM, behind a guest kernel.
`internal/executor/vm`.

This is the strong-isolation tier. Every other backend separates a step from
its neighbours with namespaces and cgroups, which is a boundary inside one
kernel; this one gives the step a kernel of its own. That is the whole
difference, and it is why the tier exists: a step you would not run beside your
control plane can run here.

Nothing in the executor interface changed to admit it, which is the claim [ADR
0006](../../.procoder/adr/0006-executors-are-pluggable-and-sandbox-lifetime-is-an-explicit.md)
made when it refused to shape that interface around Docker. `Acquire` names no
image layer, no mount and no namespace, so a guest kernel fits where a
container did. `Spec.Image`, which the container backends read as an image
reference, is read here as the rootfs snapshot a particular step should boot
from.

## Two hypervisors, one guest

| Backend                | When                                                         |
| ---------------------- | ------------------------------------------------------------ |
| `firecracker` (default) | linux/amd64 and linux/arm64 with `/dev/kvm` — the one worth having |
| `qemu`                  | the portable fallback, wherever KVM runs and Firecracker does not |

The guest is identical either way: same kernel, same rootfs, same agent, same
protocol. Only the machine model and the route to the guest's vsock differ,
which is what lets one conformance run cover both. Both are held to
`internal/executor/executortest.Contract` — a fallback nobody tested is a
fallback that behaves differently on the day it is used.

QEMU is a fallback rather than a peer for two reasons that are paid per step:
it boots a full machine model where Firecracker boots four devices, and it has
no snapshot restore in Dhole, so every sandbox is a cold boot.

One caveat, and it is a real one: the QEMU path has been written and its
command line is unit-tested, but the contract has **not** been run against it —
see "Building a rootfs" below for exactly what is missing.

## Configuration

| Field         | Meaning                                                                    |
| ------------- | -------------------------------------------------------------------------- |
| `Backend`     | `firecracker` or `qemu`; empty means `firecracker`                          |
| `BinaryPath`  | the hypervisor executable; empty means PATH                                 |
| `KernelImage` | the guest kernel every sandbox boots                                        |
| `RootfsImage` | the default rootfs snapshot — an initramfs whose init is the guest agent    |
| `SnapshotDir` | where snapshots are written; empty disables snapshotting                    |
| `VCPUs`       | guest vcpus; zero means 1                                                   |
| `MemMiB`      | guest memory; zero means 256                                                |
| `KernelArgs`  | replaces the default kernel command line                                    |

As engine environment variables: `DHOLE_EXECUTOR=vm`, `DHOLE_VM_BACKEND`,
`DHOLE_VM_HYPERVISOR`, `DHOLE_VM_KERNEL`, `DHOLE_VM_ROOTFS`,
`DHOLE_VM_SNAPSHOT_DIR`, `DHOLE_VM_VCPUS`, `DHOLE_VM_MEM_MIB`.

## The guest agent

The executor and the step are on opposite sides of a kernel boundary, so the
only thing that crosses it is one protocol (`internal/executor/vm/vmwire`)
spoken to a static binary that is the guest's init
(`internal/executor/vm/agent`). It mounts `/proc`, `/sys`, `/dev` and `/tmp`,
serves `/dhole/work` as the sandbox root, and answers on vsock port 1024.

The protocol is multiplexed, and that is not decoration: the executor contract
reads a file out of the sandbox **while** a command is running in it — that is
how it proves a cancelled step left no grandchild behind — so a channel that
carried one request at a time would deadlock rather than fail.

Every command runs in its own process group. Cancellation travels as a single
frame and the escalation (`SIGTERM`, a short grace, `SIGKILL`) happens inside
the guest, where a round trip costs nothing. A sandbox-wide `Signal` delivers
exactly what was asked and never escalates: that door is the operator's, and an
operator who asked for `TERM` and got `KILL` was lied to.

### Building a rootfs

The rootfs is an initramfs (`cpio`, gzipped) with the agent as `/init` and a
POSIX shell on the `PATH`:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o root/init ./internal/executor/vm/agent
install -D /usr/bin/busybox root/bin/busybox        # a STATIC busybox
for a in sh sleep id printf cat ls seq env mkdir rm; do ln -s /bin/busybox root/bin/$a; done
mkdir -p root/{proc,sys,dev,tmp,dhole/work}
( cd root && find . | cpio -o -H newc --quiet | gzip -1 > ../rootfs.cpio.gz )
```

A dynamically linked busybox in an initramfs with no libc fails as
`exec: "sh": executable file not found in $PATH`, which reads as a missing
program rather than a missing loader. It has to be the static one.

The kernel is any Linux image with virtio-mmio, virtio-vsock, devtmpfs and
initramfs support. **vsock has to be built in, not a module**: the agent is the
init, so nothing has loaded a module by the time it needs the socket.

The Firecracker CI kernels
(`https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.11/<arch>/vmlinux-6.1.102`)
are built for exactly this and are what the Firecracker backend is tested
against. They do **not** boot under QEMU's `virt` machine — the guest resets
before printing a line, with or without KVM, with or without `earlycon` — and a
stock distribution kernel that does boot ships vsock as a module. So the QEMU
backend currently has no guest image to run against, and
`TestQEMUExecutorContract` is unrun rather than passing. Supplying a kernel
with `CONFIG_VIRTIO_VSOCKETS=y` is the whole of what it is waiting for.

## Environment identity is the rootfs digest

`EnvironmentIdentity` is `sha256:` over the **content** of the rootfs snapshot,
not its path. A rootfs is rebuilt in place far more often than a container
image tag is republished — that is what a golden-image pipeline does — so
keying on the filename would serve a step compiled against yesterday's
toolchain as today's answer, which is the stale hit [ADR
0021](../../.procoder/adr/0021-the-environment-identity-behind-a-cache-key-comes-from-the-engines.md)
exists to prevent. The digest is cached against the file's size and
modification time, so a rootfs replaced under a running engine is re-hashed.

A rootfs that cannot be read yields `ErrNoStableIdentity` and its steps stay
uncached, which is the safe direction to be wrong in.

The **sandbox** answers for the snapshot it actually booted, and the executor
answers for its configured default. Those are different questions whenever a
step names its own rootfs, and answering both from the executor gave two steps
on two different images one cache key.

## Capabilities

Advertised: `PRIVILEGED`, always — commands genuinely run as uid 0 in the
guest. That is safe here and nowhere else in Dhole: root in a microVM is root
over a kernel that owns nothing.

Advertised conditionally: `NESTED_VIRT`, only when the host both has
`/dev/kvm` and has nesting enabled in the kvm module's `nested` parameter. The
capability is the one a microVM looks like it ought to have — it is running on
a hypervisor already — so the promise is plausible right up until the guest
opens `/dev/kvm` and finds nothing there.

Never advertised: `NETWORK`, because a Dhole guest boots with no network device
at all, and `HOST_MOUNT`, because the guest sees a rootfs and nothing of the
host. Both absences are the tier working, not the tier being unfinished.

## Snapshots

Firecracker can freeze a booted guest and restore it (`vm.Snapshotter`). It is
what makes a microVM per step affordable: a cold boot costs more than a short
step, so a strong-isolation tier that could only cold-boot is a tier nobody
would select. The snapshot is taken after the guest agent answers — one taken
earlier restores to a guest that is still booting, which is the cold boot the
snapshot was supposed to replace.

## Running the tests

They need a real hypervisor and refuse to run under emulation, because a TCG
guest measures nothing the scheduler relies on:

```bash
DHOLE_TEST_FIRECRACKER_BIN=/opt/vm/firecracker \
DHOLE_TEST_QEMU_BIN=/usr/bin/qemu-system-aarch64 \
DHOLE_TEST_VM_KERNEL=/opt/vm/vmlinux \
DHOLE_TEST_VM_ROOTFS=/opt/vm/rootfs.cpio.gz \
go test ./internal/executor/vm -count=1 -v
```
