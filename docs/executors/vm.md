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

| Backend                 | When                                                               |
| ----------------------- | ------------------------------------------------------------------ |
| `firecracker` (default) | linux/amd64 and linux/arm64 with `/dev/kvm` — the one worth having |
| `qemu`                  | the portable fallback, wherever KVM runs and Firecracker does not  |

The guest is identical either way: same kernel, same rootfs, same agent, same
protocol. Only the machine model and the route to the guest's vsock differ,
which is what lets one conformance run cover both. Both are held to
`internal/executor/executortest.Contract` — a fallback nobody tested is a
fallback that behaves differently on the day it is used.

QEMU is a fallback rather than a peer for two reasons that are paid per step:
it boots a full machine model where Firecracker boots four devices, and it has
no snapshot restore in Dhole, so every sandbox is a cold boot.

Both are held to that contract on real arm64 hardware with one guest kernel between
them — see "Building a guest kernel" below. QEMU needs one thing from the host
that Firecracker does not: `/dev/vhost-vsock`, because its
`vhost-vsock-device` is the kernel's vhost backend where Firecracker proxies
vsock over a unix socket of its own.

## Configuration

| Field         | Meaning                                                                  |
| ------------- | ------------------------------------------------------------------------ |
| `Backend`     | `firecracker` or `qemu`; empty means `firecracker`                       |
| `BinaryPath`  | the hypervisor executable; empty means PATH                              |
| `KernelImage` | the guest kernel every sandbox boots                                     |
| `RootfsImage` | the default rootfs snapshot — an initramfs whose init is the guest agent |
| `SnapshotDir` | where snapshots are written; empty disables snapshotting                 |
| `VCPUs`       | guest vcpus; zero means 1                                                |
| `MemMiB`      | guest memory; zero means 256                                             |
| `KernelArgs`  | replaces the default kernel command line                                 |

As engine environment variables: `DHOLE_EXECUTOR=vm`, `DHOLE_VM_BACKEND`,
`DHOLE_VM_HYPERVISOR`, `DHOLE_VM_KERNEL`, `DHOLE_VM_ROOTFS`,
`DHOLE_VM_SNAPSHOT_DIR`, `DHOLE_VM_VCPUS`, `DHOLE_VM_MEM_MIB`.

## On Kubernetes, the device is not the permission

A vm engine in a pod needs two separate things, and asking for the first does
not give the second:

1. The device. `devices.kubevirt.io/kvm: "1"` in the container's resources,
   served by KubeVirt's kvm device plugin.
2. Permission to OPEN it. `/dev/kvm` is `root:kvm 0660` on every distribution,
   and the engine image runs as uid 65532 in group 0 — so the open fails with
   EACCES and firecracker reports it in its own words: `Error creating KVM
object: Permission denied (os error 13) Make sure the user launching the
firecracker process is configured on the /dev/kvm file's ACL`. Every step on
   that engine then fails to acquire a sandbox, which reads as a broken engine
   rather than a missing group.

The chart's `engines[].kvmGroup` supplies the second: it adds the node's `kvm`
gid to the pod's `supplementalGroups`. That gid belongs to the NODE and
distributions disagree — 994 on Armbian noble, 108 on Debian, 36 on Fedora —
so the chart defaults it to nothing rather than guessing, because a wrong gid
grants nothing and fails exactly as it did before. Read it on a node:

```bash
stat -c %g /dev/kvm
```

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
for a in sh sleep id printf echo cat ls seq env mkdir rm true false sync \
         head tail grep tr sed awk wc sort uniq cut tee dd base64 \
         md5sum sha256sum touch cp mv ln chmod find xargs date test expr \
         basename dirname tar gzip du df ps kill uname mktemp stat; do
  ln -s /bin/busybox root/bin/$a
done
mkdir -p root/{proc,sys,dev,tmp,dhole/work}
( cd root && find . | cpio -o -H newc --quiet | gzip -1 > ../rootfs.cpio.gz )
```

**Symlink generously.** busybox only answers to a name it is linked as, and a
name that is missing is `exit 127` — "command not found" — from a shell that
started perfectly well, which reads as a broken step rather than a thin rootfs.
An earlier version of this recipe linked ten applets and the conformance suite
failed `secret-redemption` on it, because that case pipes the redeemed value
through `tr`. The engine was correct and the guest could not run the step.

A dynamically linked busybox in an initramfs with no libc fails as
`exec: "sh": executable file not found in $PATH`, which reads as a missing
program rather than a missing loader. It has to be the static one.

### Building a guest kernel

The kernel needs virtio-mmio, virtio-vsock, devtmpfs and initramfs support.
**vsock has to be built in, not a module**: the agent is the init, so nothing
has loaded a module by the time it needs the socket.

Two readily available kernels each miss one half. The Firecracker CI kernels
(`https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci/v1.11/<arch>/vmlinux-6.1.102`)
boot Firecracker but **not** QEMU's `virt` machine — the guest resets before
printing a line, with or without KVM, with or without `earlycon`. A stock
distribution kernel boots under QEMU and runs the agent, and then the agent
cannot open a vsock socket because vsock is a module the initramfs never loads.

`hack/vm-guest/build-kernel.sh` builds one that does both. It takes a pinned
kernel.org release (`KERNEL_VERSION`, default `6.12.110`), starts from the
architecture's `defconfig`, merges `hack/vm-guest/guest-kernel.config` (plus
`guest-kernel.<arch>.config` where one exists) and refuses to build if any of
the options that matter did not end up `=y`. It builds natively — run it on a
machine of the target architecture:

```bash
# Debian: build-essential bc bison flex libelf-dev libssl-dev curl xz-utils
hack/vm-guest/build-kernel.sh out/
# out/Image (arm64) or out/vmlinux (amd64), and out/config beside it
```

The arm64 image this produced on 2026-09-14 — linux 6.12.110, gcc from Debian
trixie, sha256 `2f3edcb947b13ea8454b44be326df9f9489943dd3c3414ca0118d9dda8bbd001`
— passes `TestVMExecutorContract` under Firecracker v1.13.1 **and**
`TestQEMUExecutorContract` under QEMU 10.0.13 with KVM, with the rootfs above.
A rebuild is not bit-for-bit identical (the build embeds a timestamp and host
name), which is why the digest is a record of what was tested rather than
something to verify a rebuild against. The amd64 build of the same recipe has
not been run.

On Kubernetes, QEMU's `/dev/vhost-vsock` is not one of the devices KubeVirt's
plugin serves (it serves `kvm`, `tun` and `vhost-net`), so the pod that ran the
QEMU contract was privileged. A vm engine pod that must stay unprivileged runs
Firecracker.

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
DHOLE_TEST_VM_KERNEL=/opt/vm/Image \
DHOLE_TEST_VM_ROOTFS=/opt/vm/rootfs.cpio.gz \
go test ./internal/executor/vm -count=1 -v
```
