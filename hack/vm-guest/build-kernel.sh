#!/usr/bin/env bash
# Build the guest kernel the vm executor boots, for both the Firecracker and
# the QEMU backends. Run it on a machine of the TARGET architecture (it builds
# natively; nothing here cross-compiles) with gcc, make, bc, bison, flex,
# libelf and libssl headers, curl and xz installed.
#
#   hack/vm-guest/build-kernel.sh OUT_DIR
#
# Writes OUT_DIR/Image (arm64) or OUT_DIR/vmlinux (amd64) and OUT_DIR/config,
# the fully resolved .config the image was built from.
set -euo pipefail

KERNEL_VERSION="${KERNEL_VERSION:-6.12.110}"
out="$(realpath -m "${1:?usage: build-kernel.sh OUT_DIR}")"
here="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$out"

case "$(uname -m)" in
aarch64)
	arch=arm64
	target=Image
	artefact=arch/arm64/boot/Image
	;;
x86_64)
	arch=x86_64
	target=vmlinux
	artefact=vmlinux
	;;
*)
	echo "build-kernel.sh: unsupported architecture $(uname -m)" >&2
	exit 1
	;;
esac

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
major="${KERNEL_VERSION%%.*}"
curl -fsSL "https://cdn.kernel.org/pub/linux/kernel/v${major}.x/linux-${KERNEL_VERSION}.tar.xz" |
	tar -xJ -C "$work"
cd "$work/linux-${KERNEL_VERSION}"

fragments=("$here/guest-kernel.config")
if [ -f "$here/guest-kernel.${arch}.config" ]; then
	fragments+=("$here/guest-kernel.${arch}.config")
fi
make ARCH="$arch" defconfig
scripts/kconfig/merge_config.sh -m .config "${fragments[@]}"
make ARCH="$arch" olddefconfig

# merge_config.sh warns and carries on when a symbol does not take; a guest
# whose vsock quietly became a module boots, runs the agent, and then cannot
# open its socket. Refuse that here rather than at the first step.
for sym in VSOCKETS VIRTIO_VSOCKETS VIRTIO_MMIO BLK_DEV_INITRD RD_GZIP DEVTMPFS; do
	if ! grep -qx "CONFIG_${sym}=y" .config; then
		echo "build-kernel.sh: CONFIG_${sym} is not built in: $(grep "CONFIG_${sym}[= ]" .config || echo unset)" >&2
		exit 1
	fi
done

make ARCH="$arch" -j"$(nproc)" "$target"
install -m 0644 "$artefact" "$out/$target"
install -m 0644 .config "$out/config"
echo "built linux-${KERNEL_VERSION} $target: $(sha256sum "$out/$target" | cut -d' ' -f1)"
