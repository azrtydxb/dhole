#!/usr/bin/env bash
# Build the eStargz fixture TestLazyPullFetchesFewerBytesThanFullImage pulls:
# busybox plus one layer holding 512MiB of random bytes, which gzip cannot
# shrink, so the image really is that large on the wire. Converted to eStargz
# and pushed to REGISTRY, which must accept plain-HTTP pushes.
#
#   hack/lazy-pull/build-fixture.sh 127.0.0.1:5000
#
# Needs crane, and nerdctl talking to a running containerd. Prints the
# reference to put in DHOLE_TEST_ESTARGZ_IMAGE.
set -euo pipefail

registry="${1:?usage: build-fixture.sh REGISTRY}"
repo="$registry/dhole/lazy-fixture"
arch="$(uname -m)"
case "$arch" in aarch64) platform=linux/arm64 ;; x86_64) platform=linux/amd64 ;; *) platform="linux/$arch" ;; esac

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/root/opt/payload"
head -c $((512 << 20)) /dev/urandom >"$work/root/opt/payload/blob.bin"
tar -C "$work/root" -cf "$work/payload.tar" opt

crane append --platform "$platform" --insecure \
	-b docker.io/library/busybox:1.36 -f "$work/payload.tar" -t "$repo:full"
nerdctl pull -q --insecure-registry "$repo:full"
nerdctl image convert --estargz --oci "$repo:full" "$repo:estargz"
nerdctl push --insecure-registry "$repo:estargz"

# The conversion left every layer in containerd's content store, and content is
# shared across containerd namespaces: left there, the test's full-pull control
# would fetch nothing and refuse to trust its own counter.
nerdctl image rm "$repo:full" "$repo:estargz"
echo "DHOLE_TEST_ESTARGZ_IMAGE=$repo:estargz"
