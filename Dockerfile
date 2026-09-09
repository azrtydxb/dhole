# The release image. GoReleaser has already built the binary; this only wraps
# it, which is why there is no build stage and no toolchain in the result.
#
# BINARY selects which of the two this image carries — `dhole` (control plane
# and CLI) or `dhole-engine`. One Dockerfile rather than two, because the two
# images differ in exactly one line and two files would drift.
FROM gcr.io/distroless/static-debian12:nonroot

ARG BINARY=dhole
COPY ${BINARY} /usr/local/bin/dhole-entrypoint

# distroless/static carries no shell, so there is nothing in this image that can
# run a command it was not given. The state directory is a volume: the CAS and
# the blob store are not part of the image.
VOLUME ["/var/lib/dhole"]
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/dhole-entrypoint"]
