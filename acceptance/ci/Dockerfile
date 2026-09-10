# The image the CI acceptance pipeline builds.
#
# It is checked in, and the pipeline READS IT: acceptance/ci/pipeline.yaml
# declares a file at this path and the `build` step binds it to an input port,
# so the bytes are uploaded beside the definition and the revision pins them
# (ADR 0023). Nothing copies this text anywhere, which is why there is no
# longer a test keeping two copies equal.
FROM busybox:1.36
COPY hello /usr/local/bin/hello
# Never root: nothing here needs it, and the gate refuses a Dockerfile that
# does not say so.
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/hello"]
