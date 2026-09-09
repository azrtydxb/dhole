# The image the CI acceptance pipeline builds.
#
# It is checked in, and `TestCIPipelineBuildsTheCheckedInDockerfile` requires
# that the pipeline's `source` step emits exactly these bytes: a definition
# cannot reference a file from the repository — a step's inputs come from
# edges and nothing else (ADR 0001) — so the text is carried in the step, and
# that test is what stops the copy drifting from this file.
FROM busybox:1.36
COPY hello /usr/local/bin/hello
# Never root: nothing here needs it, and the gate refuses a Dockerfile that
# does not say so.
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/hello"]
