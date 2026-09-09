# 0006 — Executors are pluggable and sandbox lifetime is an explicit lease

Status: accepted
Date: 2026-09-09

## Context

Containers are not universally the right execution environment: macOS and iOS builds,
Windows-native work, GPU and nested-virt workloads, hardware-in-the-loop, and full-VM
labs such as RouterOS CHR cannot run in one. Separately, "containers are slow" is usually
not the container — it is image pull, cold start per step, a fresh clone every run, and
no warm process. Those are lifetime problems, not engine problems, and conflating the two
leads to adding backends as a workaround for a caching deficiency.

## Decision

Two orthogonal axes, selected independently.

Engine — where a step runs, behind one interface: `acquire`, `exec`, `put`/`get`,
`signal`, `release`. Anything an engine cannot do is a capability it does not advertise,
never a special case in the core. Steps declare requirements (os, arch, gpu, nested-virt,
isolation level); engines advertise capabilities; the scheduler matches, with an explicit
override available.

Lease — how long the sandbox lives: `step`, `job`, `pipeline`, `pool` (warm, reused
across runs), plus `service` leases for sidecars bounded by the pipeline.

Cacheability is a property of the (engine, lease) pair and is computed and displayed, not
assumed: a `pool` lease has an input we cannot hash, so it is opt-in dirty-for-speed.
Environment identity likewise varies — image digest for containers, snapshot id for VMs,
and for host processes no honest identity exists, so those are non-cacheable unless a
toolchain fingerprint is declared.

Ship containerd/OCI (not the Docker daemon), local process, and Kubernetes first; VM once
the interface has survived contact with those three.

Rejected: modelling the interface on Docker, which makes non-container backends
second-class fakes. Rejected: making lifetime implicit per engine.

## Consequences

Easier: exotic targets are capability declarations rather than forks. The speed levers —
lazy image pull, warm pools, content-addressed workspace mounts — are available without
changing the execution model.

Harder: every engine re-implements log streaming, cancellation, timeouts, signal
propagation, resource limits and secret delivery, and the semantics diverge subtly
(SIGTERM propagation, exit code on OOM, killing a Windows process tree). A conformance
suite is mandatory or "pluggable engines" becomes several incompatible products sharing a
config format. Cross-engine state transfer cannot use shared volumes, so the CAS is the
only portable path and volume sharing degrades to an optimisation within a single lease.
