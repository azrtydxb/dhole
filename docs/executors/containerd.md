# The `containerd` executor

`kind: containerd` — steps run as OCI containers on a containerd daemon.
`internal/executor/containerd`.

There is no Docker daemon anywhere in this picture. [ADR
0006](../../.procoder/adr/0006-executors-are-pluggable-and-sandbox-lifetime-is-an-explicit.md)
ships containerd/OCI precisely so the container backend is a client of the
runtime rather than of another daemon that owns it. A sandbox is one container
holding an idle init process, and commands are exec processes inside it —
the container is an implementation detail behind `Acquire`, and nothing in the
executor interface grew a container shape to make it work.

## Configuration

| Field                | Meaning                                                                |
| -------------------- | ---------------------------------------------------------------------- |
| `Address`            | the containerd socket; empty means `/run/containerd/containerd.sock`   |
| `Namespace`          | the containerd namespace sandboxes live in; empty means `dhole`        |
| `Image`              | the sandbox image; empty means `docker.io/library/busybox:1.36`        |
| `Snapshotter`        | override; empty lets the executor choose, preferring lazy pull         |
| `Privileged`         | run sandboxes privileged and as root, and advertise `PRIVILEGED`       |
| `HostNetwork`        | put sandboxes in the host's network namespace, and advertise `NETWORK` |
| `InsecureRegistries` | registry hosts reachable over plain HTTP; everything else is HTTPS     |
| `Logger`             | receives the pull-mode decisions                                       |

The namespace is deliberately not `default` or `k8s.io`: those hold somebody
else's containers, and `Release` deletes what it finds by name.

## It runs next to the daemon, not across the network

containerd's socket is a unix socket and the exec streams are FIFOs the shim
opens **by path**, so the executor and the daemon must see the same paths. A
process on the node satisfies that; a container beside the node's containerd
does too, given the socket and the FIFO directory (`/run/containerd`) mounted
at the same paths. It also needs write access to both, which for a packaged
containerd means root.

Nothing else about the daemon's filesystem is required. The obvious
`oci.WithImageConfig` is not used for exactly that reason — it resolves the
image's user by temp-mounting the rootfs on whatever machine the CLIENT runs
on, and fails with `open .../snapshots/25009/fs: no such file or directory` for
anything not sharing the daemon's disk. The image's environment is read from
the content store instead, and the user and working directory are set outright.

## Rootless by default

A step that gets root in its sandbox gets root over every host resource that
sandbox is given, and nothing in a passing build would show it. So the
container runs as uid/gid 65534 with `no_new_privs`, and the sandbox root
(`/dhole`) is a tmpfs — the image's own filesystem belongs to root, and a step
that is not root cannot write to it.

Privilege exists only where it was configured: `Privileged` makes the executor
advertise `CAPABILITY_PRIVILEGED` and grant it. A spec requesting a capability
the executor does not advertise is **refused**, before anything is created,
with `capability not advertised`. Running it anyway would give the step weaker
isolation than it asked for, and it would never learn.

## Lease scopes mean something here

- `step` — the container and its snapshot are deleted when the step ends. The
  only scope with no carried state a cache key cannot see.
- `job` / `pipeline` — the same container is entered again for every step.
  Faster, and carrying state no cache key can describe, which is why
  `internal/cache` refuses to cache steps that run in one.
- `pool` — a warm sandbox reused across runs. Deliberately dirty; not cacheable.
- `service` — a sidecar bounded by the pipeline that started it.

Signals are delivered by sweeping the container's process table rather than by
killing the task's cgroup, and that is what makes the scopes above survivable:
killing the cgroup would take the init process with it, and a pipeline-leased
container has to still be there for the next step.

## Cancellation kills the tree, not just the child

A step is a process tree. Its direct child often exits immediately — anything
that backgrounds a server does this — leaving grandchildren reparented to the
container's init with nothing connecting them to the step. Every process of a
command therefore carries a marker in its environment, and a cancelled step is
swept by that marker.

The sweep runs from a watcher on the caller's context rather than after the
exit status, and the ordering is load-bearing: containerd cannot tear down an
exec process whose stdio something the step left running still holds open, so a
sweep that waits for the status waits for the very process it is meant to kill,
and `Exec` returns fifteen seconds after the cancellation instead of within the
contract's two-second window.

## Environment identity is the image digest, never the tag

The same tag serves different content over time, so a cache keyed on one
returns yesterday's answer for today's image ([ADR
0021](../../.procoder/adr/0021-the-environment-identity-behind-a-cache-key-comes-from-the.md)).
The identity is resolved from the **registry**, not the local image store,
because the store answers with whatever was pulled last. Images are pulled by
that digest for the same reason: a cache key describing a different image than
the one that ran is worse than no cache at all.

An image that cannot be resolved yields `ErrNoStableIdentity` and an empty
string — absent, never invented — and its steps stay uncached.

The identity describes the executor's **configured image**, so a caller mixing
images on one executor is asking a single identity to describe several
environments. Run one executor per image where cache correctness matters.

## Lazy pull is an optimisation, and it proves itself

With the stargz snapshotter, a container starts after fetching an image's table
of contents and the few chunks the command touches. `SelectPullMode` enables it
when the daemon advertises the snapshotter, and falls back to a full pull with
a logged warning otherwise — a silent fallback presents as "the pipeline got
slower for no reason".

Advertising it is not proof it works. A k3s node reports a stargz snapshotter
with no init error whose containers fail to create, so a container that cannot
be made with the lazy snapshotter demotes the executor to the ordinary one for
the rest of its life, warns, and retries. A snapshotter named explicitly in
`Snapshotter` is never demoted: that is a configuration decision, and silently
running somewhere else would hide it.

## Testing against a real containerd

The tests create and destroy containers in their own containerd namespace and
skip themselves when `DHOLE_TEST_CONTAINERD_SOCK` is unset. There is no fake
client behind them on purpose: a mock never pulls an image, starts a shim or
reaps a process, so nothing they assert would be real.

```
sudo make test-integration DHOLE_TEST_CONTAINERD_SOCK=/run/containerd/containerd.sock
```

On a k3s node the socket is `/run/k3s/containerd/containerd.sock`. CI runs the
same suite against the containerd its runners already have, and fails the job
if the contract only skipped.
