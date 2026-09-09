# The `process` executor

`kind: process` — steps run as bare host processes. `internal/executor/process`.

It is the simplest backend and the reason the executor interface is not shaped
around containers: there is no image, no namespace and no daemon here, yet every
method of `executor.Sandbox` still means something
([ADR 0006](../../.procoder/adr/0006-executors-are-pluggable-and-sandbox-lifetime-is-an-explicit.md)).

A sandbox is a temporary directory on the host and the processes currently
running in it. `Spec.Image` is ignored — this backend has no images, and asking
for one is not an error.

## What it advertises: nothing

`Capabilities()` returns an empty set, on purpose.

A bare process shares the host's user, filesystem and network. It cannot grant
privilege as a *controlled* capability, and it cannot bound a host mount,
because every path on the host is already reachable. Advertising either would be
a promise this backend cannot keep — and a capability the scheduler would then
rely on.

The consequence is exactly the one you want: the scheduler never hands this
backend a step that assumes isolation it does not have.

## It has no environment identity

`EnvironmentIdentity()` returns `ErrNoStableIdentity`.

A host process runs against whatever compilers, libraries and tools the host
happens to carry, and no honest digest describes that. Returning a fabricated
identity would let the cache serve results from an environment that has since
changed, so this backend reports the truth and **its steps stay non-cacheable**
([ADR 0009](../../.procoder/adr/0009-content-addressed-cache-is-a-v1-core-primitive.md)).

If you ran the quickstart twice and wondered why nothing came from the cache,
this is why. It is the safe direction to be wrong in.

## When to use it

- Development, `dhole local run`, and the embedded single binary's own engine.
- A homelab where the host is the sandbox and everyone involved knows it.
- Any workload whose isolation requirement is genuinely "none".

Not for untrusted work, and not where cache hits matter. For a reproducible
environment and a real digest to cache against, use
[the kubernetes executor](kubernetes.md).
