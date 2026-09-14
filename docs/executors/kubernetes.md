# The `kubernetes` executor

`kind: kubernetes` — steps run as pods in a cluster.
`internal/executor/kubernetes`.

A sandbox is one pod holding an idle container; commands run inside it through
the API server's exec subresource. Nothing in the executor interface grew a
Kubernetes shape to make this work — the pod is an implementation detail behind
`Acquire`.

## Configuration

| Field            | Meaning                                                              |
| ---------------- | -------------------------------------------------------------------- |
| `Kubeconfig`     | path to a kubeconfig; empty means the in-cluster service account     |
| `Namespace`      | where sandbox pods are created; empty means `default`                |
| `ServiceAccount` | the identity sandbox pods run as; empty leaves the namespace default |
| `PodTemplate`    | the pod every sandbox is made from                                   |
| `Resources`      | requests and limits for the step container; unset means unsized      |

The template's **first container** supplies the image and the security context.
Its command is replaced: the pod exists to be entered, not to run one program.

An engine reads these from its environment:

| Variable                        | Field                                  |
| ------------------------------- | -------------------------------------- |
| `DHOLE_KUBECONFIG`              | `Kubeconfig`                           |
| `DHOLE_SANDBOX_NAMESPACE`       | `Namespace`                            |
| `DHOLE_SANDBOX_SERVICE_ACCOUNT` | `ServiceAccount`                       |
| `DHOLE_SANDBOX_CPU_REQUEST`     | `Resources.Requests[cpu]`, e.g. `500m` |
| `DHOLE_SANDBOX_CPU_LIMIT`       | `Resources.Limits[cpu]`, e.g. `2`      |
| `DHOLE_SANDBOX_MEMORY_REQUEST`  | `Resources.Requests[memory]`, `512Mi`  |
| `DHOLE_SANDBOX_MEMORY_LIMIT`    | `Resources.Limits[memory]`, `4Gi`      |

## Sizing sandbox pods

With no resources set, a sandbox pod has no requests and no limits. The
scheduler counts it as free, and a `go build` inside it takes the whole node.
On kw the engine sharing that node took 36 seconds to pick up its next dispatch
— past its 30 second lease — so the control plane declared a live attempt lost
and ran it again. Nothing was wrong in the scheduler or the lease; the engine
was simply not getting CPU.

**Sizing is per engine, and it is the operator's.** In the chart it is
`engines[].sandbox.resources`, so each trust tier has its own:

```yaml
engines:
  - name: trusted
    slots: 2
    sandbox:
      resources:
        requests: { cpu: 500m, memory: 512Mi }
        limits: { cpu: "2", memory: 4Gi }
    resources: # the engine's own pod
      requests: { cpu: 500m, memory: 512Mi }
```

There is deliberately no resource field on a step. One would let a pipeline
author decide how much of the cluster's nodes their pods take, and that is a
capacity decision for whoever runs the fleet. A step that needs a bigger pod
belongs on a tier whose engines are sized for it. The natural extension — not
built — is a per-step _request_ bounded by the operator's limit.

How to size:

- **Protect the engine.** An engine runs up to `slots` sandboxes at once, so
  keep `slots × sandbox limits.cpu + engine requests.cpu` within the node's
  allocatable CPU. A tier running every slot flat out then still leaves the
  engine the CPU it asked for, and it keeps renewing its leases. Do the same
  sum for memory. The chart's default engine request is `500m`.
- **Requests are what the scheduler reserves**, so set them to what a typical
  step really uses; the scheduler will then spread sandboxes instead of piling
  them onto one node.
- **A limit with no request** makes Kubernetes use the limit as the request.
- **Nothing configured stays unsized** — exactly the behaviour before this
  setting existed. No default is invented, because no number suits both a
  Raspberry Pi and a 96-core node, and a guessed memory limit OOM-kills a step
  on a cluster whose operator never asked for one.

A value the engine cannot parse, a negative one, or a request above its limit
**stops the engine at startup**, naming the variable. It is never ignored: a
limit an operator believes is in force and is not is worse than none.

Keys you set override the same keys in a `PodTemplate`'s first container; keys
you leave unset keep the template's. Only the step container is sized — sidecars
a template adds are the template's to size.

**A LimitRange is no longer needed** once this is set. It was the only lever a
cluster had while sandbox pods were unsized; with `sandbox.resources` configured,
remove it rather than keep two sources of truth for the same number — a
LimitRange's `max` can still refuse a pod Dhole sized larger, and that shows up
as every step failing to acquire a sandbox.

Empty `Kubeconfig` is what lets the same binary work as a pod inside the cluster
it schedules into.

## Lease scopes mean something here

This is where [ADR 0006](../../.procoder/adr/0006-executors-are-pluggable-and-sandbox-lifetime-is-an-explicit.md)'s
lease scopes become visible:

- `step` — the pod is deleted when the step ends. No carried state, and the only
  scope with nothing a cache key cannot see.
- `job` / `pipeline` — the same pod is entered again for every step of a job or a
  run. Faster, and carrying state no cache key can describe, which is exactly why
  `internal/cache` refuses to cache steps that run in one.
- `pool` — a warm sandbox reused across runs. Deliberately dirty; not cacheable.
- `service` — a sidecar bounded by the pipeline that started it.

## Capabilities are what the template genuinely provides

A pod always has a network namespace, so `NETWORK` is advertised. Privilege and
host mounts exist **only if the template asked for them**, and secret delivery is
not implemented in this backend at all. Advertising any of those would be a
promise the scheduler would then rely on.

## Environment identity is the image digest, never the tag

A tag is a moving target, so caching against one serves results produced in an
environment that no longer exists. An image that cannot be resolved to a digest
yields `ErrNoStableIdentity` and its steps stay uncached — the safe direction.

There are two of them, and the difference matters. `Executor.EnvironmentIdentity`
describes the **configured template** — what a sandbox acquired with an empty
`Spec` would run — and it is what an engine announces on registration
([ADR 0021](../../.procoder/adr/0021-the-environment-identity-behind-a-cache-key-comes-from-the-engines.md)).
`Sandbox.EnvironmentIdentity` describes the pod that was actually created, image
and all.

The cache key is hashed against the second kind of answer. It used to be hashed
against the first, and one executor running several images then reported the
template's digest for every one of them: two steps on two different images
produced ONE cache key, and the second to run was served the first one's
outputs. A pipeline that names its own image is keyed against that image
instead — pinned to a digest, or not cached at all, because the control plane
resolves no tags.

## Testing against a real cluster

The Kubernetes executor's tests create and destroy their own namespace and skip
themselves when `DHOLE_TEST_KUBECONFIG` is unset:

```
make test-integration DHOLE_TEST_KUBECONFIG="$HOME/.kube/config"
```

`deploy/test/` holds manifests for the integration suite's other dependencies —
Postgres, MinIO and a zot registry — for a machine with no local container
runtime.
