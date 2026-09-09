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

The template's **first container** supplies the image and the security context.
Its command is replaced: the pod exists to be entered, not to run one program.

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

One caveat worth knowing: the identity describes the executor's **configured
template**. A `Spec` naming its own image gets that image for its pod, and a
caller mixing images on one executor is asking a single identity to describe
several environments. Task 37b moves the identity to the sandbox for exactly
this reason; until it lands, run one executor per image where cache correctness
matters.

## Testing against a real cluster

The Kubernetes executor's tests create and destroy their own namespace and skip
themselves when `DHOLE_TEST_KUBECONFIG` is unset:

```
make test-integration DHOLE_TEST_KUBECONFIG="$HOME/.kube/config"
```

`deploy/test/` holds manifests for the integration suite's other dependencies —
Postgres, MinIO and a zot registry — for a machine with no local container
runtime.
