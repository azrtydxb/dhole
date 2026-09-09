# Deployment topologies

The same code paths serve a laptop and a cluster. What changes between them is
which parts are embedded and which are separate processes — not which engine
runs your steps, and not which scheduler decides them.

## Single binary (laptop, homelab)

```
dhole serve
```

An embedded NATS server with JetStream, a SQLite run store, filesystem object
stores, one engine hosted beside the control plane, and the API on
`127.0.0.1:7777`.

The hosted engine is not called in-process. It dials the embedded bus and takes
its work off the same `job.dispatch.*` work queue an engine in another
datacentre would, which is what makes a laptop and a cluster the same system
rather than two that resemble each other.

| Flag                   | Default                            | Selects                                                  |
| ---------------------- | ---------------------------------- | -------------------------------------------------------- |
| `--mode`               | `embedded`                         | `embedded` or `distributed` (control plane only)         |
| `--store-dsn`          | `<user config dir>/dhole/dhole.db` | a Postgres DSN, or any other value as a SQLite path      |
| `--bus-url`            | —                                  | the NATS server to dial; ignored in embedded mode        |
| `--blob-root`          | `<user config dir>/dhole`          | where the CAS, the blob store and the embedded bus live  |
| `--api-addr`           | `127.0.0.1:7777`                   | where the one contract is served (`DHOLE_API_ADDR`)      |
| `--no-api`             | off                                | serve no contract at all                                 |
| `--api-allowed-origin` | none                               | a browser origin allowed cross-origin API calls          |
| `--deployment-id`      | derived from `--blob-root`         | which plane owns an outbox claim (`DHOLE_DEPLOYMENT_ID`) |
| `--otlp-endpoint`      | —                                  | where traces and metrics go (`DHOLE_OTLP_ENDPOINT`)      |

For a one-off run with no plane at all, `dhole local run` brings the same
embedded plane up, runs one pipeline and exits — see [the quickstart](quickstart.md).

## Control plane plus remote engines

The step that makes a homelab useful: keep the plane where it is, and put
engines where the work belongs — a build Mac, a GPU box, a machine inside a
customer's network.

Engines dial **outbound only**. There is no port to open on the engine and no
inbound firewall rule to write, which is what lets one live behind NAT or CGNAT.

```
DHOLE_BUS_URL=nats://plane.internal:4222 \
DHOLE_ENGINE_ID=buildmac-1 \
DHOLE_TIER=trusted \
DHOLE_SLOTS=4 \
dhole-engine
```

`DHOLE_ENGINE_ID` must be unique per process — it is the subject its control
messages arrive on. `DHOLE_TIER` is the trust tier whose work it takes, and its
bus credentials permit that tier only: an engine in the untrusted tier that
subscribes to the trusted tier's dispatch subjects gets a permissions error from
the server, not a polite refusal from the control plane.

## Distributed (cluster)

```
dhole serve --mode distributed \
  --store-dsn 'postgres://…' \
  --bus-url nats://nats:4222 \
  --api-addr 0.0.0.0:7777
```

Postgres and clustered NATS are the tuned target. JetStream is the availability
floor ([ADR 0005](../.procoder/adr/0005-nats-jetstream-is-the-bus-and-the-availability-floor.md)):
if the bus is a single replica, so is the deployment, whatever the plane's
replica count says.

Two control-plane processes may share a database — that is what lets both drain
the same outbox backlog for availability — and they must then share a
`--deployment-id`. Two _different_ deployments sharing one is the failure mode
to avoid: an outbox row is addressed to one plane's bus, and a claim that does
not name the plane makes each publish the other's dispatches to engines that
have never heard of the run.

## The Helm chart

`charts/dhole/` deploys the control plane, engines, and — for a deployment that
has not brought its own — NATS and Postgres.

```
helm install dhole charts/dhole \
  --set postgres.external='postgres://dhole:…@postgres:5432/dhole?sslmode=disable' \
  --set nats.external='nats://nats:4222' \
  --set controlPlane.persistence.enabled=true
```

The defaults deploy something that **works** and is not something to run a
business on: single-replica in-chart Postgres and NATS, both with `emptyDir`. A
chart whose defaults fail teaches nothing, and one whose defaults quietly look
production-ready teaches something false — so the install notes print a warning
for each of them.

The values worth knowing:

| Value                              | Default  | What it decides                                      |
| ---------------------------------- | -------- | ---------------------------------------------------- |
| `postgres.external`                | `""`     | a DSN the chart does not manage. Set it.             |
| `nats.external`                    | `""`     | a `nats://` URL the chart does not manage. Set it.   |
| `controlPlane.replicas`            | `1`      | planes sharing one database and one deployment id    |
| `controlPlane.persistence.enabled` | `false`  | whether the CAS survives a reschedule                |
| `engines`                          | one tier | a list; one Deployment per trust tier                |
| `image.tag`                        | `""`     | empty means the chart's `appVersion`, never `latest` |

`engines` is a list because a tier is a deployment decision, not a label:

```yaml
engines:
  - name: trusted
    tier: trusted
    replicas: 3
    slots: 4
  - name: untrusted
    tier: untrusted
    replicas: 2
    slots: 1
```

There is no Service for engines and nothing to expose: they dial out. Their
`DHOLE_ENGINE_ID` is the pod name, which is the one identifier Kubernetes
guarantees is unique and stable for a pod's life.

## Where steps actually run

The plane decides _whether_ a step may run; an [executor](executors/process.md)
decides _where_. The backend is configuration, not a constant, because it is
what reports the environment identity every cache key is folded over
([ADR 0009](../.procoder/adr/0009-content-addressed-cache-is-a-v1-core-primitive.md)):

- [`process`](executors/process.md) — bare host processes. No isolation, no
  environment identity, therefore no caching. Development and homelab.
- [`kubernetes`](executors/kubernetes.md) — a pod per sandbox, with the image
  digest as the environment identity, so steps in it are genuinely cacheable.

## Integration dependencies for testing

`deploy/test/` holds Kubernetes manifests for Postgres, MinIO and a zot
registry, for a machine with no local container runtime;
`docker-compose.test.yml` is the same set for a machine that has one. Every
integration test skips itself with a reason when its endpoint variable is unset
— an integration test that silently degrades to nothing is worse than none,
because it reports green.

## Observability

`--otlp-endpoint` sends traces and metrics over OTLP; `--otlp-insecure` is for a
collector without TLS. `JobDispatch.trace_context` carries the run's W3C trace
context to the engine, so a step's spans join the run's trace even though the
engine is a different process on a different machine.
