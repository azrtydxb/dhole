# Dhole

An everything-pipeline engine: durable, typed workflows that run anywhere, driven by
anything, editable by humans and agents alike.

> **Status: usable, not finished.** Pipelines run end to end from a single
> binary and from a distributed control plane, and the same definition produces
> identical results against SQLite or Postgres, embedded or out-of-process NATS,
> and filesystem or S3 object stores.
>
> In place: the event-sourced run store, the scheduler, the outbox, durable
> waits and human approval, leases and fencing, the content-addressed store and
> the cache, the engine wire protocol with a conformance suite an engine in any
> language is held to, the executor interface with host-process and Kubernetes
> backends, CEL policy with trust tiers and audit, taint tracking, identity and
> OIDC federation, the plugin catalog with `oci://` and `cas://` resolution,
> signature verification, federated upstreams and lockfile resolution at save,
> the definition store with revisions and a one-way git mirror, four triggers
> (schedule, HTTP, git webhook, pipeline completion), the LLM, bounded-loop and
> agent step types, the ConnectRPC API with a CLI derived from its descriptors,
> the React Flow editor, importers for four other CI formats, and OpenTelemetry
> throughout.
>
> Several things are deliberately unfinished — see **Still open** below, and
> `.procoder/plans/dhole.md` for the task-by-task truth.

## What it is

Dhole is a general workflow engine. CI is one profile of it, not its purpose.

A pipeline is a typed DAG. Steps declare their inputs and outputs, so the graph is derived
from data flow rather than authored as an ordering — which makes content-addressed caching
correct by construction, parallelism free, and local execution possible. Runs are durable
and event-sourced, so a workflow can wait three days for an approval and survive a restart
without noticing.

Work is executed by engines: separate processes, written in any language, that pull
self-contained jobs from a message bus and publish status and logs back. They connect
outbound only, so they run in a homelab, on a build Mac, on a GPU box, or behind CGNAT.
The control plane restarting is a non-event for work already in flight.

Triggers are pluggable and bind to a pipeline's typed inputs, so the same pipeline can be
started by a schedule, a webhook, an API call, a queue message, a human, or an agent.

## Design goals

- **Not another CI tool.** CI, infrastructure automation, and LLM/agent orchestration are
  three profiles over one core.
- **Caching that is correct rather than advisory.** Content-addressed, derived from
  declared inputs, not from cache keys someone invented.
- **Side effects are declared.** Every step carries an effect class, and it governs both
  caching and retry — so "send the message" is never silently replayed.
- **Engines are polyglot.** The wire schema is the contract; write one in any language.
- **Agents are first-class clients.** The drag-and-drop GUI has no privileged endpoints —
  it drives the same API an agent does.
- **Low overhead.** Single binary with an embedded bus, embedded database, and an
  in-process engine for the homelab case; the same code paths scale out.

## Architecture decisions

Every significant decision is recorded in [`.procoder/adr/`](.procoder/adr/), with the
constraint that forced it and the price it pays. Records are immutable — a change of mind
writes a superseding record.

| #                                                                                          | Decision                                                          |
| ------------------------------------------------------------------------------------------ | ----------------------------------------------------------------- |
| [0001](.procoder/adr/0001-typed-content-addressed-dag-replaces-the-shared-mutable.md)      | Typed content-addressed DAG replaces the shared mutable workspace |
| [0002](.procoder/adr/0002-effect-classes-govern-caching-and-retry.md)                      | Effect classes govern caching and retry                           |
| [0003](.procoder/adr/0003-runs-are-durable-and-event-sourced.md)                           | Runs are durable and event-sourced                                |
| [0004](.procoder/adr/0004-control-plane-and-data-plane-split-with-polyglot-engines.md)     | Control plane and data plane split with polyglot engines          |
| [0005](.procoder/adr/0005-nats-jetstream-is-the-bus-and-the-availability-floor.md)         | NATS JetStream is the bus and the availability floor              |
| [0006](.procoder/adr/0006-executors-are-pluggable-and-sandbox-lifetime-is-an-explicit.md)  | Executors are pluggable and sandbox lifetime is an explicit lease |
| [0007](.procoder/adr/0007-triggers-are-pluggable-and-bind-to-typed-pipeline-inputs.md)     | Triggers are pluggable and bind to typed pipeline inputs          |
| [0008](.procoder/adr/0008-definitions-live-in-the-server-database-with-a-one-way-git.md)   | Definitions live in the server database with a one-way git mirror |
| [0009](.procoder/adr/0009-content-addressed-cache-is-a-v1-core-primitive.md)               | Content-addressed cache is a v1 core primitive                    |
| [0010](.procoder/adr/0010-runtime-engine-registry-is-separate-from-the-durable-catalog.md) | Runtime engine registry is separate from the durable catalog      |
| [0011](.procoder/adr/0011-plugin-artifacts-resolve-through-one-scheme-addressed.md)        | Plugin artifacts resolve through one scheme-addressed resolver    |
| [0012](.procoder/adr/0012-policy-is-a-first-class-subsystem-keyed-on-trust-tier.md)        | Policy is a first-class subsystem keyed on trust tier             |
| [0013](.procoder/adr/0013-one-api-contract-serves-gui-cli-and-agents-equally.md)           | One API contract serves GUI, CLI and agents equally               |
| [0014](.procoder/adr/0014-tenancy-exists-in-the-data-model-from-the-first-commit.md)       | Tenancy exists in the data model from the first commit            |
| [0015](.procoder/adr/0015-agent-loops-are-bounded-nodes-and-untrusted-data-is-tainted.md)  | Agent loops are bounded nodes and untrusted data is tainted       |
| [0016](.procoder/adr/0016-v1-is-three-acceptance-pipelines-built-in-parallel.md)           | v1 is three acceptance pipelines built in parallel                |
| [0017](.procoder/adr/0017-the-project-is-named-dhole.md)                                   | The project is named Dhole                                        |
| [0018](.procoder/adr/0018-dhole-is-licensed-apache-2-0.md)                                 | Dhole is licensed Apache-2.0                                      |
| [0019](.procoder/adr/0019-policy-is-expressed-in-cel.md)                                   | Policy is expressed in CEL                                        |

## Definition of done for v1

Three acceptance pipelines, one per profile, all running end to end:

1. **CI** — a container build that demonstrably hits the cache on a second run with
   unchanged inputs.
2. **Automation** — schedule and API triggers, a long wait, execution across more than one
   engine type.
3. **Agent** — a schema-validated structured output, a bounded loop, a human approval
   gate, and recorded token cost.

## Running it

```
make build
./dhole serve
```

With no flags that is the single binary: an embedded NATS server with JetStream, a SQLite
run store, filesystem object stores, one engine hosted beside the control plane, and the
API on `127.0.0.1:7777`. The engine is not called in-process — it dials the embedded bus
and takes its work off the same `job.dispatch.*` work queue an engine in another
datacentre would, which is what makes a laptop and a cluster the same system rather than
two that resemble each other.

| Flag                   | Default                            | What it selects                                         |
| ---------------------- | ---------------------------------- | ------------------------------------------------------- |
| `--mode`               | `embedded`                         | `embedded` or `distributed` (control plane only)        |
| `--store-dsn`          | `<user config dir>/dhole/dhole.db` | a Postgres DSN, or any other value as a SQLite path     |
| `--bus-url`            | —                                  | the NATS server to dial; ignored in embedded mode       |
| `--blob-root`          | `<user config dir>/dhole`          | where the CAS, the blob store and the embedded bus live |
| `--api-addr`           | `127.0.0.1:7777`                   | where the one contract is served (`DHOLE_API_ADDR`)     |
| `--no-api`             | off                                | serve no contract at all; nothing can then talk to it   |
| `--api-allowed-origin` | none                               | a browser origin allowed to make cross-origin API calls |

### The first credential

The API has no unauthenticated call — one contract serves the GUI, the CLI and agents, and
all three authenticate ([ADR 0013](.procoder/adr/0013-one-api-contract-serves-gui-cli-and-agents-equally.md)).
So a plane that had no way to hand out a first credential would be a plane nobody can
reach, and that pressure is how control planes end up with an anonymous mode. `dhole serve`
therefore mints a bootstrap service token at start-up, prints it once, and writes it mode
0600 to `<blob-root>/bootstrap.token`:

```
dhole v0.1.0 (abc1234): embedded control plane, bus nats://127.0.0.1:4222, API http://127.0.0.1:7777
bootstrap credential (valid 24h0m0s, also written to ~/.config/dhole/bootstrap.token):
  export DHOLE_TOKEN=dht_default_…
```

It is an ordinary service token: the store keeps only its SHA-256, it expires, and it is
authenticated by exactly the code path every other credential is. For a second tenant, a CI
account or a replacement for one that leaked, mint another beside the plane's database:

```
dhole token issue --tenant default --subject ci --ttl 720h
```

That is a local administrative command — it takes `--store-dsn` rather than `--server` —
because the contract has no identity service yet, and inventing one that only the CLI could
reach would be the ADR 0013 mistake with the CLI in the privileged seat.

`dhole version` prints the version and commit the binary was built from. SIGINT and
SIGTERM stop the plane rather than killing it: unacknowledged dispatches go back to the
queue and unsent outbox rows are still owed.

## Still open

Named rather than glossed. Each of these is a real gap, and the plan carries the
task that closes it.

- **`dhole serve` runs no step types, no triggers and no timer poll.** It
  imports none of `internal/steps/{llm,loop,approval,agent}`, none of
  `internal/trigger/*`, and never runs the durable timer's poll. The three
  acceptance pipelines pass — CI with a real cache hit, automation across a
  deliberate restart, the agent profile with a bounded loop and an approval —
  but the acceptance harness is what drives those subsystems, not the server.
  Until that wiring lands, "three profiles over one core" is demonstrated by
  the tests and not yet by the product.
- **Arming a durable gate is not atomic with the scheduler's readiness
  decision**, so a wait can be skipped: the gate event is written in its own
  transaction and can land in the log after the dispatch it should have
  blocked.
- **The containerd/OCI executor is not built** (Task 36). There is no container
  runtime on the machine this was developed on, and a backend nobody can run is
  a backend nobody has tested. `internal/executor/containerd` holds only the
  lazy-pull decision; the executors that exist are host-process and Kubernetes,
  and both pass the same shared contract.
- **The lazy-pull byte assertion is unwritten**, and honestly so: proving an
  eStargz image transfers fewer bytes needs a containerd with the stargz
  snapshotter _and_ the executor above. No mock-registry byte count was written,
  because that number would mean nothing.
- **Environment identity belongs to the executor, not the sandbox** (Task 37b).
  One Kubernetes executor running steps with different images reports one
  identity for several environments, so run one executor per image where cache
  correctness matters.
- **Three subsystems are built, tested, and not wired to a caller.** The fair
  queue and per-pipeline budgets (`scheduler.Queue`, `scheduler.Budgets`), and
  the tenant quota enforcer and CAS guard (`tenancy.Enforcer`, `GuardCAS`).
  Each was finished while the file that would call it belonged to another
  change. This shape — everything built, one end unconnected — is the single
  most common defect this project has produced, and it is why each is named
  here rather than assumed done.
- **Nothing publishes to the catalog.** `catalog.Publish` has no caller outside
  tests, so a plugin's declaration can be read through `GetPlugin` and there is
  no supported way to put one there.
- **Four run-view end-to-end cases still fail**, and a fifth is skipped saying
  why: no operation builds a loop node, so there is no loop pipeline to look at.
- **Not started:** the three acceptance pipelines, the VM executor with snapshot
  restore, and multiplayer editing.

## Documentation

[`docs/`](docs/) — start with the [quickstart](docs/quickstart.md), whose every
command is executed by the test suite.

| Page                                              | For                                          |
| ------------------------------------------------- | -------------------------------------------- |
| [Quickstart](docs/quickstart.md)                  | a pipeline running in fifteen minutes        |
| [The engine wire contract](docs/wire-contract.md) | the protocol every engine is written against |
| [Writing an engine](docs/writing-an-engine.md)    | contract, engine, `make conformance`         |
| [Writing a plugin](docs/writing-a-plugin.md)      | manifests, capabilities, effect classes      |
| [Policy authoring](docs/policy.md)                | CEL rules and how to test them               |
| [Deployment topologies](docs/deployment.md)       | laptop, homelab, cluster, Helm               |
| [Upgrades and version skew](docs/upgrades.md)     | what N and N-1 obliges                       |

There is a reference page for every step type, trigger kind and executor kind,
and `docs/docs_test.go` enumerates them from the source tree — so a backend
added next month fails the gate until it is documented.

## Installing a release

Releases publish `dhole` and `dhole-engine` for Linux, macOS and Windows on
amd64 and arm64, multi-arch images on `ghcr.io`, and a Helm chart. Every image
is signed keylessly by the workflow that built it and carries an SPDX SBOM
attestation:

```
cosign verify ghcr.io/azrtydxb/dhole:<version> \
  --certificate-identity-regexp '^https://github.com/azrtydxb/dhole/\.github/workflows/release\.yml@' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

`helm install dhole charts/dhole` deploys the control plane, engines, and — for
a deployment that has not brought its own — NATS and Postgres. See
[deployment topologies](docs/deployment.md) for what to set before it matters.

## Stack

Go control plane · NATS + JetStream · SQLite or Postgres · protobuf over ConnectRPC ·
CEL for policy · React and React Flow.

## License

[Apache-2.0](LICENSE). See [ADR 0018](.procoder/adr/0018-dhole-is-licensed-apache-2-0.md)
for why, and what it gives up.

## Name

A dhole is a pack-hunting wild dog. The name was chosen for a clear namespace as much as
the metaphor — see [ADR 0017](.procoder/adr/0017-the-project-is-named-dhole.md) for the
alternatives and why each was rejected.
