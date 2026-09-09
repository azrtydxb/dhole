# Dhole

An everything-pipeline engine: durable, typed workflows that run anywhere, driven by
anything, editable by humans and agents alike.

> **Status: design.** There is no code yet. This repository currently holds the
> architecture decision records that define what is being built and why.

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
run store, filesystem object stores, and one engine hosted beside the control plane. The
engine is not called in-process — it dials the embedded bus and takes its work off the
same `job.dispatch.*` work queue an engine in another datacentre would, which is what
makes a laptop and a cluster the same system rather than two that resemble each other.

| Flag          | Default                            | What it selects                                         |
| ------------- | ---------------------------------- | ------------------------------------------------------- |
| `--mode`      | `embedded`                         | `embedded` or `distributed` (control plane only)        |
| `--store-dsn` | `<user config dir>/dhole/dhole.db` | a Postgres DSN, or any other value as a SQLite path     |
| `--bus-url`   | —                                  | the NATS server to dial; ignored in embedded mode       |
| `--blob-root` | `<user config dir>/dhole`          | where the CAS, the blob store and the embedded bus live |

`dhole version` prints the version and commit the binary was built from. SIGINT and
SIGTERM stop the plane rather than killing it: unacknowledged dispatches go back to the
queue and unsent outbox rows are still owed.

## Still open

API identity and auth, the YAML surface for effect classes and taint, and the scheduler's
fairness algorithm.

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
