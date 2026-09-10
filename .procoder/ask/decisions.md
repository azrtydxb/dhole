# Decisions

## Next step for the "build our own CI" idea (2026-09-09)

Context: discussed what a from-scratch Woodpecker alternative would improve —
content-addressed DAG execution replacing the shared mutable workspace, local==remote
execution, composable/dynamic config, non-ambient secrets, rootless isolation, OTel
observability, step-level debug attach, typed plugins, fair-share scheduling, provenance.

Options:

- Full spec via `/procoder:spec` — whole system: problem statement, execution model, non-goals, phasing.
- Narrow spec — the cache/DAG engine only, as a standalone local tool; server deferred until the caching win is proven.
- Prior-art research pass first — Dagger, BuildKit, Buildkite dynamic pipelines, Tekton, Nix; what is already solved before writing any spec.
- No artifact — leave it as discussion for now.

## Pipeline engine — four forking design questions (2026-09-09)

Architecture settled: Go control plane, NATS/JetStream bus, polyglot engines, event-sourced
durable runs, typed ports + derived DAG, pluggable executors/triggers, effect classes,
React + React Flow GUI on the same API agents use, schema as public contract.

### Audience and blast radius

- Personal + homelab only (single tenant, minimal auth)
- Internal tool for Kryton work (small team, self-hosted, real auth)
- Open-source product others self-host (versioned schema, conformance suite, docs, N-1 compat)
- Eventual commercial/SaaS (hard multi-tenancy from day one)

### Source of truth for a pipeline definition

- YAML in git; stable IDs; GUI does CST-preserving surgical edits
- Graph in the control-plane DB; YAML is import/export (n8n model)
- Real config language (CUE/Starlark/Pkl) compiled to the DAG; YAML a serialised subset

### First vertical slice / first profile

- CI for own repos
- Homelab + infra automation (triggers, schedules, durability)
- LLM/agent orchestration
- GUI + editor first, over a stub executor

### Content-addressed cache: v1 or later

- v1 core primitive
- Design the model now, implement after durability + engines + GUI
- Drop it; ordinary artifact passing is enough

### ANSWERED (2026-09-09)

- Audience: personal/homelab first + open-source self-host + eventual SaaS -> tenancy in the
  data model from day one; single-binary mode retained.
- Source of truth: YAML is the format; the STORE is pluggable (git backend or central server DB).
  Git not required. Every definition carries a revision identity; runs pin to it.
- First slice: all three profiles. Proposed acceptance test = one pipeline spanning all three.
- Cache: content-addressed CAS is a v1 core primitive; engines report input/output hashes.

## Pipeline engine — round 2 questions (2026-09-09)

### v1 acceptance pipeline

- The spanning pipeline proposed (schedule+API trigger, cached image build, LLM step with schema,
  human approval, k8s deploy, across three engine types)
- Something else the user names
- Three narrower per-profile acceptance pipelines

### Definition store primacy

- Git primary, DB backend secondary
- DB primary, git as sync/export
- True peers, neither privileged

### Secrets backend

- Built-in encrypted store only
- External only (Vault / OIDC federation / cloud KMS)
- Both, behind one interface

### Does the control plane binary host an engine?

- Yes, embedded engine for single-binary mode
- No, control plane strictly orchestrates; engine always a separate process
- Embedded but same protocol over the bus (loopback)

### ANSWERED round 2 (2026-09-09)

- Acceptance: three separate per-profile pipelines. Proposed build order CI -> homelab -> LLM.
- Store: DB is primary, git is a synced mirror kept up to date. => CST-preserving editing DROPPED.
- Secrets: both built-in and external behind one resolver; engines get short-lived refs, never values.
- Embedded engine in the control-plane binary, speaking the identical protocol over loopback bus.

## Pipeline engine — round 3 questions (2026-09-09)

### Git sync direction

- One-way mirror DB -> git (git read-only, edits there discarded)
- DB -> git as branch/PR for changes flagged as needing review
- Bidirectional with conflict resolution

### Review / approval of definition changes

- None: GUI save is canonical immediately
- In-app approval state on revisions (draft -> reviewed -> active)
- Environment promotion (revision pinned per environment, promote dev -> prod)

### Build order of the three acceptance pipelines

- CI -> homelab/infra -> LLM (recommended)
- Homelab/infra first
- LLM first
- Parallel

### CAS retention / eviction

- TTL + LRU eviction with per-tenant quota
- Refcount from run history, pin anything a retained run references
- Never evict; quota only, operator prunes manually

### ANSWERED round 3 (2026-09-09)

- Git sync: one-way mirror DB -> git; direct git edits discarded.
- Approval: in-app revision state draft -> reviewed -> active, with approver recorded.
- Build order: all three acceptance pipelines in PARALLEL => schema/contract is critical path.
- CAS reclamation: refcount from retained runs; anything a retained run references is pinned.

## Registry — round 4 questions (2026-09-09)

Design position: split runtime registry (engine instances, NATS KV, TTL heartbeat) from catalog
(step/plugin/engine/trigger types, versioned, in DB). Plugin versions resolved at definition-save
time into a per-revision lockfile of digests; never resolve tags at dispatch. Manifest declares
capabilities + effect class + schemas so policy can refuse before execution. Instance lifecycle
registering -> ready -> draining -> gone.

### Plugin artifact format

- OCI artifacts in a container registry (reuse Harbor)
- Own blob store in the CAS
- Both behind one resolver

### Public plugin index

- None; bring your own registry
- Hosted community index for the OSS distribution
- Federated: multiple upstreams, locally mirrored

### Signature enforcement

- Always required
- Required per trust tier
- Optional / warn only

### ANSWERED round 4 (2026-09-09)

- Artifacts: both OCI and CAS blob store behind one scheme'd resolver (oci:// , cas://).
  Signatures stored as detached catalog records keyed by artifact digest, cosign as one source.
- Index: federated upstreams, locally mirrored; plugin identity namespaced per upstream.
- Signing: enforced per trust tier via policy.
- EMERGENT: policy is now a first-class subsystem (engine availability, upstream allowlist,
  signing identities, plugin capabilities, effect classes) with one evaluation point + audit trail.

## Still open (2026-09-09)

- How policy is expressed (built-in DSL / OPA-Rego / Go-coded)
- API identity and auth (OIDC / built-in users / both)
- YAML surface for effect classes and taint without policy boilerplate on every step
- Bounded-loop semantics for agent steps
- Scheduler fairness algorithm
- Project name

## Next move

- Consolidate everything into a spec via /procoder:spec
- Keep discussing the open items first
- Write an architecture decision record set instead

## Project name (2026-09-09)

Criteria: short binary name, unclaimed in CI/infra, metaphor matching the architecture,
usable as a schema namespace.

- Shrike — larder/caching bird; matches CAS cache as core primitive
- Starling — murmuration; matches control plane / data plane emergent coordination
- Pika — haypile caching; short, warm
- Dhole — coordinated pack; unclaimed but generic metaphor
  Rejected for conflicts: Octopus (Octopus Deploy), Heron (Apache Heron), Kestrel (ASP.NET),
  Ant, Badger, Otter, Rook, Capybara.

### Name conflict check (2026-09-09) — "Pika"

Crowded: python `pika` AMQP/RabbitMQ client (same infra neighbourhood, worst overlap),
superhighfives/pika macOS colour picker, Pika Backup, PikaOS, Marmot Pika Discovery Layer,
Pika dynamic language, Pika-Software GH org. None are CI/workflow engines.

### NAME DECIDED (2026-09-09): Dhole

Pika reversed after conflict check (7 projects incl. python AMQP client in same neighbourhood).
Magpie ruled out (Apache Magpie + Open Raven Magpie, both dev tooling).
Dhole: only conflicts are a small remote-desktop tool and a PHP crypto lib. Binary `dhole`,
namespace `dhole.v1`. gh authed as piwi3910, org azrtydxb reachable, azrtydxb/dhole free.

## ADRs written 2026-09-09 — 17 records in .procoder/adr/, all `proposed`, adr check clean.

## Repo creation questions (2026-09-09)

### Visibility of azrtydxb/dhole

- Private now, public at first release
- Public immediately

### ADR status

- Accept all 17
- Review before accepting

## Post-repo open questions (2026-09-09)

Repo azrtydxb/dhole created public, 17 ADRs accepted and pushed. No LICENSE yet.

### License

- Apache-2.0 (permissive + patent grant, max adoption, no SaaS protection)
- AGPL-3.0 (network copyleft, preserves SaaS optionality; engines unaffected via bus boundary)
- BSL 1.1 (source-available, blocks competing hosted offerings, not OSI open source)
- MIT (simplest permissive, no patent grant)

### ADR 0012 — how policy is expressed

- CEL (as k8s ValidatingAdmissionPolicy)
- Embedded Rego / OPA
- Built-in declarative DSL
- Compiled Go

### ADR 0016 — does v1 scope stand

- Stands as written (three acceptance pipelines, parallel)
- Narrow to two profiles
- Sequence instead of parallel

### ANSWERED — post-repo questions (recorded 2026-09-10)

Recorded from the artifacts that carry each decision, not from memory. These were
answered at the time and written into ADRs and the tree; only this log went
unupdated, which is why `procoder ask` kept listing them as open.

- **License: Apache-2.0.** `LICENSE` is the Apache License 2.0, and ADR 0018 records
  the decision as accepted.
- **ADR 0012 / policy expression: CEL.** ADR 0019 ("policy is expressed in CEL") is
  accepted, and `internal/policy/cel.go` implements it with `github.com/google/cel-go`.
- **ADR 0016 / v1 scope: stands as written.** ADR 0016 is accepted and all three
  targets exist — `make acceptance-ci`, `acceptance-automation`, `acceptance-agent`.
- **Repo visibility: public.** `github.com/azrtydxb/dhole` is public.
- **ADR status: all accepted.** Every record in `.procoder/adr/` reads
  `Status: accepted` and `procoder adr check` is clean.

## Task 54, the VM executor — a scope decision I cannot make (2026-09-10)

Six open plan items build a Firecracker/QEMU VM executor and the strong-isolation
tier it enables. They are the only substantial work left in the plan, and I have
deliberately not started them, because the spec forbids them:

> **Out of scope** — VM executor (Firecracker/QEMU) and the strong-isolation tier it
> enables, including macOS, Windows, nested-virt and RouterOS CHR targets. Designed
> for by the executor interface, not implemented in v1.

The chain runs ADR → spec → plan → code, and it says that where reality contradicts
the spec, the spec is updated first. So building this needs a spec change, and that
is a product-scope call rather than an implementation one.

Two further facts worth having before deciding:

- ~~It cannot be verified here.~~ **CORRECTED 2026-09-10, after testing rather than
  assuming.** I wrote that the k3s cluster was "arm64 without nested virt exposed".
  That was an assumption I had not checked, and it is wrong: `/dev/kvm` is present
  on the kw nodes, KubeVirt v1.8.4 is installed, and a Fedora 42 aarch64 guest boots
  there under `<domain type='kvm'>` — hardware acceleration, not TCG emulation. All
  eight nodes advertise `devices.kubevirt.io/kvm`.

  So a VM executor CAN be exercised on real hardware: a privileged pod with
  `/dev/kvm` runs Firecracker or QEMU directly, which is what
  `DHOLE_TEST_FIRECRACKER_BIN` would point at. What remains true is that this
  MacBook cannot run it, so it would be a cluster-only test like the Kubernetes and
  containerd executor contracts already are.

- The interface is ready for it. The executor contract now has four backends
  (process, kubernetes, containerd, pool) and `Sandbox.EnvironmentIdentity` moved to
  the sandbox, so a VM backend has somewhere honest to report a snapshot digest.
  Nothing about the design is blocking.

### Options

- Leave Task 54 out of scope, as the spec says. The plan items stay open and
  annotated; nothing is lost.
- Move the VM executor into scope: amend `.procoder/specs/dhole.md`, re-run
  `procoder spec check`, and build it — accepting that CI needs a KVM-capable
  runner before any of it is verified rather than merely written.
- Build it behind the skip, as a designed-and-untested backend, and say so in
  the docs.

## Three step-type gaps that need a design decision (2026-09-10)

`dhole serve` now hosts its own step types — `builtin:wait`, `builtin:approval`,
`builtin:llm`, `builtin:loop` — where this morning it imported none of them. Three
gaps remain, and each is an architecture question rather than a wiring one, so I
have implemented none of them.

### (a) An agent step has no action space

`internal/steps/agent` has the taint check and the per-action approval, and no
`builtin:agent`, because an agent step needs an INVOKER for the actions it may take
and nothing supplies one. The question is what an agent is allowed to do: call
other pipelines, reach the API as its own principal, run a tool in a sandbox, or
something narrower.

- An agent invokes only Dhole itself — start a run, read a run, decide a gate —
  as a principal with its own quota and audit trail.
- An agent invokes a declared tool set, resolved like a plugin, run in a sandbox.
- Both, with the tool set gated by trust tier and the policy engine.

### (b) The plane cannot hold a model credential

`builtin:llm` needs `server.Config.Models`, and the CLI passes none. A model client
holds an API key, and this system deliberately leases the PLANE no secret: ADR 0010
says engines receive references, never values, and there is no plane-side
equivalent. A plane with no factory fails such a step with that exact reason rather
than pretending.

- Give the plane a secret resolver of its own — the same broker engines redeem
  against, with the plane as a principal.
- Configure model credentials as plain deployment configuration (env or a Secret),
  accepting that the plane holds a value at rest, and say so in the docs.
- Move the model call to an ENGINE step type, so the credential is leased the way
  every other secret already is.

### (c) A loop body is one builtin reference, not a subgraph

`config.body` names a single builtin. The definition format has no syntax for a
subgraph and no run can contain another, so a loop whose body dispatches to engines
needs nested runs — which is a change to what a run IS (ADR 0003), not a step type.

- Nested runs: a body is a pipeline, and an iteration starts a child run the parent
  waits on.
- Splice instead: reuse the generator machinery (`internal/dynamic`) so an iteration
  realises its body into the SAME run, which keeps one run one graph.
- Leave it: a loop body stays a single step, and anything larger is a pipeline the
  loop triggers.

My inclination is the splice for (c) — the machinery exists and it keeps one run one
graph — but it is your call, and (a) and (b) are more open than that.

## Proving the last two paths end to end (2026-09-10)

Everything else runs on the cluster and is watched running: two-step pipelines,
cache hits, approvals decided through the wire, cancellation, budget release on
failure, triggers firing and stopping, a loop body dispatching to an engine, and
all three acceptance pipelines with zero skips.

Two paths are proven in tests but have never completed a real pipeline on kw.
Both are configuration rather than code, and both need something only the owner
can supply.

### An agent step completing

`builtin:agent` reaches the model factory and refuses, correctly and by name, for
want of a credential. The action space, the loopback invoker, the taint plumbing
and the audit trail are covered by `internal/server/agent_e2e_test.go` against a
stub model. What has never happened is a real agent taking a real action.

Needs: a provider API key, supplied as `dhole serve --model-secret NAME=ENVVAR`
or the chart's `controlPlane.modelSecrets`. It is a real key with real cost, and
an agent that can `start_run` will start runs.

- Configure a model secret on the kw deployment and run an agent pipeline.
- Leave it — the stub-model e2e is enough for now.
- Configure it but grant only `read_run`, so nothing an agent does has an effect.

### A pipeline running inside a microVM

The VM executor passes the whole shared executor contract, 11 of 11, against real
Firecracker on an arm64 node with `/dev/kvm`. No engine is deployed with
`DHOLE_EXECUTOR=vm`, so no PIPELINE has run in a microVM.

Needs: a guest kernel and rootfs staged on the node, and an engine tier configured
for the vm backend. The images are built the way `docs/executors/vm.md` describes.

- Stage a kernel and rootfs on a node and deploy a vm-backed engine tier.
- Leave it — the executor contract against real Firecracker is the evidence that
  matters, and a pipeline adds deployment plumbing rather than proof.
- Do it in CI instead, on a KVM-capable runner, rather than on kw.
