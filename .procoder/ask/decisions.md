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
