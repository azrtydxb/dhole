# dhole

Status: ready

## Problem

Pipeline tooling is split into camps that cannot serve each other. CI systems
(GitLab CI, GitHub Actions, Woodpecker) assume a git commit starts the work, model a
pipeline as ordered steps sharing a mutable workspace, and therefore cannot cache
correctly, cannot parallelise safely, and cannot run locally — so debugging means pushing
commits. Automation tools (n8n, Zapier) handle arbitrary triggers and structured data but
have no notion of reproducibility, artifacts, or caching, and cannot express a build.
Durable workflow engines (Temporal) handle long-running side-effecting work but are
libraries, not systems anyone can drive from a UI. Nothing spans the three, so a team ends
up running all three and gluing them together.

Meanwhile LLM and agent workloads have no home at all: they are side-effecting,
non-deterministic, cost-bearing, need human approval gates and bounded loops, and expose a
prompt-injection path into whatever executes their decisions. Bolting them onto a CI
runner is unsafe; bolting them onto an automation tool loses reproducibility.

Dhole is one engine for all of it: a durable, typed workflow system where CI, automation,
and agent orchestration are profiles over a shared core.

## Users

- **Pipeline author (visual)** — builds and edits pipelines on a drag-and-drop canvas,
  connecting typed output ports to input ports. Needs immediate validation, a dry run that
  shows what would execute and what is cached, and a readable diff before saving.
- **Pipeline author (textual/API)** — writes YAML or drives the API directly. Needs the
  same capabilities as the GUI with no second-class endpoints.
- **AI agent** — an equal API client, editing pipelines alongside the user through
  intent-level operations that return diffs and inverses.
- **Operator / self-hoster** — installs and runs Dhole, from a single binary in a homelab
  to a clustered deployment. Needs low operational surface, clear failure signals, and
  policy control over what untrusted pipelines may do.
- **Engine author** — implements an execution backend in any language against the
  published wire schema and a conformance suite.
- **Plugin author** — publishes step, trigger, and engine types with schemas and signed
  artifacts to a registry.

## In scope

- [S-1] Typed content-addressed DAG execution: steps declare inputs and outputs, the graph
  is derived from typed port connections, execution order follows data dependencies.
- [S-2] Effect classes (`pure`, `idempotent`, `at-most-once`) governing cache eligibility
  and retry behaviour, including exclusive lease before side effect and fencing tokens.
- [S-3] Durable event-sourced runs: state persisted per transition, replay-based recovery,
  idempotent event application, waits that outlive the process.
- [S-4] Control plane (Go) and polyglot engines communicating only over the bus, with
  self-contained job messages and outbound-only engine connections.
- [S-5] NATS + JetStream transport: request/reply, durable work queues with pull consumers,
  ephemeral log streaming, KV instance registry, accounts mapped to trust tiers.
- [S-6] Pluggable executors with capability advertisement and explicit lease scopes
- [S-22] A VM executor (Firecracker, with QEMU as the portable fallback) providing the
  strong-isolation tier: a step runs in a microVM rather than a namespace, and its
  environment identity is the rootfs snapshot it booted from
  (`step`, `job`, `pipeline`, `pool`, `service`). v1 ships three: containerd/OCI, local
  process (also the in-process engine for single-binary mode), and Kubernetes.
- [S-7] Pluggable triggers bound to typed pipeline inputs. v1 ships four: schedule (cron),
  HTTP/API invoke and generic webhook, git webhook (push and PR), and pipeline completion.
- [S-8] Definition store: DB-primary with revision identity, one-way git mirror, in-app
  approval state (`draft -> reviewed -> active`).
- [S-9] Content-addressed store and cache-keyed execution, with refcount reclamation from
  retained runs.
- [S-10] Registry: ephemeral runtime instance registry plus durable catalog, per-revision
  plugin lockfile, engine lifecycle states.
- [S-11] Plugin resolution through one scheme-addressed resolver (`oci://`, `cas://`) with
  detached signature records and federated mirrored upstreams.
- [S-12] Policy subsystem in CEL, one evaluation point, keyed on trust tier, with audit.
- [S-13] One protobuf/ConnectRPC API contract serving GUI, CLI and agents, with
  operation-level editing, `validate`, and `plan`.
- [S-14] React + React Flow visual editor driving that API: full drag-and-drop authoring,
  schema-driven property panels, and a run view showing realised graph, cache hits,
  timings and streamed logs.
- [S-15] Tenancy in the data model throughout; single-binary mode with embedded NATS,
  SQLite and in-process engine over loopback.
- [S-16] Bounded loop nodes, taint tracking from untrusted triggers, constrained agent
  action space, LLM call recording and token budget ceilings.
- [S-17] Conformance suite that a third-party engine implementation must pass.
- [S-18] CLI with full parity to the API, generated from the same protobuf schema, so no
  operation is reachable from only one client.
- [S-19] Pluggable identity: built-in local users for single-binary and homelab
  deployments, OIDC federation for everything else, behind one interface; scoped service
  tokens for agents, CLI and CI in both modes.
- [S-20] Scheduler with weighted fair queuing and per-tenant and per-pipeline concurrency
  budgets, so no single tenant or pipeline can starve the fleet.
- [S-21] LLM and agent step types built on `github.com/azrtydxb/go-ai-sdk` (Apache-2.0,
  39 providers, zero root dependencies): `GenerateObject` for schema-validated structured
  output, the `agent` package's `maxSteps` for bounded iteration, its tool-call approval
  for gated actions, `AsTool` for exposing granted pipeline steps as agent tools, and the
  contrib OTel bridge for tracing. Dhole adds model fingerprint capture, token and cost
  accounting with per-run and per-pipeline ceilings, and full call recording.

## Out of scope

- Hosted/SaaS operation of Dhole itself. The data model supports tenancy (S-15) but no
  billing, signup, tenant provisioning, or public multi-tenant deployment is built.
- A public community plugin index. Federation (S-11) supports upstreams; we do not host one.
- Migration or import tooling from GitLab CI, GitHub Actions, Woodpecker, or n8n.
- Any forge integration beyond the one-way git mirror and a webhook trigger — no PR
  status reporting, no checks API, no forge-native review.
- Multiplayer/concurrent editing of a single pipeline definition.
- RouterOS CHR as a VM target, and the macOS and Windows VM hosts. The VM executor
  below runs on Linux with `/dev/kvm`; the other hosts need their own hypervisors and
  are not built.

## Constraints

- Scale: v1 is designed for platform scale — tens of thousands of runs per day, thousands
  of concurrent steps, hundreds of engines across multiple networks. Consequences that
  follow and are therefore v1 requirements: Postgres and clustered NATS are the primary
  supported deployment (SQLite and embedded NATS are a development and homelab target, not
  the tuned path); the control plane scales out with consumers partitioned by run id;
  scheduler fairness and backpressure are built rather than deferred; object storage for
  logs and artifacts is mandatory rather than optional.
- Platform: Linux on amd64 and arm64 for both the control plane and all v1 engines. No
  macOS or Windows engine in v1 (their host-process and VM paths are out of scope).
- Latency: a ready step reaches an engine in under one second at target load; a policy
  decision evaluates in under 10ms including cache lookup. Policy results are cached keyed
  on (tier, subject, policy revision).
- Security: engines never receive secret values, only short-lived references they redeem.
  Untrusted-tier work cannot reach privileged engines, unsigned plugins, or `at-most-once`
  steps without an approval gate. Subject-level authorization is enforced by the bus.
- Compatibility: the wire schema is a public contract; the control plane supports N-1
  engine protocol versions.
- Operational: single-binary deployment must work with no external dependencies.
- Toolchain: Go 1.26+, required by `go-ai-sdk` (S-21).

## Interfaces

- CLI has full parity with the API (S-18), generated from the protobuf definition rather
  than hand-maintained.
- Identity is pluggable (S-19): built-in users where there is no IdP, OIDC federation
  otherwise, with scoped service tokens in both. Both paths are secured and tested.
- Effect class and required capabilities are declared in the plugin manifest and inherited
  by the step; a step only writes them when it genuinely differs from its plugin. Ordinary
  pipelines therefore carry no policy boilerplate, and the safe value is the default rather
  than something an author must remember. An override that widens capability or weakens
  effect class is surfaced in the diff and is a policy decision point.
- gRPC and JSON-over-HTTP from one protobuf definition via ConnectRPC; generated Go server
  and TypeScript client; MCP tool definitions and OpenAPI derived from the same schemas.
- Operation-level editing API: `add_step`, `connect`, `set_property`, `remove_edge`,
  `rename`, each taking a document version and returning a diff plus its inverse.
- Read-only `validate` (structured diagnostics with source positions) and `plan` (resolved
  DAG, cache-hit prediction, engine assignment per step).
- Bus subject layout as an engine-author-facing contract: `job.dispatch.<tier>.<caps>`,
  `job.status.<run>.<step>`, `job.logs.<run>.<step>`, `engine.control.<engine-id>`,
  `engine.heartbeat.<engine-id>`.
- Executor interface: `acquire`, `exec`, `put`/`get`, `signal`, `release`.

## Data

- Run event log — append-only, per-transition, deduplicated on `(run, step, attempt,
sequence)`. SQLite single-node, Postgres clustered. Owned by the control plane.
- Pipeline definitions — DB-primary, YAML format, content-hash revision identity, approval
  state, plugin lockfile per revision. Mirrored one-way to git.
- Catalog — step/plugin/engine/trigger types, schemas, versions, detached signature
  records, upstream configuration. DB.
- Runtime engine registry — instance identity, capabilities, protocol versions, capacity,
  lifecycle state. NATS KV with heartbeat TTL. Ephemeral by design.
- Content-addressed store — step inputs, outputs, artifacts, plugin `cas://` artifacts.
  Refcounted from retained runs.
- Authoritative logs — object storage, written directly by engines, referenced by key.
- LLM call records — prompt, response, model fingerprint, params, tokens, latency.
- Retention is tiered by data class, with per-tenant overrides: run event logs longest
  (they are the audit trail), authoritative logs and artifacts shorter, CAS blobs held by
  refcount against retained runs rather than by time, and LLM call records configured
  separately because they hold prompt content.
- Every record carries a tenant scope.

## Edge cases

- Duplicate job delivery (JetStream at-least-once) against an `at-most-once` step.
- Network partition where the control plane declares a live engine dead and redispatches.
- Control plane restart mid-run; engine restart mid-step; bus unreachable from an engine.
- A `pool` lease reused across runs, making inputs unhashable — cacheability must degrade
  visibly rather than silently.
- Host-process engine with no honest environment identity.
- Dynamic pipelines whose graph does not exist until a generator step runs.
- Plugin upgrade changing a cache key; a tag that moved under a previously-resolved digest.
- Tainted data reaching an effectful step; an agent attempting a step outside its granted
  action space.
- Cache hit on a step whose recorded output blob has been garbage collected.
- Two engines claiming the same step after a partition; a zombie engine reporting a result
  for an attempt that has already been superseded.
- An engine registering with a protocol version the control plane no longer supports, or
  newer than it knows.
- A definition revision whose plugin lockfile references a digest no reachable registry
  mirror holds.
- A trigger firing while the previous run of the same pipeline is still active, against a
  concurrency budget of one.
- A schedule trigger whose window was missed entirely because the control plane was down.
- Clock skew between control plane and engines affecting lease expiry.
- A step producing an output larger than the object-storage part limit, or producing no
  output where one was declared.
- A run whose definition revision was superseded, or whose approval was revoked, while it
  was still executing.
- A bounded loop hitting its iteration ceiling with no exit condition satisfied.
- An LLM step whose provider returns output failing schema validation repeatedly, or whose
  token budget ceiling is reached mid-loop.
- A tenant deleted while runs, leases and CAS references are still live.
- A pipeline graph with an input port connected to an output of an incompatible type, or
  a generator step emitting a subgraph that references a step id that already exists.
- Log volume from a single step exceeding what the live subject can carry, requiring the
  UI to fall back to the stored object mid-stream.

## Failure modes

- **NATS unavailable** — engines buffer locally; control plane cannot dispatch or observe;
  in-flight work continues. NATS is the availability floor.
- **Control plane down** — in-flight work continues and publishes; pending work queues;
  recovery is replay from the durable consumer position. No run is lost.
- **Engine dies** — heartbeat lease expires, step becomes redeliverable; fencing tokens
  reject results from the zombie.
- **Object storage unavailable** — authoritative logs and large artifacts cannot be
  written; step must fail rather than report success with missing outputs.
- **Registry/upstream unreachable** — resolution falls back to local mirror; a plugin not
  mirrored blocks dispatch with a clear diagnostic.
- **Policy evaluation error** — fails closed: the step is not dispatched and the reason is
  recorded.
- **Postgres unavailable** — the control plane cannot advance any run state; it stops
  acking rather than losing events, and resumes by replay. Engines are unaffected.
- **Identity provider (OIDC) unavailable** — existing service tokens continue to work;
  interactive login fails with a clear message rather than falling back to a weaker path.
- **LLM provider unavailable, throttled, or returning malformed output** — the step retries
  under its effect class, then fails the run with the provider error recorded; it never
  silently returns a partial or unvalidated object.
- **Object storage slow** — log streaming degrades to live-only with a visible warning;
  artifact writes block the step rather than proceeding.
- **A plugin's signature fails verification** — the step is not dispatched, regardless of
  cached resolution, and the catalog entry is marked untrusted.
- **Git mirror push fails** — the definition revision is still authoritative in the DB; the
  mirror retries and reports drift rather than blocking the save.

## Acceptance criteria

- [ ] [S-1] A pipeline whose steps declare inputs and outputs executes in dependency order
      derived from its port connections, and two steps with no path between them run
      concurrently — `TestDAGDerivedFromPortsRunsIndependentStepsConcurrently`.
- [ ] [S-1] [S-13] Connecting an output port to an input port of an incompatible type is
      rejected by `dhole validate` with a diagnostic naming both ports, before any run
      starts — `TestValidateRejectsIncompatiblePortTypes`.
- [ ] [S-2] A step declared `at-most-once` delivered twice executes exactly once; the second
      delivery is rejected by its fencing token and recorded —
      `TestAtMostOnceRejectsDuplicateDeliveryByFence`.
- [ ] [S-2] A step declared `idempotent` is retried automatically on engine death and
      carries the same idempotency key on both attempts —
      `TestIdempotentRetryReusesIdempotencyKey`.
- [ ] [S-3] Killing the control plane mid-run and restarting it completes the run with no
      duplicated or lost transitions in the event log —
      `TestControlPlaneRestartMidRunReplaysWithoutDuplication`.
- [ ] [S-3] A pipeline containing a wait resumes after the control plane is restarted during
      that wait — `TestDurableWaitSurvivesRestart`.
- [ ] [S-4] With the control plane stopped, a running engine completes its step and
      publishes status and logs; on restart the control plane replays them and advances the
      run — `TestEngineContinuesWhileControlPlaneDown`.
- [ ] [S-4] An engine on a network with no inbound reachability registers, receives work and
      reports results — `TestOutboundOnlyEngineRegistersAndExecutes`.
- [ ] [S-5] A step dispatched to a tier an engine is not credentialled for is refused at the
      bus subject level rather than by application code —
      `TestTierSubjectPermissionRefusesForeignDispatch`.
- [ ] [S-6] The same pipeline runs unmodified on the containerd, local-process and
      Kubernetes engines, and `dhole plan` reports the engine each step lands on —
      `TestSamePipelineAcrossThreeEngines`.
- [ ] [S-22] The VM executor satisfies the same shared executor contract every other
      backend does, against a real hypervisor on a host with `/dev/kvm` —
      `TestVMExecutorContract`. A microVM's environment identity is the rootfs snapshot
      it booted from, so two steps on different snapshots do not share a cache key —
      `TestEnvironmentIdentityIsSnapshotDigest`.
- [ ] [S-22] `Capabilities()` omits nested virtualisation on a host that cannot grant it,
      rather than advertising a promise the backend cannot keep —
      `TestNestedVirtCapabilityIsAdvertisedOnlyWhenAvailable`.
- [ ] [S-6] A step run under a `pool` lease is reported non-cacheable with its reason
      visible in the run view — `TestPoolLeaseMarksStepNonCacheable`.
- [ ] [S-7] One pipeline is started by cron, an HTTP call, a git webhook and another
      pipeline's completion with no change to its definition —
      `TestAllFourTriggersStartSamePipeline`.
- [ ] [S-8] A GUI edit produces a new revision with content-hash identity, a reviewable diff
      and an approval state, and the git mirror receives a commit containing that YAML —
      `TestRevisionCreatedAndMirroredOnEdit`.
- [ ] [S-8] An edit made directly in the mirrored git repository does not alter the
      authoritative definition — `TestGitMirrorEditsAreNotAuthoritative`.
- [ ] [S-9] Re-running with unchanged inputs skips `pure` steps and reports cache hits;
      changing one input re-executes exactly that step and its dependents —
      `TestCacheHitOnUnchangedInputsAndInvalidationOnChange`.
- [ ] [S-9] A CAS blob referenced by a retained run is not collected; one referenced only by
      an expired run is — `TestRefcountGCPreservesRetainedRunBlobs`.
- [ ] [S-10] An engine that stops heartbeating leaves the runtime registry within its TTL and
      its in-flight step becomes redeliverable, while the catalog survives a full control
      plane restart — `TestHeartbeatExpiryRedeliversAndCatalogPersists`.
- [ ] [S-10] Saving a definition writes a lockfile pinning every plugin to an exact version
      and digest, and a moved upstream tag does not change what an existing revision runs —
      `TestLockfilePinsPluginDigestsAgainstMovedTag`.
- [ ] [S-11] Plugins referenced by `oci://` and by `cas://` are resolved, signature-verified
      and executed through the same code path — `TestResolverHandlesBothSchemesUniformly`.
- [ ] [S-12] A policy denial names the rule, tier and subject in the audit log, and
      evaluation completes within the 10ms budget under load —
      `TestPolicyDenialAuditedWithinLatencyBudget`.
- [ ] [S-12] A step requiring a capability its tier forbids is refused at definition-save
      time, not at dispatch — `TestForbiddenCapabilityRefusedAtSave`.
- [ ] [S-13] Every operation the GUI performs runs against the same public API from a
      scripted client, with no GUI-only endpoint —
      `TestNoPrivilegedGUIEndpoints`.
- [ ] [S-13] An `add_step` operation returns a diff and an inverse, and applying the inverse
      restores the previous revision exactly — `TestOperationInverseRestoresRevision`.
- [ ] [S-14] `TestCanvasAuthorsPipelineEndToEnd` (Playwright `e2e/canvas-authoring.spec.ts`):
      a pipeline is authored entirely on the canvas — nodes added, ports connected,
      properties set from schema-driven panels — and the run view shows the realised graph
      with per-step cache hits, timings and streamed logs; fails if any authoring action
      cannot be completed on the canvas, or the run view omits cache-hit, timing or log data
      for any step.
- [ ] [S-15] The single binary starts with no external dependency and runs a pipeline through
      its in-process engine over the loopback bus; the same build runs against external NATS
      and Postgres unchanged — `TestSingleBinaryAndDistributedParity`.
- [ ] [S-15] Every stored record and bus subject carries a tenant scope, and a second tenant
      cannot read or dispatch the first tenant's work — `TestTenantIsolationAcrossStoreAndBus`.
- [ ] [S-16] A bounded loop stops at its iteration ceiling and reports why, and an agent step
      attempting a step outside its granted action space is refused —
      `TestBoundedLoopCeilingAndActionSpaceRefusal`.
- [ ] [S-16] Data from an untrusted trigger is refused entry to an effectful step until it
      passes an explicit sanitisation gate — `TestTaintBlocksEffectfulStepUntilSanitised`.
- [ ] [S-17] `TestConformanceMinimalPythonEngine` (`make conformance` against
      `testdata/engines/minimal-python`): a deliberately minimal third-party engine written
      in a language other than Go passes the conformance suite and executes a step end to
      end; fails if any conformance case requires Go-specific behaviour, or the engine
      cannot complete a step without control-plane calls beyond the bus.
- [ ] [S-18] Every API operation has a CLI equivalent, asserted by a generated test that
      enumerates the protobuf service — `TestCLICoversEveryRPC`.
- [ ] [S-19] One deployment authenticates a human via OIDC and an agent via a scoped service
      token, and the token still works with the IdP unreachable —
      `TestOIDCAndServiceTokenWithIdPDown`.
- [ ] [S-20] Under saturation a tenant exceeding its concurrency budget does not delay another
      tenant's steps beyond the sub-second dispatch target —
      `TestWeightedFairQueuingUnderSaturation`.
- [ ] [S-21] An LLM step returns an object validated against its declared schema, records
      model fingerprint, prompt, response, tokens and latency, and halts the run at its token
      ceiling — `TestLLMStepSchemaFingerprintAndBudgetCeiling`.
- [ ] [S-1] [S-9] [S-6] `TestAcceptanceCICacheHit` (`make acceptance-ci`): the CI acceptance
      pipeline builds a container image and hits the cache on a second run with unchanged
      inputs; fails if the second run rebuilds the image or reports no cache hit.
- [ ] [S-3] [S-7] [S-20] `TestAcceptanceAutomationTriggersAndWait`, run by the
      `acceptance-automation` target: the automation acceptance pipeline runs from a schedule
      and an API trigger, holds a long wait, and executes across two engine types; fails if
      either trigger does not start the pipeline, the wait does not survive a control-plane
      restart, or both steps land on the same engine type.
- [ ] [S-16] [S-21] `TestAcceptanceAgentLoopAndApproval` (`make acceptance-agent`): the agent
      acceptance pipeline produces schema-validated structured output, bounds its loop, gates
      on human approval and records token cost; fails if unvalidated output is accepted, the
      loop exceeds its ceiling, the approval gate can be bypassed, or token cost is not
      recorded.

## Open questions
