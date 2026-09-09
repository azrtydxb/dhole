# dhole — implementation plan

Status: draft
Spec: .procoder/specs/dhole.md

## Goal

Build Dhole end to end: a durable, typed, content-addressed workflow engine with polyglot
engines over a message bus, a visual editor, a registry, and CI, automation and agent
profiles — through every phase, including the capabilities the spec places beyond v1.

## Architecture

A Go control plane owns the DAG, scheduling, policy, the API and the GUI; it never calls an
engine directly. Engines are separate processes in any language that pull self-contained
jobs from NATS/JetStream and publish status and logs back, so a control-plane restart is a
replay rather than a loss. Steps declare typed inputs and outputs, making the graph derived
rather than authored and the content-addressed cache correct by construction; effect classes
decide what may be cached and what may be retried.

## Constraints

- Go 1.26+ (required by `github.com/azrtydxb/go-ai-sdk`). Node 22+ for the web app.
- Linux amd64 and arm64 for the control plane and all engines through Phase 9. macOS and
  Windows engines arrive in Phase 10 only.
- Licence Apache-2.0. Every new source file carries no licence header; the repository
  `LICENSE` governs.
- Module path `github.com/azrtydxb/dhole`. Protobuf package `dhole.v1`. Binary `dhole`.
- The wire schema is a public contract: additive changes only within a major version, and
  the control plane must accept engines speaking version N and N-1.
- Every stored record and every bus subject carries a tenant scope. There is no unscoped
  query and no unscoped subject, even while only one tenant exists.
- Engines never receive secret values, only short-lived references they redeem.
- Policy evaluation must complete within 10ms including cache lookup; a ready step must
  reach an engine within one second at target load.
- Postgres and clustered NATS are the tuned deployment target; SQLite and embedded NATS are
  supported for development and homelab and must remain functional.
- Tests are Go standard `testing` with `testify/require`. Web tests are Playwright.
- Every task ends gate-clean: `make check` (gofmt, go vet, golangci-lint, buf lint) passes.

## Task 1: Repository scaffold and quality gate

Files: `go.mod`, `Makefile`, `.golangci.yml`, `.github/workflows/ci.yml`, `internal/version/version.go`, `internal/version/version_test.go`
Interfaces: produces `version.Version() string`, `version.Commit() string`; `make check`, `make test` for every later task.

- [ ] Write `internal/version/version_test.go` asserting `TestVersionIsSetAtBuild`: `require.NotEmpty(t, version.Version())` and `require.NotEqual(t, "unknown", version.Version())` when built with ldflags. Run `go test ./internal/version` — expect FAIL with "undefined: version.Version".
- [ ] Create `go.mod` with `module github.com/azrtydxb/dhole` and `go 1.26`; add `github.com/stretchr/testify`.
- [ ] Implement `internal/version/version.go` with `var version, commit = "unknown", "unknown"` and exported `Version()`/`Commit()` accessors.
- [ ] Write `Makefile` with targets `check` (gofmt -l with non-empty failure, go vet, golangci-lint run, buf lint), `test` (`go test ./...`), `build` (ldflags setting `internal/version.version` and `.commit`).
- [ ] Write `.golangci.yml` enabling `errcheck, govet, staticcheck, ineffassign, unused, gosec, revive` and `.github/workflows/ci.yml` running `make check test` on `ubuntu-latest` for amd64 and arm64.
- [ ] Run `make build && ./dhole` then `go test ./internal/version` — expect PASS. Commit.

## Task 2: Core protobuf schema for pipelines

Files: `proto/dhole/v1/pipeline.proto`, `proto/dhole/v1/common.proto`, `buf.yaml`, `buf.gen.yaml`, `gen/dhole/v1/` (generated), `internal/schema/schema_test.go`
Interfaces: produces messages `Pipeline`, `Step`, `Port`, `PortType`, `Edge`, `Tenant`, enum `EffectClass` (`EFFECT_CLASS_PURE`, `EFFECT_CLASS_IDEMPOTENT`, `EFFECT_CLASS_AT_MOST_ONCE`), enum `Capability`; all later tasks import `gen/dhole/v1`.

- [ ] Write `internal/schema/schema_test.go` asserting `TestEffectClassEnumValues`: `require.Equal(t, 1, int(dholev1.EffectClass_EFFECT_CLASS_PURE))` and that `EffectClass_name` has exactly four entries including the zero `EFFECT_CLASS_UNSPECIFIED`. Run `go test ./internal/schema` — expect FAIL with "no required module provides package .../gen/dhole/v1".
- [ ] Write `proto/dhole/v1/common.proto` defining `Tenant{string id}`, `Digest{string algo; string hex}`, `EffectClass`, `Capability` (`NETWORK`, `SECRETS`, `PRIVILEGED`, `HOST_MOUNT`).
- [ ] Write `proto/dhole/v1/pipeline.proto` defining `Pipeline{string id; Tenant tenant; repeated Step steps; repeated Edge edges}`, `Step{string id; string name; string plugin_ref; EffectClass effect_class; repeated Port inputs; repeated Port outputs; repeated Capability capabilities}`, `Port{string name; PortType type}`, `PortType{oneof{BlobType blob; StructType structured}}`, `Edge{string from_step; string from_port; string to_step; string to_port}`.
- [ ] Write `buf.yaml` (lint `DEFAULT`, breaking `WIRE_JSON`) and `buf.gen.yaml` emitting `protocolbuffers/go` and `connectrpc/go` into `gen/`. Run `buf generate`.
- [ ] Run `go test ./internal/schema` — expect PASS. Add `buf lint` and `buf breaking --against '.git#branch=main'` to `make check`. Commit.

## Task 3: DAG derivation and port type checking

Files: `internal/dag/dag.go`, `internal/dag/typecheck.go`, `internal/dag/dag_test.go`, `internal/dag/typecheck_test.go`
Interfaces: produces `dag.Build(p *dholev1.Pipeline) (*dag.Graph, error)`, `Graph.TopoLevels() [][]string`, `Graph.Dependents(stepID string) []string`, `dag.TypeCheck(p *dholev1.Pipeline) []dag.Diagnostic`, `Diagnostic{StepID, PortName, Message string; Line, Col int}`.

- [ ] Write `internal/dag/dag_test.go` asserting `TestDAGDerivedFromPortsRunsIndependentStepsConcurrently`: a four-step pipeline where B and C both depend on A and D depends on both yields `TopoLevels()` of `[["a"],["b","c"],["d"]]`. Run `go test ./internal/dag` — expect FAIL with "undefined: dag.Build".
- [ ] Write `internal/dag/dag_test.go` case `TestDAGRejectsCycle` asserting `dag.Build` on a pipeline whose edges form a cycle returns an error containing "cycle: a -> b -> a".
- [ ] Implement `internal/dag/dag.go`: build adjacency from `Edge`, detect cycles by DFS colouring, compute levels by Kahn's algorithm with deterministic ordering (sort step ids within a level).
- [ ] Write `internal/dag/typecheck_test.go` asserting `TestValidateRejectsIncompatiblePortTypes`: connecting a `blob` output to a `structured` input yields exactly one `Diagnostic` whose `Message` contains both `"a.out"` and `"b.in"`. Run — expect FAIL with "undefined: dag.TypeCheck".
- [ ] Implement `internal/dag/typecheck.go` comparing `PortType` oneof arms and, for `structured`, comparing JSON Schema `$id`; return one diagnostic per bad edge, plus one per edge referencing a missing step or port.
- [ ] Run `go test ./internal/dag` — expect PASS. Commit.

## Task 4: Event-sourced run store with SQLite

Files: `internal/runstore/store.go`, `internal/runstore/sqlite.go`, `internal/runstore/migrations/0001_init.sql`, `internal/runstore/sqlite_test.go`
Interfaces: produces `runstore.Store` interface with `Append(ctx, tenantID string, e Event) error`, `Replay(ctx, tenantID, runID string) ([]Event, error)`, `LastSequence(ctx, tenantID string) (uint64, error)`; `Event{RunID, StepID string; Attempt uint32; Sequence uint64; Type EventType; Payload []byte; At time.Time}`.

- [ ] Write `internal/runstore/sqlite_test.go` asserting `TestControlPlaneRestartMidRunReplaysWithoutDuplication`: append five events, close the store, reopen it, replay, and `require.Len(t, events, 5)` in sequence order. Run — expect FAIL with "undefined: runstore.NewSQLite".
- [ ] Add `TestAppendIsIdempotentOnDuplicateSequence`: appending the same `(RunID, StepID, Attempt, Sequence)` twice returns nil both times and `Replay` still yields one event.
- [ ] Write `internal/runstore/migrations/0001_init.sql` creating `run_events(tenant_id, run_id, step_id, attempt, sequence, type, payload, at)` with `PRIMARY KEY (tenant_id, run_id, step_id, attempt, sequence)` and index on `(tenant_id, sequence)`.
- [ ] Implement `internal/runstore/store.go` (interface, `Event`, `EventType` constants `RUN_CREATED`, `STEP_READY`, `STEP_DISPATCHED`, `STEP_SUCCEEDED`, `STEP_FAILED`, `RUN_COMPLETED`) and `internal/runstore/sqlite.go` using `modernc.org/sqlite`, with `INSERT ... ON CONFLICT DO NOTHING` for idempotency.
- [ ] Add `TestQueryWithoutTenantIsRejected` asserting `Replay(ctx, "", runID)` returns an error containing "tenant scope required".
- [ ] Run `go test ./internal/runstore` — expect PASS. Commit.

## Task 5: Postgres run store

Files: `internal/runstore/postgres.go`, `internal/runstore/postgres_test.go`, `internal/runstore/migrations/0001_init.postgres.sql`, `docker-compose.test.yml`
Interfaces: produces `runstore.NewPostgres(ctx, dsn string) (runstore.Store, error)` satisfying the same `runstore.Store` interface as Task 4.

- [ ] Write `internal/runstore/postgres_test.go` with `TestPostgresSatisfiesStoreContract` running the identical assertions as Task 4's tests against a Postgres DSN from `DHOLE_TEST_POSTGRES_DSN`, skipping with `t.Skip("DHOLE_TEST_POSTGRES_DSN not set")` when absent. Run — expect FAIL with "undefined: runstore.NewPostgres".
- [ ] Extract Task 4's assertions into `internal/runstore/contract_test.go` exposing `runStoreContract(t *testing.T, s runstore.Store)` and call it from both the SQLite and Postgres tests, so the two implementations are held to one contract.
- [ ] Write `0001_init.postgres.sql` mirroring the SQLite schema with `BIGINT` sequences and `TIMESTAMPTZ`.
- [ ] Implement `internal/runstore/postgres.go` with `jackc/pgx/v5`, using `INSERT ... ON CONFLICT DO NOTHING`.
- [ ] Write `docker-compose.test.yml` starting `postgres:17` on port 55432 and add `make test-integration` exporting the DSN and running `go test ./... -tags=integration`.
- [ ] Run `make test-integration` — expect PASS. Commit.

## Task 6: Content-addressed store

Files: `internal/cas/cas.go`, `internal/cas/filesystem.go`, `internal/cas/cas_test.go`
Interfaces: produces `cas.Store` interface with `Put(ctx, tenantID string, r io.Reader) (dholev1.Digest, error)`, `Get(ctx, tenantID string, d dholev1.Digest) (io.ReadCloser, error)`, `Has(ctx, tenantID string, d dholev1.Digest) (bool, error)`; `cas.NewFilesystem(root string) cas.Store`.

- [ ] Write `internal/cas/cas_test.go` asserting `TestPutIsContentAddressedAndStable`: putting the same bytes twice yields an identical digest and `Has` reports true; putting different bytes yields a different digest. Run — expect FAIL with "undefined: cas.NewFilesystem".
- [ ] Add `TestGetMissingDigestReturnsNotFound` asserting `Get` on an absent digest returns an error satisfying `errors.Is(err, cas.ErrNotFound)`.
- [ ] Add `TestTenantsCannotReadEachOthersBlobs`: put bytes as tenant `a`, then `Has(ctx, "b", digest)` returns false.
- [ ] Implement `internal/cas/cas.go` (interface, `ErrNotFound`) and `internal/cas/filesystem.go` writing to `<root>/<tenant>/<algo>/<hex[:2]>/<hex>` via a temp file plus atomic rename, computing SHA-256 while streaming.
- [ ] Run `go test ./internal/cas` — expect PASS. Commit.

## Task 7: Object storage for logs and artifacts

Files: `internal/blobstore/blobstore.go`, `internal/blobstore/s3.go`, `internal/blobstore/filesystem.go`, `internal/blobstore/blobstore_test.go`
Interfaces: produces `blobstore.Store` with `Write(ctx, tenantID, key string, r io.Reader) error`, `Read(ctx, tenantID, key string) (io.ReadCloser, error)`, `URL(ctx, tenantID, key string, ttl time.Duration) (string, error)`; `blobstore.NewS3(cfg S3Config)`, `blobstore.NewFilesystem(root string)`.

- [ ] Write `internal/blobstore/blobstore_test.go` with `blobStoreContract(t *testing.T, s blobstore.Store)` asserting write-then-read round-trips bytes and reading an absent key returns `blobstore.ErrNotFound`; call it for the filesystem implementation as `TestFilesystemBlobStoreContract`. Run — expect FAIL with "undefined: blobstore.NewFilesystem".
- [ ] Add `TestS3BlobStoreContract` running the same contract against MinIO from `DHOLE_TEST_S3_ENDPOINT`, skipping when unset; add MinIO to `docker-compose.test.yml` on port 59000.
- [ ] Implement `internal/blobstore/blobstore.go`, `filesystem.go`, and `s3.go` using `aws-sdk-go-v2` with path-style addressing so MinIO works.
- [ ] Add `TestWriteFailureIsReportedNotSwallowed` asserting a write to a read-only root returns a non-nil error mentioning the key.
- [ ] Run `make test-integration` — expect PASS. Commit.

## Task 8: Engine wire protocol and version negotiation

Files: `proto/dhole/v1/engine.proto`, `internal/wire/negotiate.go`, `internal/wire/negotiate_test.go`, `docs/wire-contract.md`
Interfaces: produces messages `JobDispatch`, `JobStatus`, `LogChunk`, `EngineRegistration`, `EngineHeartbeat`, `EngineControl`; `wire.ProtocolVersion = 1`, `wire.Negotiate(engineVersions []uint32) (uint32, error)`.

- [ ] Write `internal/wire/negotiate_test.go` asserting `TestNegotiateAcceptsCurrentAndPrevious`: `Negotiate([]uint32{1})` returns 1; `Negotiate([]uint32{0})` returns an error containing "unsupported protocol"; with `ProtocolVersion` at 2, `Negotiate([]uint32{1})` returns 1. Run — expect FAIL with "undefined: wire.Negotiate".
- [ ] Write `proto/dhole/v1/engine.proto`: `JobDispatch{string run_id; string step_id; uint32 attempt; string fence_token; Step step; repeated InputRef inputs; repeated SecretRef secrets; string output_prefix; uint32 protocol_version}`, `JobStatus{string run_id; string step_id; uint32 attempt; string fence_token; Phase phase; int32 exit_code; repeated OutputRef outputs; string error}`, `LogChunk{string run_id; string step_id; uint64 seq; bytes data; Stream stream}`, `EngineRegistration{string engine_id; repeated uint32 protocol_versions; repeated Capability capabilities; string os; string arch; uint32 slots; repeated string engine_types}`, `EngineHeartbeat{string engine_id; repeated InFlight in_flight}`, `EngineControl{oneof{Cancel cancel; Drain drain; Attach attach}}`.
- [ ] Implement `internal/wire/negotiate.go` selecting the highest mutually supported version, refusing anything below `ProtocolVersion-1`.
- [ ] Write `docs/wire-contract.md` documenting every message field, the subject layout `job.dispatch.<tier>.<caps>`, `job.status.<run>.<step>`, `job.logs.<run>.<step>`, `engine.control.<engine-id>`, `engine.heartbeat.<engine-id>`, and the rule that a `JobDispatch` is self-contained.
- [ ] Run `go test ./internal/wire && buf lint` — expect PASS. Commit.

## Task 9: Executor interface and local process engine

Files: `internal/executor/executor.go`, `internal/executor/process/process.go`, `internal/executor/process/process_test.go`, `internal/executor/contract_test.go`
Interfaces: produces `executor.Executor` with `Acquire(ctx, spec Spec) (Sandbox, error)`, `Capabilities() []dholev1.Capability`, `Kind() string`; `executor.Sandbox` with `Exec(ctx, cmd Cmd) (ExitCode int32, err error)`, `Put(ctx, name string, r io.Reader) error`, `Get(ctx, name string) (io.ReadCloser, error)`, `Signal(ctx, sig Signal) error`, `Release(ctx) error`; `executor.LeaseScope` constants `LeaseStep`, `LeaseJob`, `LeasePipeline`, `LeasePool`, `LeaseService`.

- [ ] Write `internal/executor/contract_test.go` exposing `executorContract(t *testing.T, e executor.Executor)` asserting: a sandbox runs `echo hi` with exit 0 and stdout `hi`; a command exiting 3 reports `ExitCode == 3`; `Signal(SIGTERM)` during `sleep 30` returns within 2s; `Put` then `Get` round-trips bytes; `Release` twice is not an error.
- [ ] Write `internal/executor/process/process_test.go` calling `executorContract` as `TestProcessExecutorContract`. Run — expect FAIL with "undefined: process.New".
- [ ] Implement `internal/executor/executor.go` with the interfaces, `Spec{Image string; Env map[string]string; WorkDir string; Lease LeaseScope; Requirements Requirements}` and `Requirements{OS, Arch string; Capabilities []dholev1.Capability}`.
- [ ] Implement `internal/executor/process/process.go` running commands via `os/exec` in a temp directory, propagating `SIGTERM` to the process group with `Setpgid`, and reporting `Capabilities()` as `{}` — no privileged, no host mount.
- [ ] Add `TestProcessExecutorReportsNoEnvironmentIdentity` asserting `process.New().EnvironmentIdentity()` returns `("", executor.ErrNoStableIdentity)` so Task 16 can mark its steps non-cacheable.
- [ ] Run `go test ./internal/executor/...` — expect PASS. Commit.

## Task 10: Embedded NATS and bus client

Files: `internal/bus/bus.go`, `internal/bus/nats.go`, `internal/bus/embedded.go`, `internal/bus/subjects.go`, `internal/bus/nats_test.go`
Interfaces: produces `bus.Bus` with `Publish(ctx, subject string, msg proto.Message) error`, `Request(ctx, subject string, msg proto.Message, out proto.Message) error`, `SubscribePull(ctx, stream, consumer, subject string) (Subscription, error)`, `SubscribeEphemeral(ctx, subject string, fn func([]byte)) (func(), error)`; `bus.StartEmbedded(dir string) (*bus.Embedded, error)`; `bus.SubjectDispatch(tier, capsHash string) string` and siblings in `subjects.go`.

- [ ] Write `internal/bus/nats_test.go` asserting `TestEmbeddedBusRoundTripsRequestReply`: start embedded NATS, register a responder on `engine.control.e1`, `Request` returns the reply. Run — expect FAIL with "undefined: bus.StartEmbedded".
- [ ] Add `TestPullConsumerRedeliversUnackedMessage`: publish to a work-queue stream, receive without acking, close the subscription, resubscribe, and require the same message is delivered again.
- [ ] Implement `internal/bus/subjects.go` with the exact subject builders from `docs/wire-contract.md` and a `TestSubjectsMatchDocumentedContract` asserting `SubjectDispatch("untrusted","abc") == "job.dispatch.untrusted.abc"`.
- [ ] Implement `internal/bus/embedded.go` running `nats-server` in-process with JetStream on a temp dir, and `internal/bus/nats.go` wrapping `nats.go` plus `jetstream` for pull consumers.
- [ ] Add `TestEngineCannotSubscribeToForeignTier` asserting a connection with credentials scoped to `untrusted` receives a permissions error subscribing to `job.dispatch.trusted.*`.
- [ ] Run `go test ./internal/bus` — expect PASS. Commit.

## Task 11: Outbox bridging store and bus

Files: `internal/outbox/outbox.go`, `internal/outbox/outbox_test.go`, `internal/runstore/migrations/0002_outbox.sql`
Interfaces: produces `outbox.Outbox` with `Enqueue(ctx, tx runstore.Tx, tenantID, subject string, msg proto.Message) error`, `Drain(ctx) (int, error)`; `runstore.Store.WithTx(ctx, fn func(Tx) error) error` added to Task 4's interface.

- [ ] Write `internal/outbox/outbox_test.go` asserting `TestEventAndPublishCommitTogether`: within one `WithTx`, append an event and enqueue a message, then force the transaction to roll back and require neither is present. Run — expect FAIL with "undefined: outbox.New".
- [ ] Add `TestDrainPublishesThenMarksSent` asserting `Drain` publishes to the bus and a second `Drain` returns 0.
- [ ] Add `TestDrainRetriesAfterBusFailure`: with the bus stopped, `Drain` returns an error and leaves the row unsent; after restarting the bus, `Drain` publishes it.
- [ ] Write migration `0002_outbox.sql` creating `outbox(id, tenant_id, subject, payload, created_at, sent_at NULL)` with an index on `sent_at IS NULL`.
- [ ] Implement `WithTx` on both store implementations and `internal/outbox/outbox.go` with a polling drainer on a 200ms ticker.
- [ ] Run `go test ./internal/outbox` — expect PASS. Commit.

## Task 12: Engine agent runtime

Files: `internal/engine/agent.go`, `internal/engine/registry_client.go`, `internal/engine/agent_test.go`, `cmd/dhole-engine/main.go`
Interfaces: produces `engine.Agent` with `Run(ctx) error`, `engine.Config{EngineID, Tier string; Bus bus.Bus; Executor executor.Executor; Blobs blobstore.Store; CAS cas.Store; Slots int}`; publishes `EngineRegistration` on start and `EngineHeartbeat` every 5s.

- [ ] Write `internal/engine/agent_test.go` asserting `TestOutboundOnlyEngineRegistersAndExecutes`: start an embedded bus, run an agent with the process executor, publish a `JobDispatch` running `echo hi`, and require a `JobStatus` with `Phase_SUCCEEDED` and exit 0 arrives on `job.status.<run>.<step>`. Run — expect FAIL with "undefined: engine.Agent".
- [ ] Add `TestAgentStreamsLogsToEphemeralSubjectAndWritesAuthoritativeCopy`: run `printf 'a\nb\n'`, require two `LogChunk` messages on `job.logs.<run>.<step>` and that the blobstore holds the full output at the key named in `JobStatus`.
- [ ] Add `TestAgentHeartbeatsListInFlightSteps` asserting a heartbeat during a `sleep 5` step includes that step in `in_flight`.
- [ ] Add `TestAgentRefusesDispatchWithUnsupportedProtocolVersion` asserting a `JobDispatch` with `protocol_version: 99` yields a `JobStatus` with `Phase_FAILED` and error containing "unsupported protocol".
- [ ] Implement `internal/engine/agent.go`: pull consumer on the dispatch subject filtered by capability hash, acquire a sandbox per the step's lease scope, stream stdout/stderr to both the ephemeral log subject and the blobstore, publish `JobStatus`, ack only after status is published.
- [ ] Implement `cmd/dhole-engine/main.go` reading `DHOLE_BUS_URL`, `DHOLE_ENGINE_ID`, `DHOLE_TIER` and starting the agent.
- [ ] Run `go test ./internal/engine` — expect PASS. Commit.

## Task 13: Leases, fencing tokens and orphan detection

Files: `internal/lease/lease.go`, `internal/lease/lease_test.go`, `internal/lease/kv.go`
Interfaces: produces `lease.Manager` with `Claim(ctx, tenantID, runID, stepID string, attempt uint32, ttl time.Duration) (Token, error)`, `Renew(ctx, t Token) error`, `Validate(ctx, t Token) error`, `Expire(ctx) ([]Orphan, error)`; `Token{Value string; Fence uint64}`.

- [ ] Write `internal/lease/lease_test.go` asserting `TestFenceIncrementsPerAttempt`: claiming attempt 1 then attempt 2 for the same step yields `Fence` values that strictly increase. Run — expect FAIL with "undefined: lease.New".
- [ ] Add `TestAtMostOnceRejectsDuplicateDeliveryByFence`: claim a lease at fence 1, claim again producing fence 2, then `Validate` the fence-1 token and require an error satisfying `errors.Is(err, lease.ErrFenced)`.
- [ ] Add `TestHeartbeatExpiryRedeliversAndCatalogPersists`: claim a lease with a 100ms TTL, do not renew, and require `Expire` returns that step as an orphan after the TTL.
- [ ] Implement `internal/lease/kv.go` over the NATS KV bucket `dhole-leases` with per-key revision as the fence source, and `internal/lease/lease.go` wrapping it.
- [ ] Run `go test ./internal/lease` — expect PASS. Commit.

## Task 14: Scheduler and dispatcher

Files: `internal/scheduler/scheduler.go`, `internal/scheduler/match.go`, `internal/scheduler/scheduler_test.go`
Interfaces: produces `scheduler.Scheduler` with `Advance(ctx, tenantID, runID string) error`, `OnStatus(ctx, s *dholev1.JobStatus) error`; `scheduler.Match(req executor.Requirements, engines []registry.Instance) []registry.Instance`.

- [ ] Write `internal/scheduler/scheduler_test.go` asserting `TestAdvanceDispatchesOnlyReadySteps`: for the Task 3 diamond pipeline, the first `Advance` dispatches only `a`; after `a` succeeds, the next dispatches `b` and `c` but not `d`. Run — expect FAIL with "undefined: scheduler.New".
- [ ] Add `TestMatchFiltersByCapabilityOSAndArch` asserting a step requiring `PRIVILEGED` on `linux/arm64` matches only an instance advertising all three.
- [ ] Add `TestUnschedulableStepReportsWhy` asserting a step whose requirements match no instance produces an event whose payload contains "no engine advertises capability PRIVILEGED".
- [ ] Implement `internal/scheduler/match.go` (pure filtering, no I/O) and `internal/scheduler/scheduler.go` reading the run's event log, computing ready steps from `dag.Graph`, claiming a lease, and enqueuing a `JobDispatch` through the outbox.
- [ ] Run `go test ./internal/scheduler` — expect PASS. Commit.

## Task 15: Cache keys and skip-on-hit

Files: `internal/cache/key.go`, `internal/cache/cache.go`, `internal/cache/key_test.go`, `internal/cache/cache_test.go`
Interfaces: produces `cache.Key(step *dholev1.Step, envIdentity string, inputs []dholev1.Digest, lockfile map[string]string) (dholev1.Digest, error)`, `cache.Lookup(ctx, tenantID string, k dholev1.Digest) ([]dholev1.OutputRef, bool, error)`, `cache.Record(ctx, tenantID string, k dholev1.Digest, outs []dholev1.OutputRef) error`.

- [ ] Write `internal/cache/key_test.go` asserting `TestKeyIsStableAcrossOrderingAndUnstableOnInputChange`: reordering the `inputs` slice yields the same key; changing one input digest changes it; changing `envIdentity` changes it; changing a lockfile entry changes it. Run — expect FAIL with "undefined: cache.Key".
- [ ] Add `TestKeyRefusesNonPureStep` asserting `cache.Key` on a step whose `EffectClass` is `AT_MOST_ONCE` returns an error containing "only pure steps are cacheable".
- [ ] Add `TestKeyRefusesEmptyEnvironmentIdentity` asserting an empty `envIdentity` returns an error containing "no stable environment identity".
- [ ] Write `internal/cache/cache_test.go` asserting `TestCacheHitOnUnchangedInputsAndInvalidationOnChange` end to end against the SQLite store and filesystem CAS.
- [ ] Implement `internal/cache/key.go` hashing a canonical protobuf encoding of `(step command, image digest/envIdentity, sorted input digests, sorted lockfile pairs)` with SHA-256, and `internal/cache/cache.go` persisting `cache_entries(tenant_id, key, outputs, created_at)`.
- [ ] Run `go test ./internal/cache` — expect PASS. Commit.

## Task 16: Pool leases degrade cacheability visibly

Files: `internal/cache/eligibility.go`, `internal/cache/eligibility_test.go`
Interfaces: produces `cache.Eligible(step *dholev1.Step, lease executor.LeaseScope, envIdentity string) (bool, string)` returning the reason when false.

- [ ] Write `internal/cache/eligibility_test.go` asserting `TestPoolLeaseMarksStepNonCacheable`: a `pure` step under `LeasePool` returns `(false, "pool lease reuses state that cannot be hashed")`. Run — expect FAIL with "undefined: cache.Eligible".
- [ ] Add `TestPureStepUnderStepLeaseIsEligible` asserting `(true, "")` for `LeaseStep` with a non-empty `envIdentity`.
- [ ] Add `TestEffectfulStepIsNeverEligible` asserting both `IDEMPOTENT` and `AT_MOST_ONCE` return false with a reason naming the effect class.
- [ ] Implement `internal/cache/eligibility.go` and call it from the scheduler, recording the reason on the `STEP_DISPATCHED` event so the run view can display it.
- [ ] Run `go test ./internal/cache` — expect PASS. Commit.

## Task 17: CAS refcount garbage collection

Files: `internal/cas/gc.go`, `internal/cas/gc_test.go`, `internal/runstore/migrations/0003_blob_refs.sql`
Interfaces: produces `cas.GC{Store cas.Store; Runs runstore.Store}` with `Collect(ctx, tenantID string, retain time.Duration) (freed int, err error)`.

- [ ] Write `internal/cas/gc_test.go` asserting `TestRefcountGCPreservesRetainedRunBlobs`: two runs each producing a blob, one run aged beyond `retain`; `Collect` frees exactly the expired run's blob and `Has` still reports the retained one. Run — expect FAIL with "undefined: cas.GC".
- [ ] Add `TestGCNeverCollectsBlobSharedWithRetainedRun` asserting a blob referenced by both an expired and a retained run survives.
- [ ] Add `TestCacheEntryIsDroppedWhenItsOutputBlobIsCollected` asserting the `cache_entries` row is removed alongside the blob, so a later lookup misses rather than returning a dangling reference.
- [ ] Write `0003_blob_refs.sql` creating `blob_refs(tenant_id, digest, run_id)` with a composite primary key.
- [ ] Implement `internal/cas/gc.go` computing the retained run set, then deleting blobs with no remaining reference, in that order so a concurrent run cannot lose a blob.
- [ ] Run `go test ./internal/cas` — expect PASS. Commit.

## Task 18: Single binary and the first end-to-end run

Files: `cmd/dhole/main.go`, `internal/server/server.go`, `internal/server/singlebinary.go`, `internal/server/e2e_test.go`, `testdata/pipelines/two-step.yaml`
Interfaces: produces `server.New(cfg server.Config) (*server.Server, error)`, `Server.Start(ctx) error`, `Server.Stop(ctx) error`; `server.Config{Mode ModeEmbedded|ModeDistributed; StoreDSN, BusURL, BlobRoot string}`.

- [ ] Write `internal/server/e2e_test.go` asserting `TestSingleBinaryRunsTwoStepPipeline`: start a server in `ModeEmbedded`, submit `testdata/pipelines/two-step.yaml` (step `a` writes a file, step `b` reads it), and require the run reaches `RUN_COMPLETED` with `b`'s output containing `a`'s bytes. Run — expect FAIL with "undefined: server.New".
- [ ] Add `TestSingleBinaryAndDistributedParity` running the same pipeline in `ModeDistributed` against the compose-provided Postgres, NATS and MinIO, requiring identical run output.
- [ ] Add `TestEmbeddedEngineUsesLoopbackBusNotDirectCall` asserting the run's dispatch appears on the bus subject, proving there is no in-process shortcut.
- [ ] Implement `internal/server/singlebinary.go` wiring embedded NATS, SQLite, filesystem CAS and blobstore, and an in-process `engine.Agent` connected over loopback.
- [ ] Implement `cmd/dhole/main.go` with `dhole serve`, reading flags `--mode`, `--store-dsn`, `--bus-url`, `--blob-root`.
- [ ] Run `go test ./internal/server && make test-integration` — expect PASS. Commit.

## Task 19: Effect classes, retry and idempotency keys

Files: `internal/effects/effects.go`, `internal/effects/retry.go`, `internal/effects/effects_test.go`
Interfaces: produces `effects.RetryPolicy(step *dholev1.Step) effects.Policy`; `Policy{MaxAttempts int; Backoff time.Duration; RequiresIdempotencyKey bool; RequiresExclusiveLease bool}`; `effects.IdempotencyKey(runID, stepID string, attempt uint32) string`.

- [ ] Write `internal/effects/effects_test.go` asserting `TestPolicyPerEffectClass`: `PURE` yields `MaxAttempts: 3, RequiresExclusiveLease: false`; `IDEMPOTENT` yields `RequiresIdempotencyKey: true`; `AT_MOST_ONCE` yields `MaxAttempts: 1, RequiresExclusiveLease: true`. Run — expect FAIL with "undefined: effects.RetryPolicy".
- [ ] Add `TestIdempotentRetryReusesIdempotencyKey` asserting `IdempotencyKey` is identical across attempts 1 and 2 of the same step and differs between steps.
- [ ] Add `TestAtMostOnceStepIsNeverAutoRetried` driving the scheduler through a step failure and requiring no second dispatch is enqueued, and that an event of type `STEP_AWAITING_REPLAY` is recorded instead.
- [ ] Implement `internal/effects/effects.go` and `retry.go`, and change the scheduler from Task 14 to consult `RetryPolicy` before re-dispatch and to require a valid lease fence for `RequiresExclusiveLease`.
- [ ] Run `go test ./internal/effects ./internal/scheduler` — expect PASS. Commit.

## Task 20: Durable waits, timers and human approval

Files: `internal/wait/wait.go`, `internal/wait/timer.go`, `internal/wait/wait_test.go`, `internal/steps/approval/approval.go`, `internal/steps/approval/approval_test.go`
Interfaces: produces `wait.Timers` with `Schedule(ctx, tenantID, runID, stepID string, at time.Time) error`, `Due(ctx, now time.Time) ([]wait.Due, error)`; `approval.Step` with `Request(ctx, runID, stepID string, prompt string) error`, `Decide(ctx, runID, stepID string, approver string, approved bool) error`.

- [ ] Write `internal/wait/wait_test.go` asserting `TestDurableWaitSurvivesRestart`: schedule a timer 200ms out, stop and restart the server, and require the run resumes and completes. Run — expect FAIL with "undefined: wait.Timers".
- [ ] Add `TestMissedScheduleWindowFiresOnceOnRecovery` asserting a timer whose due time passed entirely while the server was down fires exactly once, not once per missed interval.
- [ ] Write `internal/steps/approval/approval_test.go` asserting `TestApprovalGateBlocksUntilDecided`: a run containing an approval step does not proceed until `Decide(approved: true)`, and the approver identity is recorded in the event log.
- [ ] Add `TestApprovalDenialFailsRunWithReason` asserting `Decide(approved: false)` produces `RUN_COMPLETED` with a failure whose payload names the approver.
- [ ] Implement `internal/wait/timer.go` persisting timers in the run store (so they survive restart) with a 1s poll for due entries, and `internal/steps/approval/approval.go` as a step type that emits `STEP_AWAITING_APPROVAL` and resumes on decision.
- [ ] Run `go test ./internal/wait ./internal/steps/approval` — expect PASS. Commit.

## Task 21: CEL policy engine, tiers and audit

Files: `internal/policy/policy.go`, `internal/policy/cel.go`, `internal/policy/audit.go`, `internal/policy/policy_test.go`
Interfaces: produces `policy.Engine` with `Evaluate(ctx, in policy.Input) (policy.Decision, error)`; `Input{Tier, TenantID, Subject string; Capabilities []dholev1.Capability; EffectClass dholev1.EffectClass; PluginRef string; Signed bool; Upstream string}`; `Decision{Allow bool; Rule, Reason string}`.

- [ ] Write `internal/policy/policy_test.go` asserting `TestForbiddenCapabilityRefusedAtSave`: a tier whose rule is `!("PRIVILEGED" in input.capabilities)` denies a step requesting `PRIVILEGED`, with `Decision.Rule` naming the rule id. Run — expect FAIL with "undefined: policy.New".
- [ ] Add `TestPolicyDenialAuditedWithinLatencyBudget` asserting a denial writes an audit row containing rule, tier and subject, and that 1000 sequential evaluations average under 10ms each with the compiled-program cache warm.
- [ ] Add `TestPolicyEvaluationErrorFailsClosed` asserting a rule referencing an undefined field yields `Allow: false` and a `Reason` containing "policy error".
- [ ] Add `TestUnsignedPluginDeniedInProductionTierAllowedInDev` asserting the same input differs by tier only.
- [ ] Implement `internal/policy/cel.go` using `github.com/google/cel-go` with an env declaring every `Input` field, compiling and caching programs keyed by `(tier, policy revision)`, and `internal/policy/audit.go` writing decisions to `policy_audit`.
- [ ] Wire the engine into the scheduler before dispatch and into definition save.
- [ ] Run `go test ./internal/policy` — expect PASS. Commit.

## Task 22: Tenancy enforcement across store and bus

Files: `internal/tenant/tenant.go`, `internal/tenant/nats_accounts.go`, `internal/tenant/tenant_test.go`
Interfaces: produces `tenant.FromContext(ctx) (string, error)`, `tenant.WithTenant(ctx, id string) context.Context`, `tenant.AccountName(tenantID string) string`, `tenant.ProvisionAccount(ctx, srv *bus.Embedded, tenantID string) (creds string, err error)`.

- [ ] Write `internal/tenant/tenant_test.go` asserting `TestTenantIsolationAcrossStoreAndBus`: tenant `b` cannot replay tenant `a`'s run, cannot read `a`'s CAS blob, and cannot subscribe to `a`'s dispatch subject. Run — expect FAIL with "undefined: tenant.ProvisionAccount".
- [ ] Add `TestNoStoreMethodAcceptsEmptyTenant` iterating every exported `runstore.Store`, `cas.Store` and `blobstore.Store` method by reflection and requiring each returns an error containing "tenant scope required" when passed `""`.
- [ ] Add `TestSubjectBuildersIncludeTenant` asserting every builder in `internal/bus/subjects.go` produces a subject containing the tenant segment.
- [ ] Implement `internal/tenant/nats_accounts.go` provisioning one NATS account per tenant with subject permissions limited to that tenant's prefixes, and `internal/tenant/tenant.go` for context propagation.
- [ ] Run `go test ./internal/tenant ./internal/bus ./internal/runstore` — expect PASS. Commit.

## Task 23: Built-in identity and service tokens

Files: `internal/identity/identity.go`, `internal/identity/local.go`, `internal/identity/token.go`, `internal/identity/local_test.go`, `internal/runstore/migrations/0004_identity.sql`
Interfaces: produces `identity.Provider` with `Authenticate(ctx, credential string) (identity.Principal, error)`; `Principal{Subject, TenantID string; Scopes []string; Kind PrincipalUser|PrincipalService}`; `identity.NewLocal(store)`, `identity.IssueToken(ctx, p Principal, ttl time.Duration) (string, error)`.

- [ ] Write `internal/identity/local_test.go` asserting `TestServiceTokenAuthenticatesWithScopes`: issue a token with scope `pipelines:write`, authenticate it, and require the returned `Principal` carries that scope and tenant. Run — expect FAIL with "undefined: identity.NewLocal".
- [ ] Add `TestExpiredTokenIsRejected` asserting a token issued with a -1s TTL returns an error satisfying `errors.Is(err, identity.ErrExpired)`.
- [ ] Add `TestPasswordsAreStoredAsArgon2idNotPlaintext` asserting the stored credential does not contain the password and begins with `$argon2id$`.
- [ ] Write `0004_identity.sql` creating `principals(tenant_id, subject, kind, credential_hash)` and `tokens(tenant_id, subject, token_hash, scopes, expires_at)`; store only token hashes.
- [ ] Implement `internal/identity/local.go` with `golang.org/x/crypto/argon2` and `internal/identity/token.go` issuing 32-byte random tokens.
- [ ] Run `go test ./internal/identity` — expect PASS. Commit.

## Task 24: OIDC federation

Files: `internal/identity/oidc.go`, `internal/identity/oidc_test.go`, `internal/identity/chain.go`
Interfaces: produces `identity.NewOIDC(cfg OIDCConfig) (identity.Provider, error)`, `identity.Chain(providers ...identity.Provider) identity.Provider`.

- [ ] Write `internal/identity/oidc_test.go` asserting `TestOIDCAndServiceTokenWithIdPDown`: a chain of OIDC and local providers authenticates an OIDC id token while the mock IdP is up, and still authenticates a local service token after the IdP is stopped. Run — expect FAIL with "undefined: identity.NewOIDC".
- [ ] Add `TestOIDCTokenWithWrongAudienceIsRejected` asserting an id token whose `aud` does not match configuration returns an error containing "audience".
- [ ] Add `TestOIDCFailureDoesNotFallBackToWeakerAuth` asserting that when the IdP is unreachable, an OIDC credential returns an error rather than being accepted by the local provider.
- [ ] Implement `internal/identity/oidc.go` with `github.com/coreos/go-oidc/v3` verifying issuer, audience, expiry and signature against cached JWKS, and `internal/identity/chain.go` selecting a provider by credential shape.
- [ ] Run `go test ./internal/identity` — expect PASS. Commit.

## Task 25: Definition store, revisions and approval state

Files: `internal/defstore/defstore.go`, `internal/defstore/revision.go`, `internal/defstore/defstore_test.go`, `internal/runstore/migrations/0005_definitions.sql`
Interfaces: produces `defstore.Store` with `Save(ctx, tenantID string, p *dholev1.Pipeline, author string) (Revision, error)`, `Get(ctx, tenantID, pipelineID, revisionID string) (*dholev1.Pipeline, error)`, `Active(ctx, tenantID, pipelineID string) (Revision, error)`, `Approve(ctx, tenantID, revisionID, approver string) error`; `Revision{ID, PipelineID string; ContentHash string; State draft|reviewed|active; Lockfile map[string]string}`.

- [ ] Write `internal/defstore/defstore_test.go` asserting `TestRevisionCreatedAndMirroredOnEdit`: saving a pipeline returns a revision whose `ContentHash` is stable across identical saves and changes when any field changes. Run — expect FAIL with "undefined: defstore.New".
- [ ] Add `TestNewRevisionStartsAsDraftAndOnlyBecomesActiveOnApproval` asserting `Active` returns the previous revision until `Approve` is called, and that the approver is persisted.
- [ ] Add `TestRunPinsRevisionAndIsUnaffectedBySubsequentSaves` asserting a run started against revision 1 still executes revision 1's definition after revision 2 is approved.
- [ ] Add `TestApprovalRevokedMidRunDoesNotAlterRunningRun` asserting a run in flight completes on its pinned revision.
- [ ] Write `0005_definitions.sql` creating `pipelines`, `revisions(tenant_id, id, pipeline_id, content_hash, state, lockfile, author, approver, created_at)`.
- [ ] Implement `internal/defstore/revision.go` computing the content hash over canonical protobuf bytes and `defstore.go` over the run store's connection.
- [ ] Run `go test ./internal/defstore` — expect PASS. Commit.

## Task 26: One-way git mirror

Files: `internal/mirror/git.go`, `internal/mirror/git_test.go`, `internal/mirror/yaml.go`
Interfaces: produces `mirror.Git` with `Push(ctx, tenantID string, rev defstore.Revision, p *dholev1.Pipeline) error`, `Status(ctx, tenantID string) (mirror.State, error)`; `mirror.ToYAML(p *dholev1.Pipeline) ([]byte, error)`, `mirror.FromYAML([]byte) (*dholev1.Pipeline, error)`.

- [ ] Write `internal/mirror/git_test.go` asserting `TestGitMirrorEditsAreNotAuthoritative`: push a revision to a bare repo, commit an unrelated change directly in a clone, then push a new revision and require the authoritative YAML overwrites the manual edit and `defstore.Active` is unchanged by it. Run — expect FAIL with "undefined: mirror.NewGit".
- [ ] Add `TestYAMLRoundTripsLosslessly` asserting `FromYAML(ToYAML(p))` equals `p` by `proto.Equal` for a pipeline exercising every field, so backend export and import stay lossless.
- [ ] Add `TestYAMLContainsNoBackendSpecificFields` asserting the emitted YAML has no `layout`, `approver` or `state` key.
- [ ] Add `TestMirrorPushFailureDoesNotBlockSave` asserting that with an unreachable remote, `defstore.Save` still succeeds and `mirror.Status` reports drift.
- [ ] Implement `internal/mirror/yaml.go` with `protojson` to YAML via `sigs.k8s.io/yaml`, and `internal/mirror/git.go` with `go-git`, committing one file per pipeline under `pipelines/<id>.yaml` and retrying with backoff.
- [ ] Run `go test ./internal/mirror` — expect PASS. Commit.

## Task 27: ConnectRPC API and operation-level editing

Files: `proto/dhole/v1/api.proto`, `internal/api/server.go`, `internal/api/operations.go`, `internal/api/operations_test.go`, `internal/api/auth.go`
Interfaces: produces service `PipelineService` with RPCs `GetPipeline`, `ApplyOperation`, `Validate`, `Plan`, `ListRevisions`, `ApproveRevision`, `StartRun`, `WatchRun`; `api.Operation` oneof `AddStep`, `Connect`, `SetProperty`, `RemoveEdge`, `Rename`; `ApplyOperationResponse{Revision revision; Diff diff; Operation inverse}`.

- [ ] Write `internal/api/operations_test.go` asserting `TestOperationInverseRestoresRevision`: apply `AddStep`, capture the returned `inverse`, apply it, and require the resulting pipeline equals the original by `proto.Equal`. Run — expect FAIL with "undefined: api.NewServer".
- [ ] Add `TestApplyOperationRejectsStaleVersion` asserting applying against a superseded `base_revision` returns `CodeAborted` with a message containing "revision conflict".
- [ ] Add `TestEveryOperationReturnsANonEmptyDiff` iterating all five operation kinds and requiring each response carries a diff naming the changed step or edge.
- [ ] Write `proto/dhole/v1/api.proto` with the service and messages; run `buf generate`.
- [ ] Implement `internal/api/operations.go` applying each operation to a copy of the pipeline and computing the inverse, and `internal/api/server.go` serving it over ConnectRPC with `internal/api/auth.go` resolving a `Principal` from the `Authorization` header and rejecting unscoped calls.
- [ ] Run `go test ./internal/api` — expect PASS. Commit.

## Task 28: Validate and plan endpoints

Files: `internal/api/validate.go`, `internal/api/plan.go`, `internal/api/plan_test.go`
Interfaces: produces `Validate(ctx, *ValidateRequest) (*ValidateResponse{repeated Diagnostic diagnostics})`, `Plan(ctx, *PlanRequest) (*PlanResponse{repeated PlannedStep steps})`; `PlannedStep{string step_id; bool cache_hit; string engine_kind; string non_cacheable_reason}`.

- [ ] Write `internal/api/plan_test.go` asserting `TestPlanReportsCacheHitsAndEngineAssignment`: plan a pipeline whose first step is already cached and require `steps[0].cache_hit == true` and every step carries a non-empty `engine_kind`. Run — expect FAIL with "undefined: api.Plan".
- [ ] Add `TestPlanReportsNonCacheableReasonForPoolLease` asserting a `LeasePool` step returns the exact reason string from Task 16.
- [ ] Add `TestValidateSurfacesTypeErrorsWithPositions` asserting the Task 3 type error is returned with the step id and port name populated.
- [ ] Add `TestPlanDoesNotDispatchAnything` asserting no message is published to any `job.dispatch.*` subject during a `Plan` call.
- [ ] Implement `internal/api/validate.go` delegating to `dag.TypeCheck` plus plugin schema validation, and `internal/api/plan.go` resolving the DAG, computing cache keys, consulting `cache.Lookup` and `scheduler.Match` without side effects.
- [ ] Run `go test ./internal/api` — expect PASS. Commit.

## Task 29: CLI at full API parity

Files: `cmd/dhole/cli/`, `cmd/dhole/cli/gen.go`, `cmd/dhole/cli/coverage_test.go`, `cmd/dhole/cli/run.go`, `cmd/dhole/cli/pipeline.go`
Interfaces: produces commands `dhole pipeline get|apply|validate|plan|revisions|approve`, `dhole run start|watch|logs|cancel`, `dhole engine list|drain`, `dhole policy test`, `dhole local run`.

- [ ] Write `cmd/dhole/cli/coverage_test.go` asserting `TestCLICoversEveryRPC`: reflect over the `PipelineService` and `RunService` protobuf descriptors and require every RPC name maps to a registered cobra command, failing with the list of uncovered RPCs. Run — expect FAIL with "undefined: cli.Root".
- [ ] Add `TestLocalRunExecutesWithoutServer` asserting `dhole local run testdata/pipelines/two-step.yaml` completes using an in-process embedded server and prints step outputs.
- [ ] Add `TestCLIOutputIsJSONWhenRequested` asserting `--output json` emits parseable JSON for `plan`.
- [ ] Implement `cmd/dhole/cli/gen.go` generating a command stub per RPC from the descriptors at build time, and the hand-written commands for the friendlier surfaces.
- [ ] Run `go test ./cmd/dhole/...` — expect PASS. Commit.

## Task 30: Catalog of step, plugin, engine and trigger types

Files: `internal/catalog/catalog.go`, `internal/catalog/manifest.go`, `internal/catalog/catalog_test.go`, `internal/runstore/migrations/0006_catalog.sql`
Interfaces: produces `catalog.Store` with `Publish(ctx, tenantID string, m catalog.Manifest) error`, `Resolve(ctx, tenantID, ref string) (catalog.Entry, error)`, `List(ctx, tenantID string) ([]catalog.Entry, error)`; `Manifest{Namespace, Name, Version string; Digest dholev1.Digest; Kind step|trigger|engine; EffectClass dholev1.EffectClass; Capabilities []dholev1.Capability; InputSchema, OutputSchema []byte; EngineTypes []string}`.

- [ ] Write `internal/catalog/catalog_test.go` asserting `TestManifestDeclaresEffectClassAndCapabilities`: publishing a manifest and resolving it returns the declared effect class and capability set unchanged. Run — expect FAIL with "undefined: catalog.New".
- [ ] Add `TestStepInheritsEffectClassFromManifest` asserting a step with no explicit effect class resolves to its plugin's, and that an explicit widening override is flagged in the returned `Entry.OverrideWarnings`.
- [ ] Add `TestCatalogSurvivesControlPlaneRestart` asserting entries persist across a store reopen, unlike the runtime registry.
- [ ] Add `TestManifestWithInvalidJSONSchemaIsRejected` asserting `Publish` returns an error naming the offending schema.
- [ ] Write `0006_catalog.sql` creating `catalog_entries(tenant_id, namespace, name, version, digest, kind, effect_class, capabilities, input_schema, output_schema, engine_types)`.
- [ ] Implement `internal/catalog/manifest.go` validating schemas with `santhosh-tekuri/jsonschema/v6` and `catalog.go`.
- [ ] Run `go test ./internal/catalog` — expect PASS. Commit.

## Task 31: Runtime engine registry and lifecycle

Files: `internal/registry/registry.go`, `internal/registry/kv.go`, `internal/registry/registry_test.go`
Interfaces: produces `registry.Registry` with `Register(ctx, r *dholev1.EngineRegistration) error`, `Heartbeat(ctx, h *dholev1.EngineHeartbeat) error`, `Instances(ctx, tenantID string) ([]registry.Instance, error)`, `Drain(ctx, engineID string) error`; `Instance{ID string; State registering|ready|draining|gone; Capabilities []dholev1.Capability; OS, Arch string; Slots int; ProtocolVersions []uint32}`.

- [ ] Write `internal/registry/registry_test.go` asserting `TestInstanceAgesOutWithoutHeartbeat`: register with a 200ms TTL, stop heartbeating, and require `Instances` omits it after the TTL. Run — expect FAIL with "undefined: registry.New".
- [ ] Add `TestDrainStopsNewWorkButFinishesInFlight` asserting a drained instance receives no new dispatch while its in-flight step still completes.
- [ ] Add `TestRegistrationWithUnsupportedProtocolIsRefused` asserting a registration advertising only version 0 is rejected with an error containing "unsupported protocol".
- [ ] Add `TestSchedulerRoutesOnlyToInstancesSatisfyingResolvedRequirements` asserting a fleet on mixed protocol versions receives work only where the version and capabilities match.
- [ ] Implement `internal/registry/kv.go` over NATS KV bucket `dhole-engines` with TTL, and `registry.go` mapping lifecycle transitions.
- [ ] Run `go test ./internal/registry ./internal/scheduler` — expect PASS. Commit.

## Task 32: Plugin resolver for oci:// and cas://

Files: `internal/plugins/resolver.go`, `internal/plugins/oci.go`, `internal/plugins/casref.go`, `internal/plugins/resolver_test.go`
Interfaces: produces `plugins.Resolver` with `Resolve(ctx, tenantID, ref string) (plugins.Artifact, error)`, `Fetch(ctx, tenantID string, a plugins.Artifact) (io.ReadCloser, error)`; `Artifact{Ref string; Digest dholev1.Digest; Scheme oci|cas; MediaType string}`.

- [ ] Write `internal/plugins/resolver_test.go` asserting `TestResolverHandlesBothSchemesUniformly`: an `oci://` reference against a local registry and a `cas://` reference against the filesystem CAS both resolve to an `Artifact` with a populated digest and both `Fetch` successfully. Run — expect FAIL with "undefined: plugins.NewResolver".
- [ ] Add `TestTagIsResolvedToDigestAtSaveAndNeverAtDispatch` asserting `Resolve` on `oci://reg/img:v1` returns a digest, and that dispatch with a tag-only reference returns an error containing "unresolved tag".
- [ ] Add `TestMovedTagDoesNotChangeExistingRevision` asserting that after retagging the registry image, an existing lockfile still fetches the original digest.
- [ ] Implement `internal/plugins/oci.go` with `google/go-containerregistry` (digest-pinned pulls, no tag resolution at fetch), `casref.go` delegating to `cas.Store`, and `resolver.go` dispatching on scheme.
- [ ] Add `zot` to `docker-compose.test.yml` on port 55000 as the test registry.
- [ ] Run `make test-integration` — expect PASS. Commit.

## Task 33: Detached signature records and verification

Files: `internal/plugins/signature.go`, `internal/plugins/cosign.go`, `internal/plugins/signature_test.go`, `internal/runstore/migrations/0007_signatures.sql`
Interfaces: produces `plugins.Signatures` with `Record(ctx, tenantID string, d dholev1.Digest, sig plugins.Signature) error`, `Verify(ctx, tenantID string, d dholev1.Digest, allowed []string) error`; `Signature{Identity, Issuer string; Payload []byte; Source cosign|manual}`.

- [ ] Write `internal/plugins/signature_test.go` asserting `TestVerificationIsUniformAcrossSchemes`: the same signature record verifies for an `oci://` and a `cas://` artifact with identical code paths. Run — expect FAIL with "undefined: plugins.NewSignatures".
- [ ] Add `TestUnsignedPluginIsNotDispatchedRegardlessOfCachedResolution` asserting that after a successful resolve, removing the signature record causes dispatch to fail with "signature verification failed" and marks the catalog entry untrusted.
- [ ] Add `TestSignatureFromDisallowedIdentityIsRejected` asserting verification against an `allowed` list not containing the signer's identity fails.
- [ ] Write `0007_signatures.sql` creating `artifact_signatures(tenant_id, digest, identity, issuer, payload, source)`.
- [ ] Implement `internal/plugins/cosign.go` populating records from cosign attestations and `signature.go` verifying against the tier's allowed identities from policy.
- [ ] Run `go test ./internal/plugins` — expect PASS. Commit.

## Task 34: Federated upstreams and local mirroring

Files: `internal/plugins/upstream.go`, `internal/plugins/mirror.go`, `internal/plugins/upstream_test.go`
Interfaces: produces `plugins.Upstreams` with `Add(ctx, tenantID string, u Upstream) error`, `Sync(ctx, tenantID, namespace string) (int, error)`; `Upstream{Namespace, URL string; AllowedIdentities []string; MirrorPolicy always|on_demand}`.

- [ ] Write `internal/plugins/upstream_test.go` asserting `TestNamespacingPreventsCollisionBetweenUpstreams`: two upstreams both publishing `docker-build` resolve independently as `a/docker-build` and `b/docker-build`. Run — expect FAIL with "undefined: plugins.NewUpstreams".
- [ ] Add `TestSyncMirrorsArtifactsLocallyAndSurvivesUpstreamOutage` asserting that after `Sync`, stopping the upstream registry still allows resolve and fetch from the local mirror.
- [ ] Add `TestUnmirroredPluginBlocksDispatchWithClearDiagnostic` asserting dispatch of an unmirrored plugin from an unreachable upstream fails with an error naming the plugin and the upstream.
- [ ] Add `TestPerUpstreamAllowedIdentitiesAreEnforced` asserting a plugin signed by an identity allowed on upstream `a` is rejected when pulled from upstream `b`.
- [ ] Implement `internal/plugins/upstream.go` and `mirror.go` copying artifacts into the local CAS or registry mirror and recording signatures at sync time.
- [ ] Run `make test-integration` — expect PASS. Commit.

## Task 35: Lockfile resolution at definition save

Files: `internal/defstore/lockfile.go`, `internal/defstore/lockfile_test.go`
Interfaces: produces `defstore.ResolveLockfile(ctx, tenantID string, p *dholev1.Pipeline, r plugins.Resolver) (map[string]string, error)` returning plugin ref to digest.

- [ ] Write `internal/defstore/lockfile_test.go` asserting `TestLockfilePinsPluginDigestsAgainstMovedTag`: save a pipeline referencing `oci://reg/img:v1`, retag the registry to different content, and require a run of the saved revision fetches the original digest. Run — expect FAIL with "undefined: defstore.ResolveLockfile".
- [ ] Add `TestSaveFailsWhenAPluginCannotBeResolved` asserting `Save` returns an error naming the unresolvable ref rather than persisting a partial lockfile.
- [ ] Add `TestLockfileChangeProducesVisibleDiff` asserting a re-save after an intentional plugin upgrade yields a diff listing the old and new digests.
- [ ] Add `TestCacheKeyChangesWhenLockfileChanges` asserting the Task 15 key differs between two revisions differing only in lockfile.
- [ ] Implement `internal/defstore/lockfile.go` resolving every plugin ref during `Save` and storing the map on the revision.
- [ ] Run `go test ./internal/defstore ./internal/cache` — expect PASS. Commit.

## Task 36: containerd/OCI executor

Files: `internal/executor/containerd/containerd.go`, `internal/executor/containerd/identity.go`, `internal/executor/containerd/containerd_test.go`
Interfaces: produces `containerd.New(cfg containerd.Config) (executor.Executor, error)` satisfying Task 9's interface; `Executor.EnvironmentIdentity() (string, error)` returning the image digest.

- [ ] Write `internal/executor/containerd/containerd_test.go` calling Task 9's `executorContract` as `TestContainerdExecutorContract` against a containerd socket from `DHOLE_TEST_CONTAINERD_SOCK`, skipping when unset. Run — expect FAIL with "undefined: containerd.New".
- [ ] Add `TestEnvironmentIdentityIsImageDigestNotTag` asserting `EnvironmentIdentity` returns the resolved digest and changes when the image content changes under the same tag.
- [ ] Add `TestPrivilegedIsRefusedUnlessCapabilityAdvertised` asserting a spec requesting `PRIVILEGED` against an executor not advertising it returns an error containing "capability not advertised".
- [ ] Add `TestRootlessByDefault` asserting the container's uid inside the sandbox is non-zero unless `PRIVILEGED` is granted.
- [ ] Implement `internal/executor/containerd/containerd.go` using `containerd/containerd/v2` client — pull by digest, create a rootless container, stream stdout/stderr, propagate `SIGTERM` then `SIGKILL` after a grace period — and `identity.go` resolving image digests.
- [ ] Add containerd to CI as a service and run `make test-integration` — expect PASS. Commit.

## Task 37: Kubernetes executor

Files: `internal/executor/kubernetes/kubernetes.go`, `internal/executor/kubernetes/kubernetes_test.go`
Interfaces: produces `kubernetes.New(cfg kubernetes.Config) (executor.Executor, error)`; `Config{Kubeconfig, Namespace string; ServiceAccount string; PodTemplate *corev1.PodSpec}`.

- [ ] Write `internal/executor/kubernetes/kubernetes_test.go` calling `executorContract` as `TestKubernetesExecutorContract` against a kind cluster from `DHOLE_TEST_KUBECONFIG`, skipping when unset. Run — expect FAIL with "undefined: kubernetes.New".
- [ ] Add `TestPodPerStepLeaseIsDeletedOnRelease` asserting no pod remains after `Release` for a `LeaseStep` sandbox.
- [ ] Add `TestPipelineLeaseReusesOnePodAcrossSteps` asserting two `Exec` calls under one `LeasePipeline` sandbox run in the same pod.
- [ ] Add `TestPodEvictionSurfacesAsStepFailureNotSuccess` asserting a deleted pod mid-exec yields a non-zero exit and an error mentioning eviction.
- [ ] Implement `internal/executor/kubernetes/kubernetes.go` with `client-go`, creating pods from the template, attaching via the exec subresource, and cleaning up on release with a finalizer-free delete.
- [ ] Add a kind cluster to CI and run `make test-integration` — expect PASS. Commit.

## Task 38: Lease scopes, warm pools and lazy image pull

Files: `internal/executor/pool/pool.go`, `internal/executor/pool/pool_test.go`, `internal/executor/containerd/lazypull.go`
Interfaces: produces `pool.Manager` with `Acquire(ctx, key string, mk func() (executor.Sandbox, error)) (executor.Sandbox, error)`, `Reap(ctx, idle time.Duration) (int, error)`.

- [ ] Write `internal/executor/pool/pool_test.go` asserting `TestPoolReusesSandboxAcrossRuns`: two runs with the same pool key receive the same sandbox id, and a file written by the first is visible to the second. Run — expect FAIL with "undefined: pool.New".
- [ ] Add `TestReapReleasesIdleSandboxes` asserting a sandbox idle beyond the threshold is released and the next acquire creates a new one.
- [ ] Add `TestPooledSandboxIsReportedNonCacheable` asserting Task 16's `cache.Eligible` is consulted and returns false for every step run from the pool.
- [ ] Add `TestLazyPullFetchesFewerBytesThanFullImage` asserting that with stargz enabled, bytes read from the registry for a 500MB image running `true` are under 50MB, using the test registry's request log.
- [ ] Implement `internal/executor/pool/pool.go` keyed on `(tenant, engine kind, spec hash)` with an idle reaper, and `lazypull.go` enabling stargz snapshotter when available and falling back to a full pull with a logged warning.
- [ ] Run `make test-integration` — expect PASS. Commit.

## Task 39: Engine conformance suite

Files: `conformance/suite.go`, `conformance/cases.go`, `conformance/main.go`, `testdata/engines/minimal-python/engine.py`, `Makefile`
Interfaces: produces `conformance.Run(ctx, cfg conformance.Config) (conformance.Report, error)`; `make conformance ENGINE=<cmd>` runs the suite against any engine binary.

- [ ] Write `conformance/cases.go` with one case per contract obligation: registration and version negotiation, dispatch and success, non-zero exit, cancellation within 2s, timeout enforcement, 10MB log throughput, binary artifact round-trip, secret reference redemption without the value appearing in logs, lease renewal during a long step, and refusal of a fenced-out attempt.
- [ ] Write `testdata/engines/minimal-python/engine.py`: a NATS client that registers, pulls dispatches, runs the command with `subprocess`, streams logs, and publishes status — deliberately not Go.
- [ ] Write `conformance/suite_test.go` asserting `TestConformanceMinimalPythonEngine` runs every case against the Python engine and requires `Report.Failed == 0`. Run — expect FAIL with "undefined: conformance.Run".
- [ ] Add `TestConformanceDetectsAnEngineThatIgnoresCancellation` asserting a deliberately broken engine variant fails exactly the cancellation case, proving the suite can fail.
- [ ] Implement `conformance/suite.go` and `main.go`, and add `make conformance` to the Makefile.
- [ ] Run `make conformance ENGINE="python3 testdata/engines/minimal-python/engine.py"` — expect PASS. Commit.

## Task 40: Trigger interface and schedule trigger

Files: `internal/trigger/trigger.go`, `internal/trigger/schedule/schedule.go`, `internal/trigger/schedule/schedule_test.go`
Interfaces: produces `trigger.Trigger` with `Start(ctx, sink trigger.Sink) error`, `Kind() string`; `trigger.Sink` with `Fire(ctx, tenantID, pipelineID string, inputs map[string]*structpb.Value) error`; `trigger.Binding{PipelineID string; InputMapping map[string]string}`.

- [ ] Write `internal/trigger/schedule/schedule_test.go` asserting `TestScheduleFiresAtCronBoundary`: a `* * * * * *` schedule fires at least twice within 3s with the bound inputs populated. Run — expect FAIL with "undefined: schedule.New".
- [ ] Add `TestMissedScheduleWindowFiresOnceOnRecovery` asserting a schedule whose window elapsed entirely during downtime fires exactly once on restart, reusing Task 20's timer store.
- [ ] Add `TestConcurrencyBudgetOfOneSkipsOverlappingFire` asserting a second fire while the previous run is active is recorded as skipped with a reason, not queued indefinitely.
- [ ] Implement `internal/trigger/trigger.go` and `internal/trigger/schedule/schedule.go` using `robfig/cron/v3` with persistence through the durable timer store.
- [ ] Run `go test ./internal/trigger/...` — expect PASS. Commit.

## Task 41: HTTP, git webhook and pipeline-completion triggers

Files: `internal/trigger/http/http.go`, `internal/trigger/git/git.go`, `internal/trigger/completion/completion.go`, `internal/trigger/http/http_test.go`, `internal/trigger/git/git_test.go`, `internal/trigger/completion/completion_test.go`
Interfaces: produces `http.New(cfg)`, `git.New(cfg)` (GitHub, Gitea and Forgejo payloads), `completion.New(cfg)` satisfying `trigger.Trigger`.

- [ ] Write `internal/trigger/http/http_test.go` asserting `TestHTTPTriggerMapsBodyToTypedInputs`: a POST whose JSON body has `{"ref":"main"}` fires with input `ref` set, and a body failing the pipeline's input schema returns HTTP 400 with the validation error. Run — expect FAIL with "undefined: http.New".
- [ ] Write `internal/trigger/git/git_test.go` asserting `TestGitWebhookVerifiesSignatureAndRejectsForgery`: a GitHub push payload with a valid HMAC fires; one with a wrong signature returns 401 and does not fire.
- [ ] Add `TestGitWebhookMarksPayloadTainted` asserting the fired run's inputs carry the taint marker from Task 51.
- [ ] Write `internal/trigger/completion/completion_test.go` asserting `TestCompletionTriggerFiresDownstreamPipeline` and that a failed upstream run does not fire it.
- [ ] Write `internal/trigger/trigger_test.go` asserting `TestAllFourTriggersStartSamePipeline` — one unchanged pipeline definition fired by schedule, HTTP, git webhook and completion.
- [ ] Implement the three triggers, sharing input validation against the pipeline's declared input schema.
- [ ] Run `go test ./internal/trigger/...` — expect PASS. Commit.

## Task 42: Weighted fair queuing and concurrency budgets

Files: `internal/scheduler/fairness.go`, `internal/scheduler/budget.go`, `internal/scheduler/fairness_test.go`
Interfaces: produces `scheduler.Queue` with `Enqueue(ctx, item QueueItem) error`, `Next(ctx, slots int) ([]QueueItem, error)`; `scheduler.Budgets` with `Acquire(ctx, tenantID, pipelineID string) (release func(), ok bool)`.

- [ ] Write `internal/scheduler/fairness_test.go` asserting `TestWeightedFairQueuingUnderSaturation`: with tenant `a` enqueuing 10000 steps and tenant `b` enqueuing 10, `b`'s steps are all dispatched within the one-second target and `a` does not occupy more than its weighted share. Run — expect FAIL with "undefined: scheduler.NewQueue".
- [ ] Add `TestConcurrencyBudgetCapsPipelineInFlight` asserting a pipeline with a budget of 2 never has 3 steps dispatched simultaneously.
- [ ] Add `TestBudgetReleaseOnStepFailureNotOnlyOnSuccess` asserting a failed step releases its budget slot.
- [ ] Add `TestQueueIsDeterministicUnderEqualWeights` asserting equal-weight tenants interleave one-for-one.
- [ ] Implement `internal/scheduler/fairness.go` as a deficit round-robin over per-tenant queues and `budget.go` as a counting semaphore persisted in NATS KV so budgets survive a control-plane restart.
- [ ] Run `go test ./internal/scheduler` — expect PASS. Commit.

## Task 43: Control-plane scale-out and backpressure

Files: `internal/server/partition.go`, `internal/server/partition_test.go`, `internal/bus/backpressure.go`
Interfaces: produces `server.PartitionFor(runID string, n int) int`, `server.ClaimPartitions(ctx, instanceID string) ([]int, error)`; `bus.WithMaxAckPending(n int) bus.SubOption`.

- [ ] Write `internal/server/partition_test.go` asserting `TestSingleWriterPerRun`: two control-plane instances consuming the same stream never both advance the same run, verified by asserting no duplicate sequence is written for 1000 concurrent runs. Run — expect FAIL with "undefined: server.ClaimPartitions".
- [ ] Add `TestPartitionRebalanceOnInstanceLoss` asserting that killing one of three instances results in its partitions being claimed by the survivors within 5s.
- [ ] Add `TestBackpressureStopsPullingWhenAckPendingReached` asserting the consumer stops fetching once `MaxAckPending` is outstanding and resumes after acks.
- [ ] Implement `internal/server/partition.go` hashing run ids into partitions claimed via NATS KV leases, and `internal/bus/backpressure.go`.
- [ ] Run `make test-integration` — expect PASS. Commit.

## Task 44: Observability

Files: `internal/obs/tracing.go`, `internal/obs/metrics.go`, `internal/obs/obs_test.go`
Interfaces: produces `obs.Init(ctx, cfg obs.Config) (shutdown func(context.Context) error, err error)`, `obs.StepSpan(ctx, runID, stepID string) (context.Context, trace.Span)`.

- [ ] Write `internal/obs/obs_test.go` asserting `TestEachStepEmitsOneSpanWithRunAndStepAttributes`: running the two-step pipeline against an in-memory exporter yields two step spans carrying `dhole.run_id` and `dhole.step_id`, parented to a run span. Run — expect FAIL with "undefined: obs.StepSpan".
- [ ] Add `TestStepResourceMetricsAreRecorded` asserting `dhole_step_cpu_seconds` and `dhole_step_max_rss_bytes` are exported per step.
- [ ] Add `TestBuildDurationRegressionMetricIsExported` asserting `dhole_step_duration_seconds` carries a `cache_hit` label so slow-down analysis can separate the two.
- [ ] Implement `internal/obs/tracing.go` and `metrics.go` with OpenTelemetry, propagating trace context through the `JobDispatch` message so engine-side spans join the run trace.
- [ ] Run `go test ./internal/obs` — expect PASS. Commit.

## Task 45: Web app scaffold and generated client

Files: `web/package.json`, `web/vite.config.ts`, `web/src/main.tsx`, `web/src/api/client.ts`, `web/buf.gen.web.yaml`, `web/src/api/client.test.ts`, `web/playwright.config.ts`
Interfaces: produces `web/src/api/client.ts` exporting `pipelineClient`, `runClient` built from generated Connect-Web stubs; `npm run gen` regenerates them; `make web-check` runs typecheck, lint and unit tests.

- [ ] Write `web/src/api/client.test.ts` asserting `TestClientIsGeneratedNotHandWritten`: importing `pipelineClient` exposes a method for every RPC listed in the generated service descriptor, failing with the missing names. Run `npm test` — expect FAIL with "Cannot find module '../gen/dhole/v1/api_connect'".
- [ ] Write `web/buf.gen.web.yaml` emitting `bufbuild/es` and `connectrpc/es` into `web/src/gen`, and add `npm run gen` invoking it.
- [ ] Scaffold Vite + React 19 + TypeScript strict, TanStack Query, and set `web/package.json` engines to Node 22.
- [ ] Implement `web/src/api/client.ts` wiring the Connect transport with the `Authorization` header from the stored token.
- [ ] Add `make web-check` to `make check` and Playwright to CI with `playwright.config.ts` pointing at a `dhole serve --mode embedded` fixture.
- [ ] Run `make check` — expect PASS. Commit.

## Task 46: Canvas with typed ports

Files: `web/src/canvas/Canvas.tsx`, `web/src/canvas/StepNode.tsx`, `web/src/canvas/edges.ts`, `web/src/canvas/layout.ts`, `web/e2e/canvas-authoring.spec.ts`
Interfaces: produces `<Canvas pipelineId revisionId />`; `layout.autoLayout(steps, edges): NodePositions` (deterministic, derived from the DAG, never persisted).

- [ ] Write `web/e2e/canvas-authoring.spec.ts` asserting `TestCanvasAuthorsPipelineEndToEnd`: add two nodes, drag from `a.out` to `b.in`, set a property, save, and require the resulting revision from the API contains the edge. Run `npx playwright test` — expect FAIL with "locator not found: [data-testid=add-step]".
- [ ] Add a case asserting dragging from a `blob` output to a `structured` input is refused at drop time with a visible message, and no `ApplyOperation` call is made.
- [ ] Add a case asserting node positions are not sent in any `ApplyOperation` request body, proving layout stays out of the document.
- [ ] Implement `web/src/canvas/` with React Flow: `StepNode` rendering one handle per declared port, `edges.ts` validating type compatibility before allowing a connection, `layout.ts` computing deterministic positions from `dag` levels.
- [ ] Run `npx playwright test` — expect PASS. Commit.

## Task 47: Schema-driven property panel and diff review

Files: `web/src/panel/PropertyPanel.tsx`, `web/src/panel/schemaForm.tsx`, `web/src/review/DiffView.tsx`, `web/e2e/panel.spec.ts`
Interfaces: produces `<PropertyPanel stepId />` rendering a form from the plugin's `InputSchema`; `<DiffView revisionId />` showing the diff returned by `ApplyOperation`.

- [ ] Write `web/e2e/panel.spec.ts` asserting `TestPanelIsRenderedFromPluginSchemaNotHardcoded`: publishing a new plugin with an added `retries` integer field causes that field to appear in the panel with no web code change. Run — expect FAIL with "locator not found: [name=retries]".
- [ ] Add a case asserting a value violating the schema is rejected in the form with the schema's own error message before any request is sent.
- [ ] Add a case asserting every save shows a diff the user must confirm, and cancelling it makes no `ApplyOperation` call.
- [ ] Add a case asserting an effect-class override that widens capability is highlighted in the diff as a policy decision point.
- [ ] Implement `schemaForm.tsx` rendering JSON Schema draft 2020-12 to controls, `PropertyPanel.tsx`, and `DiffView.tsx`.
- [ ] Run `npx playwright test` — expect PASS. Commit.

## Task 48: Run view with realised graph and streamed logs

Files: `web/src/run/RunView.tsx`, `web/src/run/LogStream.tsx`, `internal/api/stream.go`, `web/e2e/run-view.spec.ts`
Interfaces: produces `GET /v1/runs/{id}/events` as SSE emitting run and step transitions; `GET /v1/runs/{id}/steps/{step}/logs` as SSE for live tail, falling back to the stored object once complete.

- [ ] Write `web/e2e/run-view.spec.ts` asserting `TestRunViewShowsRealisedGraphCacheHitsAndLogs`: run the two-step pipeline, and require each node shows a duration, the second run shows both nodes marked cached, and log lines appear without a page reload. Run — expect FAIL with "locator not found: [data-testid=run-graph]".
- [ ] Add a case asserting the view switches from the live log subject to the stored object when the run completes, and the full log is present after reload.
- [ ] Add a case asserting a non-cacheable step displays the exact reason string from Task 16.
- [ ] Add a case asserting a bounded loop renders as a container node that expands to its unrolled iterations in the run view.
- [ ] Implement `internal/api/stream.go` with SSE (not WebSocket) plus `Last-Event-ID` resume, and the two React components.
- [ ] Run `npx playwright test` — expect PASS. Commit.

## Task 49: LLM step on go-ai-sdk

Files: `internal/steps/llm/llm.go`, `internal/steps/llm/fingerprint.go`, `internal/steps/llm/record.go`, `internal/steps/llm/llm_test.go`, `internal/runstore/migrations/0008_llm_calls.sql`
Interfaces: produces `llm.Step` implementing the step-type interface; `llm.Config{Provider, Model string; Temperature float32; OutputSchema []byte; MaxTokens int}`; `llm.Fingerprint(resp ai.Response) string`.

- [ ] Write `internal/steps/llm/llm_test.go` asserting `TestLLMStepSchemaFingerprintAndBudgetCeiling` against a stub `ai.LanguageModel`: the step returns an object validated against `OutputSchema`, records model fingerprint, prompt, response, tokens and latency, and halts the run when the token ceiling is exceeded. Run — expect FAIL with "undefined: llm.New".
- [ ] Add `TestMalformedObjectIsRetriedThenFailsWithProviderError` asserting a model returning unparseable JSON three times fails the step with the provider error recorded and never returns a partial object.
- [ ] Add `TestFingerprintIncludesResolvedModelNotAlias` asserting two responses from the same alias but different underlying model ids produce different fingerprints, and that the Task 15 cache key changes accordingly.
- [ ] Add `TestLLMStepIsPureOnlyWithPinnedModelAndZeroTemperature` asserting `EffectClass` resolves to `PURE` only when temperature is 0 and the model is digest-pinned, otherwise `IDEMPOTENT`.
- [ ] Write `0008_llm_calls.sql` creating `llm_calls(tenant_id, run_id, step_id, attempt, model_fingerprint, prompt, response, prompt_tokens, completion_tokens, latency_ms)` with its own retention setting.
- [ ] Implement `llm.go` using `ai.GenerateObject` from `github.com/azrtydxb/go-ai-sdk`, `fingerprint.go`, and `record.go`.
- [ ] Run `go test ./internal/steps/llm` — expect PASS. Commit.

## Task 50: Bounded loops and agent steps

Files: `internal/steps/loop/loop.go`, `internal/steps/agent/agent.go`, `internal/steps/agent/actionspace.go`, `internal/steps/loop/loop_test.go`, `internal/steps/agent/agent_test.go`
Interfaces: produces `loop.Node{Subgraph *dholev1.Pipeline; MaxIterations int; ExitCondition string}` evaluated by CEL; `agent.Step{GrantedSteps []string; MaxSteps int}` exposing granted steps through `agent.AsTool`.

- [ ] Write `internal/steps/loop/loop_test.go` asserting `TestBoundedLoopCeilingAndActionSpaceRefusal`: a loop whose exit condition never holds stops at `MaxIterations` and records an event whose payload contains "iteration ceiling reached". Run — expect FAIL with "undefined: loop.New".
- [ ] Add `TestTopLevelGraphRemainsAcyclicWithLoopNode` asserting `dag.Build` succeeds on a pipeline containing a loop node and that the loop's subgraph is validated independently.
- [ ] Write `internal/steps/agent/agent_test.go` asserting `TestAgentCannotInvokeStepOutsideGrantedSet`: an agent granted only `format` attempting `deploy` is refused with an error naming both.
- [ ] Add `TestAgentCannotInvokeAtMostOnceStepWithoutApproval` asserting the call is routed through the Task 20 approval gate rather than executing.
- [ ] Implement `loop.go` unrolling iterations into the run's event log so the run view can expand them, and `agent.go` wrapping `go-ai-sdk`'s `agent` package — `maxSteps` bounding iteration, `AsTool` exposing only granted steps, and its tool-call approval hook delegating to the approval step.
- [ ] Run `go test ./internal/steps/...` — expect PASS. Commit.

## Task 51: Taint tracking

Files: `internal/taint/taint.go`, `internal/taint/propagate.go`, `internal/taint/taint_test.go`
Interfaces: produces `taint.Mark(v *structpb.Value, source string) *structpb.Value`, `taint.IsTainted(v *structpb.Value) bool`, `taint.Propagate(in []dholev1.OutputRef, out []dholev1.OutputRef)`, `taint.Gate` step type clearing marks after explicit sanitisation.

- [ ] Write `internal/taint/taint_test.go` asserting `TestTaintBlocksEffectfulStepUntilSanitised`: data from an untrusted git webhook reaching an `AT_MOST_ONCE` step is refused with an error naming the trigger source; inserting a `taint.Gate` step allows it. Run — expect FAIL with "undefined: taint.Mark".
- [ ] Add `TestTaintPropagatesThroughPureSteps` asserting a `pure` step consuming tainted input produces tainted output.
- [ ] Add `TestTaintReachingPrivilegedEngineIsRefused` asserting dispatch to an engine advertising `PRIVILEGED` with tainted inputs is denied by policy with a reason naming the taint.
- [ ] Add `TestGateRecordsWhoSanitisedWhat` asserting the gate writes an event naming the principal and the fields cleared.
- [ ] Implement `taint.go` storing marks in a reserved `structpb` field and on CAS object metadata, `propagate.go` called by the scheduler on every step completion, and wire the check into Task 21's policy input.
- [ ] Run `go test ./internal/taint ./internal/policy` — expect PASS. Commit.

## Task 52: The three acceptance pipelines

Files: `acceptance/ci/pipeline.yaml`, `acceptance/automation/pipeline.yaml`, `acceptance/agent/pipeline.yaml`, `acceptance/acceptance_test.go`, `Makefile`
Interfaces: produces `make acceptance-ci`, `make acceptance-automation`, `make acceptance-agent`.

- [ ] Write `acceptance/acceptance_test.go` asserting `TestAcceptanceCICacheHit`: run `acceptance/ci/pipeline.yaml` twice against a real containerd engine and require the second run reports `cache_hit` for the build step and a wall time under 20% of the first. Run — expect FAIL with "no such file: acceptance/ci/pipeline.yaml".
- [ ] Write `acceptance/ci/pipeline.yaml` building a small container image from a checked-in Dockerfile with declared inputs and outputs.
- [ ] Add `TestAcceptanceAutomationTriggersAndWait` running `acceptance/automation/pipeline.yaml` from both a cron schedule and an HTTP call, holding a 5s durable wait across a deliberate control-plane restart, with one step on the process engine and one on Kubernetes.
- [ ] Add `TestAcceptanceAgentLoopAndApproval` running `acceptance/agent/pipeline.yaml`: an LLM step producing schema-validated output, a bounded loop capped at 3, an approval gate decided through the API, and a token cost assertion greater than zero.
- [ ] Add the three `make acceptance-*` targets and run them in CI nightly.
- [ ] Run `make acceptance-ci acceptance-automation acceptance-agent` — expect PASS. Commit.

## Task 53: VM executor with snapshot restore

Files: `internal/executor/vm/vm.go`, `internal/executor/vm/firecracker.go`, `internal/executor/vm/qemu.go`, `internal/executor/vm/vm_test.go`
Interfaces: produces `vm.New(cfg vm.Config) (executor.Executor, error)`; `Config{Backend firecracker|qemu; KernelImage, RootfsImage string; SnapshotDir string; VCPUs int; MemMiB int}`; `EnvironmentIdentity()` returns the rootfs snapshot digest.

- [ ] Write `internal/executor/vm/vm_test.go` calling Task 9's `executorContract` as `TestVMExecutorContract` against Firecracker from `DHOLE_TEST_FIRECRACKER_BIN`, skipping when unset. Run — expect FAIL with "undefined: vm.New".
- [ ] Add `TestSnapshotRestoreBootsUnderOneHundredMilliseconds` asserting a restored microVM reaches the guest agent in under 100ms, measured over 20 iterations.
- [ ] Add `TestEnvironmentIdentityIsSnapshotDigest` asserting the identity changes when the rootfs changes and is stable otherwise, so Task 15's cache key is honest for VM steps.
- [ ] Add `TestNestedVirtCapabilityIsAdvertisedOnlyWhenAvailable` asserting `Capabilities()` omits nested virt on a host without `/dev/kvm`.
- [ ] Implement `firecracker.go` with the Firecracker API over its unix socket including snapshot create and load, `qemu.go` as the portable fallback, and `vm.go` selecting a backend and exposing a guest agent for `Exec`, `Put` and `Get`.
- [ ] Run `make conformance ENGINE="dhole-engine --executor vm"` — expect PASS. Commit.

## Task 54: macOS and Windows process engines

Files: `internal/executor/process/process_darwin.go`, `internal/executor/process/process_windows.go`, `internal/executor/process/process_platform_test.go`, `.github/workflows/ci.yml`
Interfaces: produces build-tagged implementations of the Task 9 signal and process-tree behaviour for `darwin` and `windows`.

- [ ] Write `internal/executor/process/process_platform_test.go` asserting `TestProcessTreeIsKilledOnCancelOnEveryPlatform`: a command spawning a child that outlives its parent is fully terminated within 2s. Run on Windows — expect FAIL with "signal: not supported by windows".
- [ ] Add `TestExitCodeOnOOMIsReportedConsistently` asserting a memory-exhausting command yields a documented exit code on each platform rather than a silent success.
- [ ] Implement `process_windows.go` using a Job Object to kill the process tree, and `process_darwin.go` using process groups with `SIGTERM` then `SIGKILL`.
- [ ] Extend `executorContract` with the two new cases so all executors are held to them, and update `docs/wire-contract.md` with the documented OOM exit codes.
- [ ] Add `macos-latest` and `windows-latest` engine jobs to CI running `make conformance`.
- [ ] Run `make conformance` on all three platforms — expect PASS. Commit.

## Task 55: Dynamic pipelines

Files: `internal/dynamic/generator.go`, `internal/dynamic/generator_test.go`, `web/src/canvas/GeneratorNode.tsx`
Interfaces: produces `dynamic.Generator` step type emitting a `*dholev1.Pipeline` fragment; `dynamic.Splice(parent *dholev1.Pipeline, at string, fragment *dholev1.Pipeline) (*dholev1.Pipeline, error)`.

- [ ] Write `internal/dynamic/generator_test.go` asserting `TestGeneratorEmitsSubgraphSplicedIntoRun`: a generator emitting three steps results in those three executing and the run completing. Run — expect FAIL with "undefined: dynamic.Splice".
- [ ] Add `TestSpliceRejectsDuplicateStepID` asserting a fragment reusing an existing step id fails with an error naming the collision rather than silently overwriting.
- [ ] Add `TestSplicedGraphIsStillAcyclic` asserting a fragment introducing a cycle is rejected at splice time.
- [ ] Add `TestAuthoredGraphShowsGeneratorAsOpaque` (Playwright) asserting the editor renders the generator as an opaque "expands at runtime" node while the run view shows the realised steps.
- [ ] Implement `generator.go` and `Splice`, recording the realised fragment in the event log so replay is deterministic, and `GeneratorNode.tsx`.
- [ ] Run `go test ./internal/dynamic && npx playwright test` — expect PASS. Commit.

## Task 56: Multiplayer editing

Files: `internal/api/presence.go`, `web/src/canvas/Presence.tsx`, `internal/api/presence_test.go`, `web/e2e/multiplayer.spec.ts`
Interfaces: produces `WatchPresence` streaming RPC emitting `PresenceEvent{principal, selection, cursor}`; operations already carry `base_revision` from Task 27.

- [ ] Write `internal/api/presence_test.go` asserting `TestConcurrentOperationsOnDifferentStepsBothApply`: two clients applying `SetProperty` to different steps from the same base revision both succeed and the final revision contains both changes. Run — expect FAIL with "undefined: api.WatchPresence".
- [ ] Add `TestConcurrentOperationsOnSameStepConflict` asserting the second returns `CodeAborted` with the newer revision attached so the client can rebase.
- [ ] Write `web/e2e/multiplayer.spec.ts` asserting two browser contexts see each other's selections and that a conflicting edit surfaces a rebase prompt rather than silently overwriting.
- [ ] Implement `presence.go` over an ephemeral bus subject per pipeline, and `Presence.tsx` rendering remote cursors and selections.
- [ ] Run `go test ./internal/api && npx playwright test` — expect PASS. Commit.

## Task 57: Importers for GitLab CI, GitHub Actions, Woodpecker and n8n

Files: `internal/importers/gitlab.go`, `internal/importers/actions.go`, `internal/importers/woodpecker.go`, `internal/importers/n8n.go`, `internal/importers/importers_test.go`, `testdata/import/`
Interfaces: produces `importers.Importer` with `Import(ctx, src []byte) (*dholev1.Pipeline, importers.Report, error)`; `Report{Unsupported []string; Warnings []string}`.

- [ ] Write `internal/importers/importers_test.go` asserting `TestGitLabCIImportProducesRunnablePipeline`: importing `testdata/import/gitlab-ci.yml` yields a pipeline that passes `dag.Build` and `dag.TypeCheck` with no diagnostics. Run — expect FAIL with "undefined: importers.NewGitLab".
- [ ] Add `TestImportReportsUnsupportedConstructsRatherThanDroppingThem` asserting a GitLab file using `rules:changes` produces a `Report.Unsupported` entry naming it, and that `Import` never silently discards a job.
- [ ] Add `TestSharedWorkspaceIsTranslatedToExplicitArtifacts` asserting a Woodpecker pipeline relying on the implicit workspace produces explicit input and output ports between its steps.
- [ ] Add `TestN8NNodesMapToStepsAndConnectionsToTypedEdges` asserting an n8n export round-trips to a pipeline whose edges carry structured types.
- [ ] Implement the four importers, each emitting `at-most-once` for any step it cannot prove pure, so an import is never more permissive than the original.
- [ ] Run `go test ./internal/importers` — expect PASS. Commit.

## Task 58: Tenant provisioning, quotas and billing metering

Files: `internal/tenancy/provision.go`, `internal/tenancy/quota.go`, `internal/tenancy/meter.go`, `internal/tenancy/tenancy_test.go`, `internal/runstore/migrations/0009_tenancy.sql`
Interfaces: produces `tenancy.Provision(ctx, name string) (Tenant, error)`, `tenancy.Quota{MaxConcurrentSteps, MaxRunsPerDay int; MaxCASBytes int64}`, `tenancy.Meter.Record(ctx, tenantID string, u Usage) error`.

- [ ] Write `internal/tenancy/tenancy_test.go` asserting `TestProvisionCreatesIsolatedTenant`: a provisioned tenant receives its own NATS account credentials, and Task 22's isolation assertions hold against it. Run — expect FAIL with "undefined: tenancy.Provision".
- [ ] Add `TestQuotaExceededRejectsNewRunsWithoutAffectingRunning` asserting a tenant at its daily run quota gets a clear rejection while its in-flight runs complete.
- [ ] Add `TestCASQuotaBlocksWriteBeforeExceeding` asserting a blob write that would exceed `MaxCASBytes` fails with a quota error rather than partially writing.
- [ ] Add `TestMeteredUsageMatchesActualStepSeconds` asserting recorded usage for a known 2s step is within 10% of 2 step-seconds.
- [ ] Write `0009_tenancy.sql` creating `tenants`, `quotas`, `usage_records`.
- [ ] Implement `provision.go`, `quota.go` enforced in the scheduler and CAS, and `meter.go` deriving usage from the run event log so metering is reconstructible.
- [ ] Run `go test ./internal/tenancy` — expect PASS. Commit.

## Task 59: Documentation, release engineering and packaging

Files: `docs/`, `.goreleaser.yaml`, `.github/workflows/release.yml`, `charts/dhole/`, `docs/docs_test.go`
Interfaces: produces `dhole` and `dhole-engine` binaries for linux/darwin/windows on amd64/arm64, multi-arch container images, and a Helm chart.

- [ ] Write `docs/docs_test.go` asserting `TestEveryStepTypeAndTriggerIsDocumented`: enumerate registered step types, trigger kinds and executor kinds and require a matching page under `docs/`, failing with the undocumented names. Run — expect FAIL with "no such file or directory: docs/steps".
- [ ] Add `TestQuickstartCommandsRunAsWritten` extracting fenced `bash` blocks from `docs/quickstart.md` and executing them against a scratch directory, requiring exit 0.
- [ ] Write `docs/` covering quickstart, the wire contract, writing an engine, writing a plugin, policy authoring in CEL, deployment topologies, and the upgrade and version-skew policy.
- [ ] Write `.goreleaser.yaml` producing both binaries for all platform pairs and multi-arch images, `charts/dhole/` deploying control plane, Postgres, NATS and engines, and `.github/workflows/release.yml` publishing on `v*` tags with cosign signing and SBOM attachment.
- [ ] Add `TestReleaseArtifactsAreSignedAndHaveSBOM` verifying the published image with `cosign verify` and requiring an SPDX attestation.
- [ ] Run `goreleaser release --snapshot --clean && go test ./docs` — expect PASS. Commit.
