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
- Generated protobuf messages are always passed and returned by POINTER
  (`*dholev1.Digest`, never `dholev1.Digest`). Every generated message embeds
  `protoimpl.MessageState`, which contains a `sync.Mutex` via `DoNotCopy`, so
  a value copy trips `go vet`'s copylocks check — which has no inline
  suppression and is part of the gate. Discovered building Task 6.
- Migration files are numbered ONCE, here, because tasks build in parallel and
  two branches picking "the next free number" collide at merge. Reserved:
  0001 init (Task 4), 0002 outbox (11), 0003 blob_refs (17), 0004 identity
  (23), 0005 definitions (25), 0006 catalog (30), 0007 signatures (33),
  0008 llm_calls (49), 0009 tenancy (22), 0010 cache_entries (15),
  0011 policy_audit (21), 0012 upstreams (34), 0018 quotas_and_usage (58), 0019 pipeline_heads (27b), 0013 timers (20), 0014 trigger_schedules (40),
  0015 outbox_deployment (18b), 0016 open_runs (18b), 0017 run_sequence (18b),
  0020 terminal_once (bugfix: a run's terminal event is unique, so two racing
  advances cannot both close it), 0021 step_verdict_once (bugfix: the same
  race one level down — a step's STEP_AWAITING_REPLAY and STEP_POLICY_DENIED
  are unique per step, so two racing advances cannot both record it),
  0022 token_subjects_are_principals (bugfix: a token issued by the supported
  path made its subject authenticate but not exist — `IssueToken` wrote
  `tokens`, every "is this a principal of this tenant" check reads
  `principals` — so the holder was refused by `approval.Decide`; the backfill
  keeps a deployment's tokens in flight working),
  0023 gate_armed_once (bugfix: the same race again on the event that arms a
  durable wait — a step's STEP_AWAITING_TIMER is unique per step, so two
  advances that both find one gate ready cannot both record it),
  0024 triggers (the stored trigger table behind CreateTrigger/ListTriggers/
  DeleteTrigger — before it, an event source could only be DECLARED in the
  plane's `--triggers` file, so creating one needed a shell on that host and a
  restart). Definition-attached files (ADR 0023) took NO number and added no
  table on purpose: the declaration is a field of the definition proto, which
  the `revisions` row already stores whole, and the bytes live in the CAS,
  which is not SQL — a table binding revision to file would be a second copy of
  what the definition already says. A task
  needing a new table takes the next number after 0010 and adds it to this
  list in the same commit. The runner must tolerate gaps — a branch carries
  only its own migration until it merges. The runner applies every migration file in
  filename order on every open, with no schema-version table, so each one must
  be idempotent (`CREATE TABLE IF NOT EXISTS`). A migration that ALTERS an
  existing table therefore cannot rely on running once or running last, and
  needs the runner to grow a version table first. Discovered building Task 15.
- Storage is dialect-agnostic through `runstore.Dialect.Rebind`, which
  renumbers `?` into `$n` for Postgres and returns the query byte-identical
  for SQLite. Every store takes `(*sql.DB, runstore.Dialect)`; open one with
  `runstore.OpenSQLite` or `runstore.OpenPostgres`. Do NOT write per-dialect
  copies of a statement — two copies drift silently, and a drifted copy still
  runs, which is worse than the failure it replaced. Five packages had each
  independently written `?` placeholders that pgx rejects, so a Postgres
  deployment had only run_events and the outbox; nothing caught it because
  every per-package suite was green against SQLite. Each store now runs its
  contract against BOTH dialects.
- Postgres and SQLite migrations are told apart by FILENAME: a plain `.sql`
  file is dialect-neutral and applied by BOTH runners, and a same-numbered
  `.postgres.sql` REPLACES it for Postgres. So write the DDL once, and add the
  suffixed form only where a type actually differs (SQLite `BLOB` is Postgres
  `BYTEA`). The earlier rule — Postgres applies only suffixed files — left four
  tables reaching SQLite and silently never reaching Postgres, discovered after
  Tasks 15, 17, 23 and 25 had each added one. A new table is added to the list
  in `internal/runstore/schema_postgres_test.go` in the same commit.

## Task 1: Repository scaffold and quality gate

Files: `go.mod`, `Makefile`, `.golangci.yml`, `.github/workflows/ci.yml`, `internal/version/version.go`, `internal/version/version_test.go`
Interfaces: produces `version.Version() string`, `version.Commit() string`; `make check`, `make test` for every later task.

- [x] Write `internal/version/version_test.go` asserting `TestVersionIsSetAtBuild`: `require.NotEmpty(t, version.Version())` and `require.NotEqual(t, "unknown", version.Version())` when built with ldflags. Run `go test ./internal/version` — expect FAIL with "undefined: version.Version".
- [x] Create `go.mod` with `module github.com/azrtydxb/dhole` and `go 1.26`; add `github.com/stretchr/testify`.
- [x] Implement `internal/version/version.go` with `var version, commit = "unknown", "unknown"` and exported `Version()`/`Commit()` accessors.
- [x] Write `Makefile` with targets `check` (gofmt -l with non-empty failure, go vet, golangci-lint run, buf lint), `test` (`go test ./...`), `build` (ldflags setting `internal/version.version` and `.commit`).
- [x] Write `.golangci.yml` enabling `errcheck, govet, staticcheck, ineffassign, unused, gosec, revive` and `.github/workflows/ci.yml` running `make check test` on `ubuntu-latest` for amd64 and arm64.
- [x] Run `make build && ./dhole` then `go test ./internal/version` — expect PASS. Commit.

## Task 2: Core protobuf schema for pipelines

Files: `proto/dhole/v1/pipeline.proto`, `proto/dhole/v1/common.proto`, `buf.yaml`, `buf.gen.yaml`, `gen/dhole/v1/` (generated), `internal/schema/schema_test.go`
Interfaces: produces messages `Pipeline`, `Step`, `Port`, `PortType`, `Edge`, `Tenant`, enum `EffectClass` (`EFFECT_CLASS_PURE`, `EFFECT_CLASS_IDEMPOTENT`, `EFFECT_CLASS_AT_MOST_ONCE`), enum `Capability`; all later tasks import `gen/dhole/v1`.

- [x] Write `internal/schema/schema_test.go` asserting `TestEffectClassEnumValues`: `require.Equal(t, 1, int(dholev1.EffectClass_EFFECT_CLASS_PURE))` and that `EffectClass_name` has exactly four entries including the zero `EFFECT_CLASS_UNSPECIFIED`. Run `go test ./internal/schema` — expect FAIL with "no required module provides package .../gen/dhole/v1".
- [x] Write `proto/dhole/v1/common.proto` defining `Tenant{string id}`, `Digest{string algo; string hex}`, `EffectClass`, `Capability` (`NETWORK`, `SECRETS`, `PRIVILEGED`, `HOST_MOUNT`).
- [x] Add `LeaseScope` to `common.proto` and a `lease_scope` field to `Step`. The plan told Task 12 to "acquire a sandbox per the step's lease scope", but no message carried one and no later task added it, so the engine could only hardcode `LeaseStep` — a step asking for a pooled sandbox silently got a fresh one. Additive, so `buf breaking` stays clean.
- [x] Write `proto/dhole/v1/pipeline.proto` defining `Pipeline{string id; Tenant tenant; repeated Step steps; repeated Edge edges}`, `Step{string id; string name; string plugin_ref; EffectClass effect_class; repeated Port inputs; repeated Port outputs; repeated Capability capabilities}`, `Port{string name; PortType type}`, `PortType{oneof{BlobType blob; StructType structured}}`, `Edge{string from_step; string from_port; string to_step; string to_port}`.
- [x] Write `buf.yaml` (lint `DEFAULT`, breaking `WIRE_JSON`) and `buf.gen.yaml` emitting `protocolbuffers/go` and `connectrpc/go` into `gen/`. Run `buf generate`.
- [x] Run `go test ./internal/schema` — expect PASS. Add `buf lint` and `buf breaking --against '.git#branch=main'` to `make check`. Commit.

## Task 3: DAG derivation and port type checking

Files: `internal/dag/dag.go`, `internal/dag/typecheck.go`, `internal/dag/dag_test.go`, `internal/dag/typecheck_test.go`
Interfaces: produces `dag.Build(p *dholev1.Pipeline) (*dag.Graph, error)`, `Graph.TopoLevels() [][]string`, `Graph.Dependents(stepID string) []string`, `dag.TypeCheck(p *dholev1.Pipeline) []dag.Diagnostic`, `Diagnostic{StepID, PortName, Message string; Line, Col int}`.

- [x] Write `internal/dag/dag_test.go` asserting `TestDAGDerivedFromPortsRunsIndependentStepsConcurrently`: a four-step pipeline where B and C both depend on A and D depends on both yields `TopoLevels()` of `[["a"],["b","c"],["d"]]`. Run `go test ./internal/dag` — expect FAIL with "undefined: dag.Build".
- [x] Write `internal/dag/dag_test.go` case `TestDAGRejectsCycle` asserting `dag.Build` on a pipeline whose edges form a cycle returns an error containing "cycle: a -> b -> a".
- [x] Implement `internal/dag/dag.go`: build adjacency from `Edge`, detect cycles by DFS colouring, compute levels by Kahn's algorithm with deterministic ordering (sort step ids within a level).
- [x] Write `internal/dag/typecheck_test.go` asserting `TestValidateRejectsIncompatiblePortTypes`: connecting a `blob` output to a `structured` input yields exactly one `Diagnostic` whose `Message` contains both `"a.out"` and `"b.in"`. Run — expect FAIL with "undefined: dag.TypeCheck".
- [x] Implement `internal/dag/typecheck.go` comparing `PortType` oneof arms and, for `structured`, comparing JSON Schema `$id`; return one diagnostic per bad edge, plus one per edge referencing a missing step or port.
- [x] Run `go test ./internal/dag` — expect PASS. Commit.

## Task 4: Event-sourced run store with SQLite

Files: `internal/runstore/store.go`, `internal/runstore/sqlite.go`, `internal/runstore/migrations/0001_init.sql`, `internal/runstore/sqlite_test.go`
Interfaces: produces `runstore.Store` interface with `Append(ctx, tenantID string, e Event) error`, `Replay(ctx, tenantID, runID string) ([]Event, error)`, `LastSequence(ctx, tenantID string) (uint64, error)`, `Close() error`; `Event{RunID, StepID string; Attempt uint32; Sequence uint64; Type EventType; Payload []byte; At time.Time}`.

- [x] Write `internal/runstore/sqlite_test.go` asserting `TestControlPlaneRestartMidRunReplaysWithoutDuplication`: append five events, close the store, reopen it, replay, and `require.Len(t, events, 5)` in sequence order. Run — expect FAIL with "undefined: runstore.NewSQLite".
- [x] Add `TestAppendIsIdempotentOnDuplicateSequence`: appending the same `(RunID, StepID, Attempt, Sequence)` twice returns nil both times and `Replay` still yields one event.
- [x] Write `internal/runstore/migrations/0001_init.sql` creating `run_events(tenant_id, run_id, step_id, attempt, sequence, type, payload, at)` with `PRIMARY KEY (tenant_id, run_id, step_id, attempt, sequence)` and index on `(tenant_id, sequence)`.
- [x] Implement `internal/runstore/store.go` (interface, `Event`, `EventType` constants `RUN_CREATED`, `STEP_READY`, `STEP_DISPATCHED`, `STEP_SUCCEEDED`, `STEP_FAILED`, `RUN_COMPLETED`) and `internal/runstore/sqlite.go` using `modernc.org/sqlite`, with `INSERT ... ON CONFLICT DO NOTHING` for idempotency.
- [x] Add `TestQueryWithoutTenantIsRejected` asserting `Replay(ctx, "", runID)` returns an error containing "tenant scope required".
- [x] Run `go test ./internal/runstore` — expect PASS. Commit.

## Task 5: Postgres run store

Files: `internal/runstore/postgres.go`, `internal/runstore/postgres_test.go`, `internal/runstore/migrations/0001_init.postgres.sql`, `docker-compose.test.yml`
Interfaces: produces `runstore.NewPostgres(ctx, dsn string) (runstore.Store, error)` satisfying the same `runstore.Store` interface as Task 4.

- [x] Write `internal/runstore/postgres_test.go` with `TestPostgresSatisfiesStoreContract` running the identical assertions as Task 4's tests against a Postgres DSN from `DHOLE_TEST_POSTGRES_DSN`, skipping with `t.Skip("DHOLE_TEST_POSTGRES_DSN not set")` when absent. Run — expect FAIL with "undefined: runstore.NewPostgres".
- [x] Extract Task 4's assertions into `internal/runstore/contract_test.go` exposing `runStoreContract(t *testing.T, s runstore.Store)` and call it from both the SQLite and Postgres tests, so the two implementations are held to one contract.
- [x] Write `0001_init.postgres.sql` mirroring the SQLite schema with `BIGINT` sequences and `TIMESTAMPTZ`.
- [x] Implement `internal/runstore/postgres.go` with `jackc/pgx/v5`, using `INSERT ... ON CONFLICT DO NOTHING`.
- [x] Write `docker-compose.test.yml` starting `postgres:17` on port 55432 and add `make test-integration` exporting the DSN and running `go test ./... -tags=integration`.
- [x] Run `make test-integration` — expect PASS. Commit.

## Task 6: Content-addressed store

Files: `internal/cas/cas.go`, `internal/cas/filesystem.go`, `internal/cas/cas_test.go`
Interfaces: produces `cas.Store` interface with `Put(ctx, tenantID string, r io.Reader) (*dholev1.Digest, error)`, `Get(ctx, tenantID string, d *dholev1.Digest) (io.ReadCloser, error)`, `Has(ctx, tenantID string, d *dholev1.Digest) (bool, error)`; `cas.NewFilesystem(root string) cas.Store`.

- [x] Write `internal/cas/cas_test.go` asserting `TestPutIsContentAddressedAndStable`: putting the same bytes twice yields an identical digest and `Has` reports true; putting different bytes yields a different digest. Run — expect FAIL with "undefined: cas.NewFilesystem".
- [x] Add `TestGetMissingDigestReturnsNotFound` asserting `Get` on an absent digest returns an error satisfying `errors.Is(err, cas.ErrNotFound)`.
- [x] Add `TestTenantsCannotReadEachOthersBlobs`: put bytes as tenant `a`, then `Has(ctx, "b", digest)` returns false.
- [x] Implement `internal/cas/cas.go` (interface, `ErrNotFound`) and `internal/cas/filesystem.go` writing to `<root>/<tenant>/<algo>/<hex[:2]>/<hex>` via a temp file plus atomic rename, computing SHA-256 while streaming.
- [x] Run `go test ./internal/cas` — expect PASS. Commit.

## Task 7: Object storage for logs and artifacts

Files: `internal/blobstore/blobstore.go`, `internal/blobstore/s3.go`, `internal/blobstore/filesystem.go`, `internal/blobstore/blobstore_test.go`
Interfaces: produces `blobstore.Store` with `Write(ctx, tenantID, key string, r io.Reader) error`, `Read(ctx, tenantID, key string) (io.ReadCloser, error)`, `URL(ctx, tenantID, key string, ttl time.Duration) (string, error)`; `blobstore.NewS3(cfg S3Config)`, `blobstore.NewFilesystem(root string)`.

- [x] Write `internal/blobstore/blobstore_test.go` with `blobStoreContract(t *testing.T, s blobstore.Store)` asserting write-then-read round-trips bytes and reading an absent key returns `blobstore.ErrNotFound`; call it for the filesystem implementation as `TestFilesystemBlobStoreContract`. Run — expect FAIL with "undefined: blobstore.NewFilesystem".
- [x] Add `TestS3BlobStoreContract` running the same contract against MinIO from `DHOLE_TEST_S3_ENDPOINT`, skipping when unset; add MinIO to `docker-compose.test.yml` on port 59000.
- [x] Implement `internal/blobstore/blobstore.go`, `filesystem.go`, and `s3.go` using `aws-sdk-go-v2` with path-style addressing so MinIO works.
- [x] Add `TestWriteFailureIsReportedNotSwallowed` asserting a write to a read-only root returns a non-nil error mentioning the key.
- [x] Run `make test-integration` — expect PASS. Commit.

## Task 8: Engine wire protocol and version negotiation

Files: `proto/dhole/v1/engine.proto`, `internal/wire/negotiate.go`, `internal/wire/negotiate_test.go`, `docs/wire-contract.md`
Interfaces: produces messages `JobDispatch`, `JobStatus`, `LogChunk`, `EngineRegistration`, `EngineHeartbeat`, `EngineControl`; `wire.ProtocolVersion = 1`, `wire.SupportedWindow = 1`, `wire.Negotiate(engineVersions []uint32) (uint32, error)`, `wire.NegotiateAgainst(ours uint32, engineVersions []uint32) (uint32, error)`.

- [x] Write `internal/wire/negotiate_test.go` asserting `TestNegotiateAcceptsCurrentAndPrevious` against `NegotiateAgainst`, which takes the control plane's own version as an argument: `NegotiateAgainst(1, []uint32{1})` returns 1; `NegotiateAgainst(1, []uint32{0})` returns an error containing "unsupported protocol"; `NegotiateAgainst(2, []uint32{1})` returns 1; `NegotiateAgainst(3, []uint32{1})` is refused. `ProtocolVersion` is a constant and cannot be reassigned from a test, so the window is exercised through the parameter and `Negotiate` is the thin wrapper that supplies it. Run — expect FAIL with "undefined: wire.Negotiate".
- [x] Write `proto/dhole/v1/engine.proto`: `JobDispatch{string run_id; string step_id; uint32 attempt; string fence_token; Step step; repeated InputRef inputs; repeated SecretRef secrets; string output_prefix; uint32 protocol_version}`, `JobStatus{string run_id; string step_id; uint32 attempt; string fence_token; Phase phase; int32 exit_code; repeated OutputRef outputs; string error}`, `LogChunk{string run_id; string step_id; uint64 seq; bytes data; Stream stream}`, `EngineRegistration{string engine_id; repeated uint32 protocol_versions; repeated Capability capabilities; string os; string arch; uint32 slots; repeated string engine_types}`, `EngineHeartbeat{string engine_id; repeated InFlight in_flight}`, `EngineControl{oneof{Cancel cancel; Drain drain; Attach attach}}`.
- [x] Implement `internal/wire/negotiate.go` selecting the highest mutually supported version, refusing anything below `ProtocolVersion-1`.
- [x] Write `docs/wire-contract.md` documenting every message field, the subject layout `job.dispatch.<tier>.<caps>`, `job.status.<run>.<step>`, `job.logs.<run>.<step>`, `engine.control.<engine-id>`, `engine.heartbeat.<engine-id>`, and the rule that a `JobDispatch` is self-contained.
- [x] Run `go test ./internal/wire && buf lint` — expect PASS. Commit.

## Task 9: Executor interface and local process engine

Files: `internal/executor/executor.go`, `internal/executor/executortest/contract.go`, `internal/executor/process/process.go`, `internal/executor/process/process_unix.go`, `internal/executor/process/process_other.go`, `internal/executor/process/process_test.go`, `internal/executor/contract_test.go`. The contract body lives in its own `executortest` package, not in a `_test.go` file: a test file is invisible to other packages, so `process_test.go` could not otherwise call it, and every future backend (container, VM, k8s) imports `executortest` to inherit the identical assertions.
Interfaces: produces `executor.Executor` with `Acquire(ctx, spec Spec) (Sandbox, error)`, `Capabilities() []dholev1.Capability`, `Kind() string`; `executor.Sandbox` with `Exec(ctx, cmd Cmd) (ExitCode int32, err error)`, `Put(ctx, name string, r io.Reader) error`, `Get(ctx, name string) (io.ReadCloser, error)`, `Signal(ctx, sig Signal) error`, `Release(ctx) error`; `executor.LeaseScope` constants `LeaseStep`, `LeaseJob`, `LeasePipeline`, `LeasePool`, `LeaseService`.

- [x] Write `internal/executor/contract_test.go` exposing `executorContract(t *testing.T, e executor.Executor)` asserting: a sandbox runs `echo hi` with exit 0 and stdout `hi`; a command exiting 3 reports `ExitCode == 3`; `Signal(SIGTERM)` during `sleep 30` returns within 2s; `Put` then `Get` round-trips bytes; `Release` twice is not an error.
- [x] Write `internal/executor/process/process_test.go` calling `executorContract` as `TestProcessExecutorContract`. Run — expect FAIL with "undefined: process.New".
- [x] Implement `internal/executor/executor.go` with the interfaces, `Spec{Image string; Env map[string]string; WorkDir string; Lease LeaseScope; Requirements Requirements}` and `Requirements{OS, Arch string; Capabilities []dholev1.Capability}`.
- [x] Implement `internal/executor/process/process.go` running commands via `os/exec` in a temp directory, propagating `SIGTERM` to the process group with `Setpgid`, and reporting `Capabilities()` as `{}` — no privileged, no host mount.
- [x] Add `TestProcessExecutorReportsNoEnvironmentIdentity` asserting `process.New().EnvironmentIdentity()` returns `("", executor.ErrNoStableIdentity)` so Task 16 can mark its steps non-cacheable.
- [x] Run `go test ./internal/executor/...` — expect PASS. Commit.

## Task 10: Embedded NATS and bus client

Files: `internal/bus/bus.go`, `internal/bus/nats.go`, `internal/bus/embedded.go`, `internal/bus/subjects.go`, `internal/bus/nats_test.go`
Interfaces: produces `bus.Bus` with `Publish(ctx, subject string, msg proto.Message) error`, `Request(ctx, subject string, msg proto.Message, out proto.Message) error`, `SubscribePull(ctx, stream, consumer, subject string) (Subscription, error)`, `SubscribeEphemeral(ctx, subject string, fn func([]byte)) (func(), error)`; `bus.StartEmbedded(dir string) (*bus.Embedded, error)`; `bus.SubjectDispatch(tier, capsHash string) string` and siblings in `subjects.go`.

- [x] Write `internal/bus/nats_test.go` asserting `TestEmbeddedBusRoundTripsRequestReply`: start embedded NATS, register a responder on `engine.control.e1`, `Request` returns the reply. Run — expect FAIL with "undefined: bus.StartEmbedded".
- [x] Add `TestPullConsumerRedeliversUnackedMessage`: publish to a work-queue stream, receive without acking, close the subscription, resubscribe, and require the same message is delivered again.
- [x] Implement `internal/bus/subjects.go` with the exact subject builders from `docs/wire-contract.md` and a `TestSubjectsMatchDocumentedContract` asserting `SubjectDispatch("untrusted","abc") == "job.dispatch.untrusted.abc"`.
- [x] Implement `internal/bus/embedded.go` running `nats-server` in-process with JetStream on a temp dir, and `internal/bus/nats.go` wrapping `nats.go` plus `jetstream` for pull consumers.
- [x] Add `TestEngineCannotSubscribeToForeignTier` asserting a connection with credentials scoped to `untrusted` receives a permissions error subscribing to `job.dispatch.trusted.*`.
- [x] Run `go test ./internal/bus` — expect PASS. Commit.

## Task 11: Outbox bridging store and bus

Files: `internal/outbox/outbox.go`, `internal/outbox/outbox_test.go`, `internal/runstore/migrations/0002_outbox.sql`
Interfaces: produces `outbox.Outbox` with `Enqueue(ctx, tx runstore.Tx, tenantID, subject string, msg proto.Message) error`, `Drain(ctx) (int, error)`; `runstore.Store.WithTx(ctx, fn func(Tx) error) error` added to Task 4's interface.

- [x] Write `internal/outbox/outbox_test.go` asserting `TestEventAndPublishCommitTogether`: within one `WithTx`, append an event and enqueue a message, then force the transaction to roll back and require neither is present. Run — expect FAIL with "undefined: outbox.New".
- [x] Add `TestDrainPublishesThenMarksSent` asserting `Drain` publishes to the bus and a second `Drain` returns 0.
- [x] Add `TestDrainRetriesAfterBusFailure`: with the bus stopped, `Drain` returns an error and leaves the row unsent; after restarting the bus, `Drain` publishes it.
- [x] Write migration `0002_outbox.sql` creating `outbox(id, tenant_id, subject, payload, created_at, sent_at NULL)` with an index on `sent_at IS NULL`.
- [x] Implement `WithTx` on both store implementations and `internal/outbox/outbox.go` with a polling drainer on a 200ms ticker.
- [x] Run `go test ./internal/outbox` — expect PASS. Commit.

## Task 12: Engine agent runtime

Files: `internal/engine/agent.go`, `internal/engine/registry_client.go`, `internal/engine/agent_test.go`, `cmd/dhole-engine/main.go`
Interfaces: produces `engine.Agent` with `Run(ctx) error`, `engine.Config{EngineID, Tier string; Bus bus.Bus; Executor executor.Executor; Blobs blobstore.Store; CAS cas.Store; Slots int}`; publishes `EngineRegistration` on start and `EngineHeartbeat` every 5s.

- [x] Write `internal/engine/agent_test.go` asserting `TestOutboundOnlyEngineRegistersAndExecutes`: start an embedded bus, run an agent with the process executor, publish a `JobDispatch` running `echo hi`, and require a `JobStatus` with `Phase_SUCCEEDED` and exit 0 arrives on `job.status.<run>.<step>`. Run — expect FAIL with "undefined: engine.Agent".
- [x] Add `TestAgentStreamsLogsToEphemeralSubjectAndWritesAuthoritativeCopy`: run `printf 'a\nb\n'`, require two `LogChunk` messages on `job.logs.<run>.<step>` and that the blobstore holds the full output at the key named in `JobStatus`.
- [x] Add `TestAgentHeartbeatsListInFlightSteps` asserting a heartbeat during a `sleep 5` step includes that step in `in_flight`.
- [x] Add `TestAgentRefusesDispatchWithUnsupportedProtocolVersion` asserting a `JobDispatch` with `protocol_version: 99` yields a `JobStatus` with `Phase_FAILED` and error containing "unsupported protocol".
- [x] Implement `internal/engine/agent.go`: pull consumer on the dispatch subject filtered by capability hash, acquire a sandbox per the step's lease scope, stream stdout/stderr to both the ephemeral log subject and the blobstore, publish `JobStatus`, ack only after status is published.
- [x] Implement `cmd/dhole-engine/main.go` reading `DHOLE_BUS_URL`, `DHOLE_ENGINE_ID`, `DHOLE_TIER` and starting the agent.
- [x] Run `go test ./internal/engine` — expect PASS. Commit.

## Task 13: Leases, fencing tokens and orphan detection

Files: `internal/lease/lease.go`, `internal/lease/lease_test.go`, `internal/lease/kv.go`
Interfaces: produces `lease.Manager` with `Claim(ctx, tenantID, runID, stepID string, attempt uint32, ttl time.Duration) (Token, error)`, `Renew(ctx, t Token) error`, `Validate(ctx, t Token) error`, `Expire(ctx) ([]Orphan, error)`; `Token{Value string; Fence uint64}`.

- [x] Write `internal/lease/lease_test.go` asserting `TestFenceIncrementsPerAttempt`: claiming attempt 1 then attempt 2 for the same step yields `Fence` values that strictly increase. Run — expect FAIL with "undefined: lease.New".
- [x] Add `TestAtMostOnceRejectsDuplicateDeliveryByFence`: claim a lease at fence 1, claim again producing fence 2, then `Validate` the fence-1 token and require an error satisfying `errors.Is(err, lease.ErrFenced)`.
- [x] Add `TestHeartbeatExpiryRedeliversAndCatalogPersists`: claim a lease with a 100ms TTL, do not renew, and require `Expire` returns that step as an orphan after the TTL.
- [x] Implement `internal/lease/kv.go` over the NATS KV bucket `dhole-leases` with per-key revision as the fence source, and `internal/lease/lease.go` wrapping it.
- [x] Run `go test ./internal/lease` — expect PASS. Commit.

## Task 14: Scheduler and dispatcher

Files: `internal/scheduler/scheduler.go`, `internal/scheduler/match.go`, `internal/scheduler/scheduler_test.go`
Interfaces: produces `scheduler.Scheduler` with `Advance(ctx, tenantID, runID string) error`, `OnStatus(ctx, s *dholev1.JobStatus) error`; `scheduler.Match(req executor.Requirements, engines []registry.Instance) []registry.Instance`.

- [x] Write `internal/scheduler/scheduler_test.go` asserting `TestAdvanceDispatchesOnlyReadySteps`: for the Task 3 diamond pipeline, the first `Advance` dispatches only `a`; after `a` succeeds, the next dispatches `b` and `c` but not `d`. Run — expect FAIL with "undefined: scheduler.New".
- [x] Add `TestMatchFiltersByCapabilityOSAndArch` asserting a step requiring `PRIVILEGED` on `linux/arm64` matches only an instance advertising all three.
- [x] Add `TestUnschedulableStepReportsWhy` asserting a step whose requirements match no instance produces an event whose payload contains "no engine advertises capability PRIVILEGED".
- [x] Wire `policy.Engine.Evaluate` into the scheduler before dispatch. This is the second half of Task 21, which could only do the definition-save side because the scheduler did not exist yet. The save guard also cannot populate `Input.Signed` or `Input.Upstream` — those come from Tasks 30 and 33 — so the scheduler is where a dispatch-time decision gets the full input.
- [x] Implement `internal/scheduler/match.go` (pure filtering, no I/O) and `internal/scheduler/scheduler.go` reading the run's event log, computing ready steps from `dag.Graph`, claiming a lease, and enqueuing a `JobDispatch` through the outbox.
- [x] Run `go test ./internal/scheduler` — expect PASS. Commit.

## Task 15: Cache keys and skip-on-hit

Files: `internal/cache/key.go`, `internal/cache/cache.go`, `internal/cache/key_test.go`, `internal/cache/cache_test.go`
Interfaces: produces `cache.Key(step *dholev1.Step, envIdentity string, inputs []*dholev1.Digest, lockfile map[string]string) (*dholev1.Digest, error)`, `cache.Lookup(ctx, tenantID string, k *dholev1.Digest) ([]*dholev1.OutputRef, bool, error)`, `cache.Record(ctx, tenantID string, k *dholev1.Digest, outs []*dholev1.OutputRef) error`.

- [x] Write `internal/cache/key_test.go` asserting `TestKeyIsStableAcrossOrderingAndUnstableOnInputChange`: reordering the `inputs` slice yields the same key; changing one input digest changes it; changing `envIdentity` changes it; changing a lockfile entry changes it. Run — expect FAIL with "undefined: cache.Key".
- [x] Add `TestKeyRefusesNonPureStep` asserting `cache.Key` on a step whose `EffectClass` is `AT_MOST_ONCE` returns an error containing "only pure steps are cacheable".
- [x] Add `TestKeyRefusesEmptyEnvironmentIdentity` asserting an empty `envIdentity` returns an error containing "no stable environment identity".
- [x] Write `internal/cache/cache_test.go` asserting `TestCacheHitOnUnchangedInputsAndInvalidationOnChange` end to end against the SQLite store and filesystem CAS.
- [x] Implement `internal/cache/key.go` hashing a canonical protobuf encoding of `(step command, image digest/envIdentity, sorted input digests, sorted lockfile pairs)` with SHA-256, and `internal/cache/cache.go` persisting `cache_entries(tenant_id, key, outputs, created_at)`.
- [x] Run `go test ./internal/cache` — expect PASS. Commit.

## Task 16: Pool leases degrade cacheability visibly

Files: `internal/cache/eligibility.go`, `internal/cache/eligibility_test.go`
Interfaces: produces `cache.Eligible(step *dholev1.Step, lease executor.LeaseScope, envIdentity string) (bool, string)` returning the reason when false.

- [x] Write `internal/cache/eligibility_test.go` asserting `TestPoolLeaseMarksStepNonCacheable`: a `pure` step under `LeasePool` returns `(false, "pool lease reuses state that cannot be hashed")`. Run — expect FAIL with "undefined: cache.Eligible".
- [x] Add `TestPureStepUnderStepLeaseIsEligible` asserting `(true, "")` for `LeaseStep` with a non-empty `envIdentity`.
- [x] Add `TestEffectfulStepIsNeverEligible` asserting both `IDEMPOTENT` and `AT_MOST_ONCE` return false with a reason naming the effect class.
- [x] Implement `internal/cache/eligibility.go` and call it from the scheduler, recording the reason on the `STEP_DISPATCHED` event so the run view can display it.
- [x] Run `go test ./internal/cache` — expect PASS. Commit.

## Task 17: CAS refcount garbage collection

Files: `internal/cas/gc.go`, `internal/cas/gc_test.go`, `internal/runstore/migrations/0003_blob_refs.sql`
Interfaces: produces `cas.GC{Store cas.Store; Runs runstore.Store; DB *sql.DB}` with `Collect(ctx, tenantID string, retain time.Duration) (freed int, err error)`, plus `cas.OpenIndex(path)`, `cas.Reference(...)` and a `Delete` method on the filesystem store. The `DB` field is not in the original signature and is needed because `runstore.Store` is an interface with no handle accessor: the collection must drop blob refs, cache entries and the blobs themselves in ONE transaction, which is only possible with the shared handle. The alternative — a `DB()` accessor on `runstore.Store` — would put a storage detail on an interface every caller sees.

- [x] Write `internal/cas/gc_test.go` asserting `TestRefcountGCPreservesRetainedRunBlobs`: two runs each producing a blob, one run aged beyond `retain`; `Collect` frees exactly the expired run's blob and `Has` still reports the retained one. Run — expect FAIL with "undefined: cas.GC".
- [x] Add `TestGCNeverCollectsBlobSharedWithRetainedRun` asserting a blob referenced by both an expired and a retained run survives.
- [x] Add `TestCacheEntryIsDroppedWhenItsOutputBlobIsCollected` asserting the `cache_entries` row is removed alongside the blob, so a later lookup misses rather than returning a dangling reference.
- [x] Write `0003_blob_refs.sql` creating `blob_refs(tenant_id, digest, run_id)` with a composite primary key.
- [x] Implement `internal/cas/gc.go` computing the retained run set, then deleting blobs with no remaining reference, in that order so a concurrent run cannot lose a blob.
- [x] Run `go test ./internal/cas` — expect PASS. Commit.

## Task 18: Single binary and the first end-to-end run

Files: `cmd/dhole/main.go`, `internal/server/server.go`, `internal/server/singlebinary.go`, `internal/server/e2e_test.go`, `testdata/pipelines/two-step.yaml`
Interfaces: produces `server.New(cfg server.Config) (*server.Server, error)`, `Server.Start(ctx) error`, `Server.Stop(ctx) error`; `server.Config{Mode ModeEmbedded|ModeDistributed; StoreDSN, BusURL, BlobRoot string}`.

- [x] Write `internal/server/e2e_test.go` asserting `TestSingleBinaryRunsTwoStepPipeline`: start a server in `ModeEmbedded`, submit `testdata/pipelines/two-step.yaml` (step `a` writes a file, step `b` reads it), and require the run reaches `RUN_COMPLETED` with `b`'s output containing `a`'s bytes. Run — expect FAIL with "undefined: server.New".
- [x] Add `TestSingleBinaryAndDistributedParity` running the same pipeline in `ModeDistributed` against the compose-provided Postgres, NATS and MinIO, requiring identical run output.
- [x] Add `TestEmbeddedEngineUsesLoopbackBusNotDirectCall` asserting the run's dispatch appears on the bus subject, proving there is no in-process shortcut.
- [x] Implement `internal/server/singlebinary.go` wiring embedded NATS, SQLite, filesystem CAS and blobstore, and an in-process `engine.Agent` connected over loopback.
- [x] Implement `cmd/dhole/main.go` with `dhole serve`, reading flags `--mode`, `--store-dsn`, `--bus-url`, `--blob-root`.
- [x] Run `go test ./internal/server && make test-integration` — expect PASS. Commit.

## Task 18b: What the first end-to-end run found

Task 18 proved the architecture runs: a two-step pipeline completes from one
binary, and the same definition produces identical bytes against Postgres, an
out-of-process NATS and MinIO. It also surfaced seven gaps that no unit test
could have shown, because each lives between components. They are recorded
here as work, not as notes, because the plan had no task for any of them.

Files: `internal/runstore/`, `internal/outbox/`, `internal/scheduler/`, `internal/server/`, `proto/dhole/v1/engine.proto`
Interfaces: adds an open-run index to `runstore.Store`; scopes `outbox` claims; adds a message discriminator to the engine subjects or their payloads.

- [x] **The outbox claim is scoped to nothing.** `SELECT ... WHERE sent_at IS NULL` carries no tenant and no deployment id, so two control planes sharing a database steal each other's messages — observed for real: the server's drainer claimed and published `internal/outbox`'s own test fixtures onto its own bus. Scope the claim, and write the test that fails when two planes share a store.
- [x] **Nothing can enumerate unfinished runs.** `Replay` needs a run id you already have, so the set of runs still to advance lives only in the server's memory and a restart cannot rediscover a run that is merely waiting. This contradicts ADR 0003, whose whole claim is that a restart is a replay. Add an open-run index to the store and drive the advance loop from it.
- [x] **Nothing re-advances a run on its own.** `Advance` runs only when a status arrives, so a run submitted before any engine registered records `STEP_UNSCHEDULABLE` and stalls forever. Task 18 added a 250ms poll to get the run through; replace it with something driven by the index above, and keep the test that submits a run before any engine exists.
- [x] **A lost `EngineRegistration` is permanent.** It is fire-and-forget on a core subject, and `Heartbeat` refuses to rebuild an instance (Task 31, deliberately). An engine that starts before the plane is invisible until it restarts. Decide between a durable registration subject and a periodic re-announce, and implement it.
- [x] **An engine message's type cannot be recovered from its bytes.** An `EngineHeartbeat` decodes cleanly as an `EngineRegistration` — both start with `engine_id`, and packed `repeated uint32` shares a wire type with `repeated message`. Task 18 hit this by guessing from content and registering engines with an empty platform, which made every step unschedulable. It now subscribes to `engine.>` and dispatches on the subject. Put the discriminator somewhere it cannot be lost, and say so in the wire contract.
- [x] **Orphan re-dispatch is not expressible.** `lease.Expire` returns orphans, but `scheduler.plan` counts any step with `attempts > 0` as in flight forever and no event says an attempt died. Add the event and the sweeper that writes it.
- [x] **Two runs can share a sequence.** `run_events`'s primary key is `(tenant, run, step, attempt, sequence)`, so the per-tenant log has no single total order. Decide whether it needs one — the outbox and the run view both assume order somewhere — and either make the sequence per-tenant or document what it does order.

## Task 15b: The cache is never consulted on a real run

Files: `internal/scheduler/scheduler.go`, `internal/engine/agent.go`, `internal/cache/`
Interfaces: the scheduler consults `cache.Lookup` before dispatching an eligible step, and records `cache.Record` when one succeeds.

Task 15 is titled "Cache keys and skip-on-hit" and built the first half. The
second half was never wired: `cache.Lookup` and `cache.Record` have exactly one
caller between them, `api.Plan`, which is required to have no side effects. The
scheduler calls only `cache.Eligible`, and only to record WHY a step is not
cacheable on its dispatch event.

So a real run executes every step, every time. `Plan` truthfully reports which
steps would hit a cache that a run will never consult, which is worse than
having no cache — the report says the work will be skipped and it is not.
ADR 0009 makes the content-addressed cache a v1 core primitive. Found while
building Task 44, whose `cache_hit` metric label is always false for this
reason.

- [x] Write the failing test first: run the two-step pipeline twice against one store and require the second run to skip the first step and reuse its outputs. It must fail on the tree as it stands.
- [x] Consult `cache.Lookup` in the scheduler before dispatching a step that `cache.Eligible` accepts, and emit a STEP_SUCCEEDED-equivalent carrying the recorded outputs instead of a dispatch.
- [x] Record a successful eligible step's outputs with `cache.Record`, keyed by `cache.Key` over its resolved inputs, the environment identity and the lockfile.
- [x] A cached step must still produce the same run events a real one does, so the run view and the DAG cannot tell the difference — apart from the recorded cache hit.
- [x] Feed the real `cache_hit` into Task 44's `dhole_step_duration_seconds` label, replacing the constant false.
- [x] Never serve a hit for a step whose effect class is not PURE or whose lease scope is not step-scoped — `cache.Eligible` already decides this; call it, do not restate it.

## Task 18c: Sequence allocation, left over from 18b

Files: `internal/wait/`, `internal/steps/approval/`, `internal/api/`
Interfaces: every caller appends with `Sequence: 0` and lets the store allocate.

Task 18b made the run event log's sequence genuinely per-tenant, allocated by
the store inside the caller's transaction. It could not change three packages
that were being edited concurrently, and they still compute an explicit
sequence themselves. The allocator's high-water-mark clause stops it from
overtaking them, so nothing is broken today — but they can still collide with
each other, and a collision is silent: the append is `ON CONFLICT DO NOTHING`
for idempotence, so the loser is simply never written.

- [x] `internal/wait`, `internal/steps/approval` and `internal/api` append with `Sequence: 0`.
- [x] Add the test that two of them appending concurrently cannot lose an event — it must fail on the tree as it stands.

## Task 52b: What the acceptance pipelines found

All three acceptance pipelines run and pass — CI with a real cache hit
(11.3s cold, 2.6ms warm, with no step of the second run reaching an engine at
all), automation surviving a deliberate control-plane restart with one step on
the host and one in a pod, and the agent profile with a schema-validated model
answer, a loop capped at three and an approval gate. They pass, and the way
they pass is the finding: the acceptance harness supplies wiring the server
does not have.

Files: `internal/server/`, `internal/scheduler/`, `internal/steps/`, `internal/wait/`, `internal/trigger/`, `proto/dhole/v1/api.proto`, `.github/workflows/`

- [x] **`dhole serve` runs no step types, no triggers and no timer poll.** It
      imports none of `internal/steps/{llm,loop,approval,agent}`, none of
      `internal/trigger/*`, and never runs `internal/wait`'s poll. The
      acceptance harness IS the missing dispatcher — it drives all of them
      against the same store and tenant. Until the server does this, the three
      profiles are a claim the tests make and the product does not.
      Closed by `internal/server/builtins.go` and `internal/server/triggers.go`:
      `scheduler.BuiltinSteps` is a hook in the ready loop, and the plane
      registers `builtin:{timer,approval,llm,loop}` behind it; `Config.Triggers`
      runs cron schedules on their own poll and mounts `http`/`git` endpoints
      under `server.TriggerPrefix` on the API's own listener, declared by the
      `--triggers` flag; `internal/wait`'s Runner is a spawned loop like the
      outbox and the sweeper. `Server.Approve` decides a gate the plane armed;
      `Server.OpenRuns` is how a caller finds a run a trigger started. Tested
      through `server.New`/`Start` alone in
      `internal/server/builtins_e2e_test.go` — no test supplies wiring.
- [x] **A definition file is never collected, and ADR 0023 says it is.** That
      record's Consequences claim "a file removed by an operation is not deleted
      from the CAS: it is unreferenced, and the collector reclaims it when
      nothing points at it". The first half was true and the second was not:
      `cas.GC` enumerates `blob_refs`, and only a step OUTPUT ever writes a row
      there (`GC.Reference`), so a definition file had no row, was never
      enumerated, and was retained for ever against the MaxCASBytes quota those
      very files are charged to.
      Closed by teaching the collector what a REVISION pins. `cas.GC.Collect`
      now takes its candidates from two tables and its retention from two
      holders: a blob survives if a run within the retain window references it
      OR if some revision carries it as a file, because identical bytes are one
      object and each holder's claim on it is whole
      (`unpinnedDigests`/`carriedDigests` in `internal/cas/gc.go`).
      SCHEMA DECISION, recorded at length in migration
      `0025_definition_blobs.sql`: a SIBLING table `definition_blobs`
      (tenant_id, digest, uploaded_at), not a widened `blob_refs` key. Widening
      loses twice — SQLite cannot change a primary key in place, so the one
      table the collector's safety argument reads would have to be rebuilt by
      a runner that re-applies every file on every open; and a run reference
      and an upload candidacy have opposite lifetimes, so every query that ages
      a row by its run would have to learn to skip the rows that have none, and
      the one predicate somebody forgets ages a definition file instantly and
      deletes a file a pinned revision still carries. What the table
      deliberately does NOT hold is which revision carries which digest: that
      is derived from the stored definition protobuf on each sweep, because a
      denormalised copy that a future write path forgets to write is a pinned
      file the collector believes is an orphan. The cost is decoding a tenant's
      definitions on a sweep that has candidates to judge, and the sweep skips
      the read entirely when it has none.
      RETENTION IS BY REVISION, AND REVISIONS ARE NEVER PRUNED. Detaching a
      file therefore reclaims nothing: the revision that carried it is
      immutable and still names the bytes, which is what lets a run pinned to
      it stay reproducible and what makes the inverse of the detachment work.
      What this reclaims today is the upload no revision ever carried — the
      GUI's file picker, an abandoned edit, a push that failed validation — and
      the same rule reclaims a replaced file unchanged on the day revision
      retention arrives.
      An upload younger than the retain window is never collected, because
      `PutDefinitionFile` is a separate call from the edit that declares the
      file and in between the bytes are carried by nothing; re-uploading the
      same bytes restarts that window rather than keeping the first upload's
      time.
      EXISTING ROWS: a deployment written before this migration keeps whatever
      it already leaked, and no backfill is possible. The leaked bytes are
      precisely the ones no revision carries, so the revisions table cannot
      name them, and the CAS cannot be walked for them because it also holds
      plugin artifacts and outputs whose reference has not been written yet.
      Convergence is from the next write on: an upload of the same bytes
      records the row, and everything uploaded afterwards is a candidate. The
      failure direction is the safe one — an unrecorded blob is never examined.
      Quota: collection credits `MaxCASBytes` back through the existing
      `GC.Collected` hook (`internal/tenancy`'s Reclaimer), after the COMMIT,
      because the ledger lives in the same database and a SQLite pool of one
      connection deadlocks on a read taken inside the collector's transaction.
      Files: `internal/cas/gc.go`, `internal/cas/definitionfiles.go` (the
      upload wrapper that records, wired at `Files:` in
      `internal/server/api.go`), `internal/runstore/migrations/0025_definition_blobs.sql`.
      Tested in `internal/cas/definition_gc_test.go` — the one that matters is
      `TestAPinnedRevisionStillResolvesEveryFileItCarriesAfterTheHeadMovedOn` —
      on BOTH dialects through `internal/cas/dialect_test.go`'s contract
      against a live Postgres, and through the served contract in
      `internal/server/files_api_test.go`. ADR 0023 is completed, not changed,
      so no record supersedes it.
- [x] **A parked agent step is not resumed after its gate is decided.** All
      five clauses of the original "what the plane does not host" are closed. (a) is closed by ADR 0025 and
      `internal/steps/agent/contract.go` + `internal/server/agent.go`: an agent
      acts ONLY through Dhole's own public API, as a principal of its tenant,
      with an action space of `start_run`, `read_run`, `decide_approval` and
      `apply_operation` and nothing else. The invoker is a Connect client over
      the plane's OWN loopback listener carrying a token minted for the agent's
      subject, NOT a direct call into `api.Server` — ADR 0013's point is that
      there is no privileged path, and an in-process shortcut would be the one
      caller that missed every interceptor added later. Taint follows the
      CREDENTIAL as well as the value, so a CEL rule can refuse an at-most-once
      effect to an agent while allowing it to a person. An agent cannot run a
      command, and `TestAnAgentStepHasNoPathToExecutingACommand` asserts the
      package cannot even reach `internal/executor`. (b), (d) and (e) are
      closed in their own entries below, and (c) is closed by ADR 0022 — a
      `builtin:loop` body is a `dhole.v1.Pipeline` fragment in `config.body`,
      spliced into the SAME run under its own id prefix rather than becoming a
      nested run. All five original clauses are therefore done. THE SIXTH,
      which was new rather than one of the five, is now closed too: a parked
      agent IS resumed. It parks rather than failing — the model's loop
      suspends with the gated batch unexecuted — and `AGENT_PARKED` records
      the whole conversation, the ids of the calls the person is being asked
      about and how much of `max_steps` was spent, in the run log and nowhere
      else (ADR 0003: the plane that parked it may be gone when a person wakes
      up). The gate records that it PARKS a step rather than IS one
      (`approval.Request.Parks`, `RequestPark`), so an approval writes
      `STEP_RESUMED` instead of `STEP_SUCCEEDED` — the scheduler folds that as
      "out of flight, dispatchable again" — and the step is re-dispatched,
      reads its own park record and re-enters the loop AT THE CALL IT STOPPED
      ON. REPLAY SAFETY is the SDK's own suspend/resume and not a skip list:
      the parked conversation is handed back verbatim with every earlier call
      already answered in it, so the model is never re-prompted and no earlier
      action is taken a second time — which for an agent acting through the
      public API would mean a second run started or a second operation
      applied. The CEILING is not reset: a resume gets `max_steps` minus what
      was spent, and a loop that parked on its last step is refused with
      `ErrCeilingSpent`. A DENIAL ends the step and fails the run naming the
      approver rather than being handed back to the model as a tool result —
      an agent told "no" and left running routes around the person who said
      it, and a gate is keyed on (run, step) so there is no second gate for
      whatever it tried next. `SweepOrphans` no longer fails a step waiting at
      a gate: its holder finished and went away, and a lease TTL is not how
      long a person has to decide. One gate per step per run stands, and a
      second park is refused by the standing decision with the step failing
      readably. Proved end to end through the shipping plane in
      `internal/server/agent_resume_e2e_test.go` — approve and deny — and
      property by property in `internal/steps/agent/park_test.go`. No ADR: it
      completes ADR 0025 and ADR 0015 rather than changing either. No
      migration: the events are rows in `run_events`, which needs no schema.
- [x] **(b) Nothing leased the PLANE a secret, so `builtin:llm` had no key.**
      `server.Config.Models` was a factory a deployment had to construct with an
      API key in hand, and the CLI passed none — so every `builtin:llm` step
      failed with that named reason. Closed by ADR 0024: the plane redeems its
      own secrets through the broker it already serves. A model configuration
      NAMES a secret (`api_key_secret` in the step's config); the plane resolves
      it at CALL time through `internal/secrets` (`Source`, `MapSource`,
      `PlaneResolver`), as a principal of the tenant whose step is running, and
      hands the value to the factory as `server.ModelRequest.APIKey` — which is
      why `ModelFactory` now takes that struct instead of two strings. The
      broker's rules are unchanged: single use, an issuer-enforced expiry, a
      refusal naming neither handle nor value. Redeemed per call and never
      cached, so a provider key rotates without a restart and an hour-long run
      holds no value for an hour. `server.DefaultModels` builds anthropic and
      openai clients from the redeemed key and REFUSES to fall back to the
      provider libraries' `os.Getenv` default — the ambient credential is the
      trap this design exists to avoid. The `--model-secret` flag on
      `dhole serve` supplies the values (never on argv), and the chart's
      `controlPlane.modelSecrets` reads each from an existing Kubernetes
      Secret. START-UP ORDERING is now a rule with a test: the broker serves
      BEFORE the advance loop, the builtin workers and the hosted engine, or
      the first LLM step of a fresh plane races its own credential
      (`internal/server/startup_order_test.go`). A plane with no factory, or one
      naming a secret it cannot resolve, still fails the step with a named
      reason — never a nil dereference and never a silent skip. No migration.
- [x] **(d) An operator could not create a trigger through the contract.**
      Triggers were declared on `server.Config` and read from a YAML file by
      `--triggers`, so creating one needed a shell on the control plane's host
      and a restart — the same shape as `CreatePipeline` and `PublishPlugin`,
      and what ADR 0013 refuses. Closed by `CreateTrigger`, `ListTriggers` and
      `DeleteTrigger` on `PipelineService` (`internal/api/triggers.go`), the
      `triggers` table of migration 0024 (`internal/trigger/store.go`), and
      `dhole trigger create|list|delete`. The binding is validated against the
      pipeline's ACTIVE revision at creation, which is where ADR 0007 puts it.
      A created trigger goes live without a restart: `internal/server`'s
      reconciler polls the table every second and rebuilds the webhook mux the
      listener dispatches through, so a second plane over one database runs the
      first plane's triggers too. PRECEDENCE: a DECLARED trigger wins. The
      `--triggers` path is untouched and still refuses at start-up; a create or
      delete naming a declared id is refused with that reason, a stored row of
      that id is shadowed, and `ListTriggers` shows both halves with `declared`
      set — an operator mid-migration can see everything they have. A trigger's
      shared secret travels inwards only: the contract returns `has_secret` and
      never the value.
- [x] **(e) A builtin step in flight when the plane died was never recovered.**
      It wrote STEP_DISPATCHED and held no lease, so `SweepOrphans` — which
      finds dead holders through `lease.Expire` and by no other means — could
      not see it, and no engine would ever report a status for a step no engine
      ever had. The run stayed in flight forever. The plan's own suggested fix
      was right and is what landed: `internal/server/builtins.go` claims a
      lease before the dispatch event, renews it every TTL/3 for as long as the
      work runs, and validates the fence INSIDE the transaction that writes the
      step's verdict — so a swept attempt cannot decide a step a later attempt
      is running. `server.Config.LeaseTTL` now governs a plane's own steps and
      its engines' alike. The behavioural test in
      `internal/server/builtin_recovery_test.go` stops a plane mid-model-call
      and a NEW plane over the same store recovers the run; its companion
      proves a step slower than the TTL is not swept out from under itself.
- [x] VERIFIED ON REAL INFRASTRUCTURE 2026-09-10: all four acceptance tests pass
      against the kw k3s cluster and a live Postgres — `TestAcceptanceCICacheHit`,
      `TestCIPipelineBuildsTheCheckedInDockerfile`,
      `TestAcceptanceAutomationTriggersAndWait` and
      `TestAcceptanceAgentLoopAndApproval`, 98s, zero skipped. That is the first
      time the three profiles have actually run rather than skipping, and it is
      the evidence the gate-atomicity work could not produce for itself: the
      automation pipeline's five-second workaround is gone and `builtin:wait`
      holds it instead.
- [x] **Arming a durable gate is not atomic with the readiness decision.**
      `STEP_AWAITING_TIMER` was written in its own transaction, so it could appear
      earlier in the log than the `STEP_DISPATCHED` of the step it should have
      gated — the sequence is allocated inside the transaction, the visibility
      is not. The gate was ignored and the wait skipped entirely. Closed by
      `internal/steps/gate`, the `builtin:wait` step type: the scheduler
      recognises a gate from the PINNED DEFINITION rather than from the log —
      so there is no commit that can arrive too late to be seen — and arms it
      through `wait.Timers.ArmInTx` in the transaction that would have
      dispatched it, via the `scheduler.Gate` seam (`internal/scheduler/gate.go`,
      four small hunks in `scheduler.go`). Migration 0023 makes a step's
      `STEP_AWAITING_TIMER` unique so two advances that both find one gate ready
      cannot both record it. `acceptance/automation/pipeline.yaml`'s five-second
      predecessor is GONE and its `hold` step is now a `builtin:wait`; the
      acceptance test no longer arms the wait out of band. The agent pipeline's
      five-second `brief` remains and is no longer this bug: it holds `classify`
      and `refine` back because nothing maps `builtin:llm`/`builtin:loop` to a
      step type, which is the wiring item above.
- [x] **There is no approval RPC**, so "an approval gate decided through the
      API" cannot be met as written. The run is created, approved and started
      through the real contract, and the gate is then decided by the principal
      that contract authenticated — through the Go API, not the wire.
      `PipelineService.DecideApproval` closes it: the approver is the
      credential's subject and never a request field,
      `dhole run approve <run-id> <step-id>` is its CLI surface, and
      `internal/server/approval_api_test.go` decides a gate over a real HTTP
      connection to a plane started by `server.New`/`Start` and asserts the run
      is released. `acceptance/acceptance_test.go` still decides through the Go
      API; moving it onto the RPC needs a live Postgres and is left to whoever
      owns that file.
- [x] **A credential's identity and an approver's identity live in different
      tables.** A token issued to a subject authenticates every API call and is
      then refused by `approval.Decide` as "not a principal of tenant":
      `IssueToken` writes `tokens`, `PrincipalCredential` reads `principals`.
      `IssueToken` now establishes the principal at the mint, through a new
      `Store.EnsurePrincipal` that inserts and never replaces — an upsert there
      would wipe the password of a person who is also issued a token — and
      migration 0022 backfills the subjects of tokens already in flight, so an
      existing deployment does not have to reissue them.
- [x] **A step naming an engine KIND is still not routed to one — the filter
      decides, and the bus does not obey.** Reopened 2026-09-10 by running it on
      kw: a step with `engine_type: vm`, on a cluster with a vm-backed engine
      and a kubernetes-backed engine in the SAME tier, ran on the kubernetes
      one. Proven rather than inferred — the step wrote
      `/proc/sys/kernel/osrelease` to an output and it read
      `6.12.58-current-rockchip64`, the node's kernel, where a microVM would
      have said `6.1.102`.
      The half that works: `scheduler.Match` filters on engine type and
      `Explain` refuses a step no engine can take. The half that does not: a
      dispatch is published to `job.dispatch.<tier>.<capsHash>`, engine type is
      NOT part of that subject, and `internal/engine` does not check the step's
      engine type on receipt — so every engine in the tier with matching
      capabilities pulls from the same work queue and whichever grabs it first
      runs it. Match therefore decides whether a step CAN be placed and has no
      say in where it GOES.
      The natural fix is to make engine type part of the dispatch subject, as
      tier and capabilities already are, and have an engine subscribe to its
      own kind. That changes the subject layout, which `docs/wire-contract.md`
      publishes, so it needs the N-1 window thinking that the protocol version
      already has. Refusing on receipt instead is cheaper and worse: the
      dispatch has already been taken off the queue, and redelivery to another
      engine is a race rather than a route.
      Unit tests could not have caught this — they exercise `Match` in
      isolation, where it is correct. It took a real pipeline on a cluster with
      two engine kinds in one tier.
      CLOSED 2026-09-11: the kind is a token of the dispatch SUBJECT, and the
      existing subject is kept as the "any kind" one. A step naming no kind
      still goes to `job.dispatch.<tier>.<caps>`, byte for byte; a step naming
      one goes to `job.dispatch.<tier>.<caps>.<kind>`. An engine binds BOTH —
      two durable consumers, `engines-<tier>-<caps>` and
      `engines-<tier>-<caps>-<kind>`, never one consumer with two filter
      subjects: that consumer is named for the tier and capability set alone
      and is therefore SHARED by every engine in the tier, so widening it would
      hand kind-targeted work to engines of the wrong kind, which is the bug
      rebuilt. Backward compatible by construction: NATS subjects are
      token-exact unless wildcarded (asserted on a live server), so an engine
      predating the token keeps taking unrestricted work and can never be
      handed kind-targeted work it would not know to honour — which preserves
      the N-1 promise without the plane having to know which engine will pull a
      message off a queue. No protocol bump: the version says what an engine
      speaking it must DO, and an older engine must do nothing differently
      here. The tier's subscribe permission widened from
      `job.dispatch.<tier>.*` to `job.dispatch.<tier>.>` — `*` matches exactly
      one token, so under it an engine was refused its OWN kind's subject —
      with the tier token still fixed, and the foreign-tier refusal asserted on
      the longer shape too. Verified by
      `TestAStepNamingAnEngineKindRunsOnAnEngineOfThatKindWhenTwoKindsShareATier`:
      two engines of different kinds in ONE tier over a real bus, a step
      naming each kind, each running on the right engine and neither on the
      other — the test whose absence let this ship. Not yet re-run on kw; the
      cluster check is the owner's.
      SUPERSEDED TEXT: **Nothing routes a step to an engine KIND.** `scheduler.Match` does not
      filter on engine type, so a capability is the only lever — the pipeline
      asks for NETWORK to reach a pod. A step's placement on the process engine
      is not expressible at all. CLOSED: `Step.engine_type` is the lever, and
      it is read off the step by BOTH `Scheduler.requirements` and
      `api.Server.requirements`, so the planner's answer and the dispatcher's
      are one computation over one field — the disagreement the filter was
      withheld for. `Match` refuses an engine that did not offer the kind (an
      engine advertising none satisfies no such step: unstated is unknown, not
      universal, the rule the platform axes already follow), and `Explain`
      names it between platform and capability, coarsest cause first, so a step
      that cannot be placed is reported rather than held.
- [x] **A tier boundary an engine can step over with a pull consumer.** Found
      2026-09-11 while asserting tier isolation for the kind subject, and older
      than that subject: `internal/bus/embedded.go` granted a tier's
      credentials `$JS.API.CONSUMER.>`, so an untrusted connection could create
      a durable on the DISPATCH stream filtered to `job.dispatch.trusted.>` and
      receive trusted work — the subscribe allow-list never sees a pull
      consumer's filter subject, because no core SUB on the dispatch subject
      happens. CLOSED for the create path, and [S-5] — "refused at the bus
      subject level rather than by application code" — is now true of it. A
      single-filter consumer create travels on
      `$JS.API.CONSUMER.CREATE.<stream>.<consumer>.<filter subject>` and the
      server refuses a body whose filter disagrees with the subject, so the
      permission is `$JS.API.CONSUMER.CREATE.*.*.job.dispatch.<tier>.>` and the
      three endpoints that carry the filter in the BODY alone — the new form
      with no filter token (also the only multi-filter form), the legacy
      durable endpoint and the legacy ephemeral endpoint — are not granted at
      all. Verified by `TestAnEngineCannotBindAWorkQueueFilteredToAnotherTier`
      and, for those three, by
      `TestAnEngineCannotReachTheConsumerCreateEndpointsThatHideTheFilterSubject`,
      which drives the raw API subjects rather than the client, since the
      client only ever sends the one form. `SubscribePull` now reports the
      refusal as `ErrPermissionDenied` instead of waiting out the caller's
      deadline: the server answers a forbidden publish with an -ERR on the
      connection and never replies, so a permissions failure arrived as
      "context deadline exceeded". The same table is now exported as
      `bus.TierPermissions` and used by `internal/tenant`'s new
      `ProvisionTierUser`, so the two copies cannot drift apart again.
      NOT closed, and carried below: pulling an EXISTING consumer by name.
- [x] **A tier boundary an engine can still step over by NAME.** Left open by
      the fix above and demonstrated on 2026-09-11 with a throwaway test: an
      untrusted connection that publishes to
      `$JS.API.CONSUMER.MSG.NEXT.DISPATCH.engines-trusted-<caps>` pulls trusted
      work off the consumer a trusted engine created, without creating anything
      itself. The consumer name is guessable — engines are named
      `engines-<tier>-<caps>` — and a NATS wildcard matches a WHOLE token, so
      `engines-untrusted-*` is not expressible and no permission can narrow
      MSG.NEXT or INFO to one tier while the stream is shared. This is not
      fixable in `internal/bus` alone. The fix is to put the tier in a token
      those subjects already carry: one dispatch stream per tier
      (`DISPATCH_<tier>`), declared by whoever calls `EnsureWorkQueue`
      (`internal/server/singlebinary.go`, `engine.DispatchStream`), after which
      `$JS.API.CONSUMER.MSG.NEXT.DISPATCH_<tier>.*` is a tier-scoped
      permission and the name stops mattering. A Go-side check is not the fix:
      an engine in another language would not have it.
      CLOSED 2026-09-11: one work-queue stream per tier, `DISPATCH_<tier>`
      (`bus.DispatchStreamName`), and all three consumer-API permissions —
      CREATE, MSG.NEXT and INFO — now name this tier's stream instead of `*`.
      The attack is the test: `TestAnEngineCannotPullFromAConsumerAnotherTiersEngineCreated`
      has an untrusted connection publish to
      `$JS.API.CONSUMER.MSG.NEXT.<trusted stream>.engines-trusted-abc`, the
      exact consumer a trusted engine created, and asserts the server refuses
      it on permissions AND that the dispatch is still there for the engine it
      was meant for. It failed before the change by pulling the payload out.
      A STREAM NAME IS NOT A SUBJECT: nats-server refuses `.`, `*`, `>`, `/`,
      `\` and whitespace in a stream name and caps it at 255 bytes, so
      `ValidTierToken` now enforces the union of the two rules and every place
      a tier is configured (`StartEmbeddedWithTiers`, `bus.TierPermissions`
      and through it `tenant.ProvisionTierUser`, `EnsureDispatchStreams`)
      refuses `local/dev` by name rather than failing at the first dispatch.
      UPGRADE ORDER, written into `docs/wire-contract.md`: the PLANE FIRST,
      the opposite of the kind-token order, because the plane is what declares
      streams and the old `DISPATCH` covers `job.dispatch.>`, which overlaps
      every per-tier stream — JetStream refuses that, so the two layouts
      cannot coexist and one side is briefly wrong either way. Plane first
      makes the losing side an engine that cannot find `DISPATCH_<tier>` and
      retries the bind forever (deliberately deadline-free), while its tier's
      work accumulates in that queue and is taken the moment it is upgraded:
      HELD, never lost and never misrouted, since a per-tier stream accepts
      only its own tier's subjects. THE OLD STREAM is handled explicitly by
      `EnsureDispatchStreams`: empty, it is deleted (it holds nothing and its
      subjects block every per-tier stream); non-empty, the plane REFUSES TO
      START, naming the stream and the count, because those messages are
      dispatches live runs are waiting on and deleting them would strand those
      runs silently. `bus.TierPermissions` stayed the one table both
      `internal/bus` and `internal/tenant` build from.
- [x] **A distributed deployment hands engines a credential that is not tier
      scoped.** `internal/tenancy`'s provisioner gives out the tenant ACCOUNT
      credential (`tenant.ProvisionAccount`), which reaches the tenant's whole
      subject space including every tier's `job.dispatch.>` — it is also what
      the plane's own components connect as. An engine holding it is separated
      from other tiers by nothing but its own choice of filter subject.
      `tenant.ProvisionTierUser` now issues a tier-scoped credential inside the
      tenant's account, carrying `bus.TierPermissions`, and
      `TestAnEngineInATenantAccountIsLimitedToItsOwnTier` proves an engine
      holding one binds BOTH of its own tier's queues and is refused another
      tier's. Nothing called it: provisioning returned only the account
      credential, so this was closed in the mechanism and open in the
      deployment.
      CLOSED. The deployment path now has TWO named credentials instead of one
      field a later edit could hand to either caller: `Tenant.PlaneCredentials`
      is the account credential the control plane keeps — unnarrowed, because
      the plane publishes to every tier and declares every tier's stream — and
      `Provisioner.EngineCredentials(ctx, tenant, tier)` issues the tier user
      an engine gets. THE TIER IS A PARAMETER OF ISSUING, not something read
      from the engine's `DHOLE_TIER`: a credential whose scope its holder
      chooses is a request, not a boundary, so `DHOLE_TIER` now only decides
      which queue an engine tries to pull and the credential decides which it
      may. `EngineCredentials` refuses a tenant that is not registered (a typo
      would otherwise mint a live account), is idempotent so a re-run does not
      lock out the engines holding the old credential, and inherits
      `bus.TierPermissions`' refusal of a tier that is not a legal subject
      token. `TestAnEngineCredentialTheProvisionerIssuesCannotReachAnotherTier`
      asserts it through the deployment path, both halves: the engine binds its
      own tier's two queues and is refused the trusted tier's.
      AN EXISTING DEPLOYMENT ROTATES; nothing is refused and nothing breaks.
      The account credential stays valid because the plane connects as it, so
      engines still holding it keep running — which also means upgrading fixes
      nothing by itself and such an engine still reaches every tier until an
      operator acts. Per tenant: issue a credential per tier, put each in a
      Secret, point that tier's engines at it
      (`engines[].busCredentialsSecret`, new in the chart; the install notes
      now WARN for every tier without one), roll the engines. The chart and
      docs/deployment.md both claimed the tier boundary already existed while
      the chart handed every engine one shared unauthenticated URL; both are
      corrected, and docs/upgrades.md carries the operator path.
- [x] **One tier holding two engine kinds turns its whole cache off.** Found
      2026-09-11 on kw, in the plane's own log, which says in as many words
      that the engines of tier `trusted` disagree about their environment
      and that nothing is cached for it — the two identities being a busybox
      image digest (the Kubernetes engine) and a rootfs digest (the VM
      engine). Both engines are correct; the aggregation is not.
      `registry.TierEnvironmentIdentity` folds every
      instance of a TIER into one identity and reports a conflict when they
      differ, and `Scheduler.tierIdentity` takes no step and therefore no kind,
      so a step that explicitly names `engine_type: vm` — which can only land
      on the VM engine, whose identity is unambiguous — is refused a cache key
      along with everything else in the tier. Before engine-kind routing a tier
      was one kind in every deployment and the fold was right; making a kind
      addressable made a mixed tier a normal configuration, and this is the
      half of that change that was missed. The fix is to resolve the identity
      against the engines a step could ACTUALLY reach: kind-scoped when the
      step names one, tier-wide when it does not, where a genuine disagreement
      among the reachable engines still means no key. A step naming no kind in
      a mixed tier must stay uncached — it really can land on either.
      CLOSED. `registry.TierEnvironmentIdentity` now takes the engine kind and
      folds only the instances of that tier offering it — the same filter
      `Match` applies, including its rule that an engine advertising no kind
      answers for no named one — and an empty kind still folds the whole tier.
      `Scheduler.tierIdentity` and `api.Plan` both pass what the step named, so
      a plan and a run resolve the same set. A step naming `vm` in a mixed tier
      caches against the VM engine's identity; a step naming no kind in that
      same tier still caches nothing, and two engines of ONE kind that disagree
      still cache nothing for it. The operator log is now per (tier, kind) and
      names which engines disagreed — "the vm engines of this tier" — latched
      per set, which also fixed a flip-flop that logged the conflict line and
      the no-identity line for every single step. ADR 0026 supersedes ADR 0021
      with the narrowed set.
- [x] **A trigger's bound inputs never reach the run it starts.** (Was: "a
      pipeline cannot name the image its steps run in" — that, the repository
      file reference and the loop body syntax are all closed; see below.) (the executor's pod
      template does) — CLOSED on all three counts it once named.
      `Step.image` reaches `executor.Spec.Image` through the dispatch
      (`JobDispatch.step` already carries the whole step) and is what the cache
      key is hashed against. The FILE reference is closed by ADR 0023: a file a
      step needs is part of the DEFINITION, not of a repository —
      `Pipeline.files` carries it, `Step.file_inputs` binds it to an input
      port, and `dag.FileInputs` resolves that into the same `InputRef` an edge
      produces, so the engine materialises it at `inputs/<port>` with no engine
      change at all and the digest lands in the cache key for free. The LOOP
      BODY's syntax is closed by ADR 0022: `config.body` holds a
      `dhole.v1.Pipeline` as JSON and an iteration is spliced into the run
      under its own id prefix. CLOSED on the last count too. The seam was
      `Server.triggerSink`: it called `s.Submit(ctx, tenantID, pipeline)`,
      which takes no inputs, and put the bound values in a log line. They now
      travel on the `RUN_CREATED` payload —
      `scheduler.RunCreated.Inputs`/`.StartedBy`, written through
      `Server.SubmitWithInputs` — which is the run log `WatchRun` already
      streams, so nothing new was added to the wire and there is no second
      channel to replay. A tainted value is stored WRAPPED, so the run log
      says which boundary the value crossed rather than flattening it into
      "an input" (ADR 0015). The sink is now also the gate: every fire is
      checked with `trigger.ValidateInputs` against the ACTIVE revision the
      run is about to pin — not the pipeline the trigger was configured with —
      and a binding that supplied nothing is refused by name, so no run starts
      on bare ports. `schedule` had no value check at all before this.
      Left open deliberately, and NOT part of this item's text: a step does
      not yet READ one of these values at its port, which needs the value
      materialised in the CAS and resolved into the same `InputRef` an edge
      produces.
- [x] **The LLM step halts the run it is given** when it gives up, so an
      off-schema answer cannot be asserted within a run that must continue.
      Closed: giving up now records `STEP_FAILED` and nothing else, so whether
      the run continues is decided by the effect class and the edges as it is
      for any other failed step (ADR 0002). `llm.Options.HaltsRun` keeps the old
      behaviour for the caller whose run genuinely has nothing left to do. The
      guarantee that an off-schema answer FAILS is untouched: the failure is in
      the log, `Run` returns no object, and nothing partial flows downstream.
      The agent acceptance test now makes the off-schema assertion inside the
      run it is testing, and that run completes.
- [x] **`dhole serve` arms durable gates and never fires them.** CLOSED BY THE
      MERGE, and neither branch could have closed it alone: the dispatcher
      branch wired `wait.NewRunner` as `Server.timerLoop`, spawned beside the
      outbox and the sweeper, while the atomicity branch wired the arming. Each
      reported the other half as still missing because each was looking at a
      tree without it. `builtin:timer` is now an ALIAS of `builtin:wait` rather
      than a second implementation — it armed out of band, which is precisely
      the bug the gate closes, under a different plugin_ref — so
      `TestATimerStepMakesTheRunWaitAndThePlanesOwnPollEndsTheWait` exercises
      arm-and-fire end to end through `server.New`/`Start` alone, with nothing
      supplied by the test.
- [x] **No nightly CI job runs the acceptance pipelines** — `.github/` was
      outside the task's scope. Closed: `.github/workflows/nightly.yml` provisions a kind cluster and a Postgres service, runs `make acceptance`, and FAILS the job if any acceptance test merely skipped — a skipped acceptance suite reads as a green one, which is worse than not running it.

## Task 19: Effect classes, retry and idempotency keys

Files: `internal/effects/effects.go`, `internal/effects/retry.go`, `internal/effects/effects_test.go`
Interfaces: produces `effects.RetryPolicy(step *dholev1.Step) effects.Policy`; `Policy{MaxAttempts int; Backoff time.Duration; RequiresIdempotencyKey bool; RequiresExclusiveLease bool}`; `effects.IdempotencyKey(runID, stepID string, attempt uint32) string`.

- [x] Write `internal/effects/effects_test.go` asserting `TestPolicyPerEffectClass`: `PURE` yields `MaxAttempts: 3, RequiresExclusiveLease: false`; `IDEMPOTENT` yields `RequiresIdempotencyKey: true`; `AT_MOST_ONCE` yields `MaxAttempts: 1, RequiresExclusiveLease: true`. Run — expect FAIL with "undefined: effects.RetryPolicy".
- [x] Add `TestIdempotentRetryReusesIdempotencyKey` asserting `IdempotencyKey` is identical across attempts 1 and 2 of the same step and differs between steps.
- [x] Add `TestAtMostOnceStepIsNeverAutoRetried` driving the scheduler through a step failure and requiring no second dispatch is enqueued, and that an event of type `STEP_AWAITING_REPLAY` is recorded instead.
- [x] Implement `internal/effects/effects.go` and `retry.go`, and change the scheduler from Task 14 to consult `RetryPolicy` before re-dispatch and to require a valid lease fence for `RequiresExclusiveLease`.
- [x] Run `go test ./internal/effects ./internal/scheduler` — expect PASS. Commit.

## Task 20: Durable waits, timers and human approval

Files: `internal/wait/wait.go`, `internal/wait/timer.go`, `internal/wait/wait_test.go`, `internal/steps/approval/approval.go`, `internal/steps/approval/approval_test.go`
Interfaces: produces `wait.Timers` with `Schedule(ctx, tenantID, runID, stepID string, at time.Time) error`, `Due(ctx, now time.Time) ([]wait.Due, error)`; `approval.Step` with `Request(ctx, runID, stepID string, prompt string) error`, `Decide(ctx, runID, stepID string, approver string, approved bool) error`.

- [x] Write `internal/wait/wait_test.go` asserting `TestDurableWaitSurvivesRestart`: schedule a timer 200ms out, stop and restart the server, and require the run resumes and completes. Run — expect FAIL with "undefined: wait.Timers".
- [x] Add `TestMissedScheduleWindowFiresOnceOnRecovery` asserting a timer whose due time passed entirely while the server was down fires exactly once, not once per missed interval.
- [x] Write `internal/steps/approval/approval_test.go` asserting `TestApprovalGateBlocksUntilDecided`: a run containing an approval step does not proceed until `Decide(approved: true)`, and the approver identity is recorded in the event log.
- [x] Add `TestApprovalDenialFailsRunWithReason` asserting `Decide(approved: false)` produces `RUN_COMPLETED` with a failure whose payload names the approver.
- [x] Implement `internal/wait/timer.go` persisting timers in the run store (so they survive restart) with a 1s poll for due entries, and `internal/steps/approval/approval.go` as a step type that emits `STEP_AWAITING_APPROVAL` and resumes on decision.
- [x] Run `go test ./internal/wait ./internal/steps/approval` — expect PASS. Commit.

## Task 21: CEL policy engine, tiers and audit

Files: `internal/policy/policy.go`, `internal/policy/cel.go`, `internal/policy/audit.go`, `internal/policy/policy_test.go`
Interfaces: produces `policy.Engine` with `Evaluate(ctx, in policy.Input) (policy.Decision, error)`; `Input{Tier, TenantID, Subject string; Capabilities []dholev1.Capability; EffectClass dholev1.EffectClass; PluginRef string; Signed bool; Upstream string}`; `Decision{Allow bool; Rule, Reason string}`.

- [x] Write `internal/policy/policy_test.go` asserting `TestForbiddenCapabilityRefusedAtSave`: a tier whose rule is `!("PRIVILEGED" in input.capabilities)` denies a step requesting `PRIVILEGED`, with `Decision.Rule` naming the rule id. Run — expect FAIL with "undefined: policy.New".
- [x] Add `TestPolicyDenialAuditedWithinLatencyBudget` asserting a denial writes an audit row containing rule, tier and subject, and that 1000 sequential evaluations average under 10ms each with the compiled-program cache warm.
- [x] Add `TestPolicyEvaluationErrorFailsClosed` asserting a rule referencing an undefined field yields `Allow: false` and a `Reason` containing "policy error".
- [x] Add `TestUnsignedPluginDeniedInProductionTierAllowedInDev` asserting the same input differs by tier only.
- [x] Implement `internal/policy/cel.go` using `github.com/google/cel-go` with an env declaring every `Input` field, compiling and caching programs keyed by `(tier, policy revision)`, and `internal/policy/audit.go` writing decisions to `policy_audit`.
- [x] Wire the engine into definition save, via `policy.NewSaveGuard` — a decorator over `defstore.Store`, so `internal/defstore` needed no change. The scheduler half moved to Task 14, which is where the dispatch-time decision lives; it is listed there rather than left as an unticked box here.
- [x] Run `go test ./internal/policy` — expect PASS. Commit.

## Task 22: Tenancy enforcement across store and bus

Files: `internal/tenant/tenant.go`, `internal/tenant/nats_accounts.go`, `internal/tenant/tenant_test.go`
Interfaces: produces `tenant.FromContext(ctx) (string, error)`, `tenant.WithTenant(ctx, id string) context.Context`, `tenant.AccountName(tenantID string) string`, `tenant.ProvisionAccount(ctx, srv *bus.Embedded, tenantID string) (creds string, err error)`.

- [x] Write `internal/tenant/tenant_test.go` asserting `TestTenantIsolationAcrossStoreAndBus`: tenant `b` cannot replay tenant `a`'s run, cannot read `a`'s CAS blob, and cannot subscribe to `a`'s dispatch subject. Run — expect FAIL with "undefined: tenant.ProvisionAccount".
- [x] Add `TestNoStoreMethodAcceptsEmptyTenant` iterating every exported `runstore.Store`, `cas.Store` and `blobstore.Store` method by reflection and requiring each returns an error containing "tenant scope required" when passed `""`.
- [x] Add `TestSubjectBuildersIncludeTenant` asserting every builder in `internal/bus/subjects.go` produces a subject containing the tenant segment.
- [x] Implement `internal/tenant/nats_accounts.go` provisioning one NATS account per tenant with subject permissions limited to that tenant's prefixes, and `internal/tenant/tenant.go` for context propagation.
- [x] Run `go test ./internal/tenant ./internal/bus ./internal/runstore` — expect PASS. Commit.

## Task 23: Built-in identity and service tokens

Files: `internal/identity/identity.go`, `internal/identity/local.go`, `internal/identity/token.go`, `internal/identity/local_test.go`, `internal/runstore/migrations/0004_identity.sql`
Interfaces: produces `identity.Provider` with `Authenticate(ctx, credential string) (identity.Principal, error)`; `Principal{Subject, TenantID string; Scopes []string; Kind PrincipalUser|PrincipalService}`; `identity.NewLocal(store)`, `identity.IssueToken(ctx, p Principal, ttl time.Duration) (string, error)`.

- [x] Write `internal/identity/local_test.go` asserting `TestServiceTokenAuthenticatesWithScopes`: issue a token with scope `pipelines:write`, authenticate it, and require the returned `Principal` carries that scope and tenant. Run — expect FAIL with "undefined: identity.NewLocal".
- [x] Add `TestExpiredTokenIsRejected` asserting a token issued with a -1s TTL returns an error satisfying `errors.Is(err, identity.ErrExpired)`.
- [x] Add `TestPasswordsAreStoredAsArgon2idNotPlaintext` asserting the stored credential does not contain the password and begins with `$argon2id$`.
- [x] Write `0004_identity.sql` creating `principals(tenant_id, subject, kind, credential_hash)` and `tokens(tenant_id, subject, token_hash, scopes, expires_at)`; store only token hashes.
- [x] Implement `internal/identity/local.go` with `golang.org/x/crypto/argon2` and `internal/identity/token.go` issuing 32-byte random tokens.
- [x] Run `go test ./internal/identity` — expect PASS. Commit.

## Task 24: OIDC federation

Files: `internal/identity/oidc.go`, `internal/identity/oidc_test.go`, `internal/identity/chain.go`
Interfaces: produces `identity.NewOIDC(cfg OIDCConfig) (identity.Provider, error)`, `identity.Chain(providers ...identity.Provider) identity.Provider`.

- [x] Write `internal/identity/oidc_test.go` asserting `TestOIDCAndServiceTokenWithIdPDown`: a chain of OIDC and local providers authenticates an OIDC id token while the mock IdP is up, and still authenticates a local service token after the IdP is stopped. Run — expect FAIL with "undefined: identity.NewOIDC".
- [x] Add `TestOIDCTokenWithWrongAudienceIsRejected` asserting an id token whose `aud` does not match configuration returns an error containing "audience".
- [x] Add `TestOIDCFailureDoesNotFallBackToWeakerAuth` asserting that when the IdP is unreachable, an OIDC credential returns an error rather than being accepted by the local provider.
- [x] Implement `internal/identity/oidc.go` with `github.com/coreos/go-oidc/v3` verifying issuer, audience, expiry and signature against cached JWKS, and `internal/identity/chain.go` selecting a provider by credential shape.
- [x] Run `go test ./internal/identity` — expect PASS. Commit.

## Task 25: Definition store, revisions and approval state

Files: `internal/defstore/defstore.go`, `internal/defstore/revision.go`, `internal/defstore/defstore_test.go`, `internal/runstore/migrations/0005_definitions.sql`
Interfaces: produces `defstore.Store` with `Save(ctx, tenantID string, p *dholev1.Pipeline, author string) (Revision, error)`, `Get(ctx, tenantID, pipelineID, revisionID string) (*dholev1.Pipeline, error)`, `Active(ctx, tenantID, pipelineID string) (Revision, error)`, `Approve(ctx, tenantID, revisionID, approver string) error`; `Revision{ID, PipelineID string; ContentHash string; State draft|reviewed|active; Lockfile map[string]string}`.

- [x] Write `internal/defstore/defstore_test.go` asserting `TestRevisionCreatedAndMirroredOnEdit`: saving a pipeline returns a revision whose `ContentHash` is stable across identical saves and changes when any field changes. Run — expect FAIL with "undefined: defstore.New".
- [x] Add `TestNewRevisionStartsAsDraftAndOnlyBecomesActiveOnApproval` asserting `Active` returns the previous revision until `Approve` is called, and that the approver is persisted.
- [x] Add `TestRunPinsRevisionAndIsUnaffectedBySubsequentSaves` asserting a run started against revision 1 still executes revision 1's definition after revision 2 is approved.
- [x] Add `TestApprovalRevokedMidRunDoesNotAlterRunningRun` asserting a run in flight completes on its pinned revision.
- [x] Write `0005_definitions.sql` creating `pipelines`, `revisions(tenant_id, id, pipeline_id, content_hash, state, lockfile, author, approver, created_at)`.
- [x] Implement `internal/defstore/revision.go` computing the content hash over canonical protobuf bytes and `defstore.go` over the run store's connection.
- [x] Run `go test ./internal/defstore` — expect PASS. Commit.

## Task 26: One-way git mirror

Files: `internal/mirror/git.go`, `internal/mirror/git_test.go`, `internal/mirror/yaml.go`
Interfaces: produces `mirror.Git` with `Push(ctx, tenantID string, rev defstore.Revision, p *dholev1.Pipeline) error`, `Status(ctx, tenantID string) (mirror.State, error)`; `mirror.ToYAML(p *dholev1.Pipeline) ([]byte, error)`, `mirror.FromYAML([]byte) (*dholev1.Pipeline, error)`.

- [x] Write `internal/mirror/git_test.go` asserting `TestGitMirrorEditsAreNotAuthoritative`: push a revision to a bare repo, commit an unrelated change directly in a clone, then push a new revision and require the authoritative YAML overwrites the manual edit and `defstore.Active` is unchanged by it. Run — expect FAIL with "undefined: mirror.NewGit".
- [x] Add `TestYAMLRoundTripsLosslessly` asserting `FromYAML(ToYAML(p))` equals `p` by `proto.Equal` for a pipeline exercising every field, so backend export and import stay lossless.
- [x] Add `TestYAMLContainsNoBackendSpecificFields` asserting the emitted YAML has no `layout`, `approver` or `state` key.
- [x] Add `TestMirrorPushFailureDoesNotBlockSave` asserting that with an unreachable remote, `defstore.Save` still succeeds and `mirror.Status` reports drift.
- [x] Implement `internal/mirror/yaml.go` with `protojson` to YAML via `sigs.k8s.io/yaml`, and `internal/mirror/git.go` with `go-git`, committing one file per pipeline under `pipelines/<id>.yaml` and retrying with backoff.
- [x] Run `go test ./internal/mirror` — expect PASS. Commit.

## Task 27: ConnectRPC API and operation-level editing

Files: `proto/dhole/v1/api.proto`, `internal/api/server.go`, `internal/api/operations.go`, `internal/api/operations_test.go`, `internal/api/auth.go`
Interfaces: produces service `PipelineService` with RPCs `GetPipeline`, `ApplyOperation`, `Validate`, `Plan`, `ListRevisions`, `ApproveRevision`, `StartRun`, `WatchRun`; `api.Operation` oneof `AddStep`, `Connect`, `SetProperty`, `RemoveEdge`, `Rename`; `ApplyOperationResponse{Revision revision; Diff diff; Operation inverse}`.

- [x] Write `internal/api/operations_test.go` asserting `TestOperationInverseRestoresRevision`: apply `AddStep`, capture the returned `inverse`, apply it, and require the resulting pipeline equals the original by `proto.Equal`. Run — expect FAIL with "undefined: api.NewServer".
- [x] Add `TestApplyOperationRejectsStaleVersion` asserting applying against a superseded `base_revision` returns `CodeAborted` with a message containing "revision conflict".
- [x] Add `TestEveryOperationReturnsANonEmptyDiff` iterating all five operation kinds and requiring each response carries a diff naming the changed step or edge.
- [x] Write `proto/dhole/v1/api.proto` with the service and messages; run `buf generate`.
- [x] Implement `internal/api/operations.go` applying each operation to a copy of the pipeline and computing the inverse, and `internal/api/server.go` serving it over ConnectRPC with `internal/api/auth.go` resolving a `Principal` from the `Authorization` header and rejecting unscoped calls.
- [x] Run `go test ./internal/api` — expect PASS. Commit.

## Task 27b: What the API surfaced

Files: `internal/defstore/`, `internal/api/`
Interfaces: adds a revision-history query to `defstore.Store`; gives the editing head a home in the schema.

- [x] **`defstore.Store` cannot list a pipeline's revisions.** `ListRevisions` is served through an optional `api.RevisionLister` and answers `CodeUnimplemented` when the store cannot list, because an empty list would be a lie about a pipeline with a long history. Add the query to the store.
- [x] **`policy.Input` carries no taint keys.** Task 51's `taint.Check` returns a `policy.Decision` but builds it itself, because `internal/policy` was a concurrent task's file. Add the taint fields to `policy.Input` and the CEL environment so an operator can write a taint rule instead of relying on the four built-in ones. Found building Task 51.
- [x] **The wire contract does not mention the registration re-announce.** Task 18b made an engine re-announce every three heartbeats so a registration lost while the plane was down is recoverable, but the contract still says only "heartbeat every five seconds". A third-party engine written to the document alone is invisible after a plane restart. Document it.
- [x] **`dhole serve` does not serve the API.** `internal/server` assembles the bus, scheduler, outbox
      and an engine, and never calls `api.Server`'s handler — nothing in `cmd/` or `internal/server` imports
      `internal/api` at all. So the binary runs a control plane with no contract on it: the CLI has nothing to
      talk to, and Task 46 had to write its own fixture binary to test the canvas at all. Serve the API from
      the server, and delete `web/e2e/fixture/main.go`, which says in its own header that it exists only until
      this is fixed. Found building Task 46.
- [x] **`PipelineService` has no catalog RPC.** The browser cannot read a plugin's
      `catalog.Manifest.InputSchema`, so Task 47's panel renders the declaration the DEFINITION carries — the
      step's structured input port's inline schema — instead. One function, `declarationOf()`, is what changes
      when a catalog RPC lands. Found building Task 47.
- [x] **A step has nowhere to store its plugin's input values.** `SetProperty` accepts only `plugin_ref`,
      `effect_class` and `lease_scope`, so a field a plugin declares can be rendered and validated but not
      persisted; the panel shows those fields with that reason attached rather than hiding them. Add a step
      config field, or extend `SetProperty` over plugin inputs. Found building Task 47.
- [x] **No RPC creates a pipeline from scratch.** `ApplyOperation` requires an existing
      `base_revision`, so a brand-new pipeline cannot be created through the contract at all — both Task 46 and
      Task 48 had to seed one through a fixture-only HTTP route. That is a hole in ADR 0013 exactly where it
      matters: the GUI cannot create a pipeline without a back door. Add `CreatePipeline`, or let
      `ApplyOperation` accept an empty base for a new id. Found independently by Tasks 46 and 48.
- [x] **No way to provision a credential.** `identity.Local.IssueToken` is unreachable from the CLI, so
      there is no path from a fresh binary to a usable token. A person needs a repeatable way to mint one for a
      tenant, not only whatever a plane prints at startup. Found building Task 48.
- [x] **No engine can run a step that carries a secret, and nothing redeems one.** `CAPABILITY_SECRETS` is sourced from `Executor.Capabilities()`, and no backend advertises it — the process executor honestly advertises nothing, and the Kubernetes one advertises NETWORK plus whatever its pod template grants. So `engine.checkSecrets` refuses every dispatch carrying a secret, always. The sourcing is the first bug: redeeming a short-lived reference is the AGENT's job, not a sandbox-isolation guarantee like PRIVILEGED or HOST_MOUNT, so asking the executor is asking the wrong component. The second is that there is no redemption code at all — `checkSecrets` only refuses, and a `SecretRef` never becomes a value. The spec requires this ("engines never receive secret values, only short-lived references they redeem"), the wire carries `SecretRef`, and the conformance suite has a `secret-redemption` case that the reference Python engine passes and the Go engine cannot reach. Found running the Go engine through conformance for the first time, 2026-09-10.
      CLOSED 2026-09-10. `CAPABILITY_SECRETS` now comes from `engine.Config.Secrets` — a
      `secrets.Redeemer` the agent holds — and `checkSecrets` asks the same thing; no executor was
      taught to advertise it. `internal/secrets` carries the broker, the bus redeemer and the
      responder; the plane serves it on `bus.SubjectSecretRedeem()` and the embedded engine redeems
      through it. The exchange is now specified in docs/wire-contract.md ("Secrets"), which no
      longer lists redemption as a gap. The conformance case exercises the whole path and its four
      leak checks, and PASSES once the two gaps below are neutralised; on the tree as it stands it
      still fails on the port layout. (Both are closed below as of 2026-09-10, and the case now
      passes outright: `make conformance` is 11/11 for the shipped Go engine and for the reference
      Python one.)
- [x] **The port layout on disk is two conventions and neither is written down.** The engine puts an
      input at the sandbox path `<port>` and reads an output from `<port>`; the conformance suite
      (and the reference Python engine) use `inputs/<port>` and `outputs/<port>`. So
      `binary-artifact-round-trip` and `secret-redemption` fail with a step that could not write
      `outputs/proof`, and the wire contract lists the layout as unspecified. Somebody has to decide
      it, write it into docs/wire-contract.md, and make the engine and the executor agree — the
      sandbox has to CREATE the outputs directory, which no backend does today. Found closing the
      secret-redemption item, 2026-09-10.
      CLOSED 2026-09-10. The contract is `inputs/<port>` and `outputs/<port>` — the suite's and
      the reference Python engine's layout, not the shipped engine's — because a step may declare
      an input and an output with the SAME port name, and flat at the sandbox root the engine
      materialised the input over the output's path and collected the untouched input back as the
      step's result: a step that did nothing passed. `docs/wire-contract.md` gains "Port layout on
      disk"; `executor.Sandbox` gains `Mkdir`, implemented by the process, Kubernetes, containerd
      and pool backends and asserted by `executortest.Contract`, and the agent creates both
      directories before the command runs. `TestAnInputAndAnOutputSharingAPortNameDoNotCollide`
      pins the reason. Every checked-in pipeline that wrote a port at the root moved with it.
- [x] **The object store protocol is still unspecified, and tenancy makes it worse.** Every Dhole
      store scopes structurally, so an engine's `log_key` resolves under `<tenant>/<key>` and its CAS
      objects under `<algo>/<xx>/<hex>`; the suite assumed a flat directory and the reference Python
      engine writes one. The suite now tries the tenant-scoped and Dhole CAS shapes before the flat
      ones, which unblocked four cases, but that is a harness accommodating two engines rather than a
      contract. Found closing the secret-redemption item, 2026-09-10.
      CLOSED 2026-09-10 for the SHAPE of a key, which is what tenancy made urgent: an object named
      by key k for tenant t resolves at `<tenant>/<key>`, and a content-addressed object at
      `<tenant>/<algo>/<first two hex>/<hex>` — the shipped stores' layout, written into
      `docs/wire-contract.md` as "The object store". The suite no longer tries a flat path first
      (an engine writing flat keys passed it and would have served one tenant's log to another),
      and the reference Python engine now scopes what it writes. The store's PROTOCOL — how an
      engine reaches it, with what credentials — is still unspecified and stays on the gap list.
- [x] **Nothing carries a step timeout, so `step-timeout` cannot pass.** Neither `JobDispatch` nor
      `Step` has a timeout field; the conformance suite passes `DHOLE_STEP_TIMEOUT_SECONDS` in
      `JobDispatch.env` and says so as a gap. The Go engine enforces nothing and holds the slot for
      the step's full runtime. This wants a schema field, additively, rather than the engine adopting
      the harness's environment convention. Found closing the secret-redemption item, 2026-09-10.
      CLOSED 2026-09-10. `Step.timeout_seconds` (field 12, additive) carries it, and
      `wire.ProtocolVersion` is 3: a bump rather than a silent addition because the field changes
      what an engine must DO and a plane cannot observe enforcement. The agent bounds `Exec` with
      it and reports PHASE_FAILED with exit code 137 — a killed step, not a cancellation, which
      would not be retried — after the authoritative log is flushed.
      `TestAStepThatOutlivesItsTimeoutIsKilledAndReportedFailedWith137` pins it, and the
      conformance case sets the field instead of `DHOLE_STEP_TIMEOUT_SECONDS`. Both engines pass
      11/11.
- [x] **The object store has no protocol, only a key shape.** Closing the tenancy half said where
      an object RESOLVES — `<tenant>/<key>`, and `<tenant>/<algo>/<xx>/<hex>` for a
      content-addressed one — and left the rest of the store undescribed: how an engine reaches it,
      what it is addressed by, what credentials it presents. The conformance suite hands an engine
      a directory in `DHOLE_BLOB_DIR`, which is a harness convention no real deployment can use,
      and the shipped engine reads `DHOLE_OBJECT_STORE` plus a bucket's worth of variables that
      appear in no contract. A third-party engine still cannot be pointed at a deployment's store
      from `docs/wire-contract.md` alone. Found closing the key-shape half, 2026-09-10. DONE: the
      variables the tree already reads ARE the contract, and the fix was to stop having two
      documents disagree about them rather than to invent a third spelling — a rename would have
      broken every chart, every compose file and both binaries to buy nothing. `docs/wire-contract.md`
      gains "Reaching the object store": `DHOLE_OBJECT_STORE` selects the backend, each backend's
      variables configure it, an engine that does not implement the named backend REFUSES to start
      rather than falling back, credentials come from the process environment and never from a
      `SecretRef`, an empty `DHOLE_S3_ACCESS_KEY_ID` means the ambient chain, the store already
      exists and an engine never creates or deletes it, and a write is durable before the status
      that names it. `docs/writing-an-engine.md` no longer carries its own copy of the table —
      `DHOLE_S3_SESSION_TOKEN` was read by the code and documented nowhere, which is exactly the
      drift. The conformance suite now sets `DHOLE_OBJECT_STORE=filesystem` beside `DHOLE_BLOB_DIR`,
      so an engine that reasonably REQUIRES the variable can be tested; the reference Python engine
      refuses a backend it cannot speak and refuses a filesystem store with no directory instead of
      defaulting under `/tmp`. Guarded by `TestEveryObjectStoreVariableTheCodeReadsIsInTheWireContract`,
      which derives the list from `internal/blobstore/fromenv.go` rather than keeping one beside it,
      and by `TestTheEngineGuideDoesNotRestateTheObjectStoreContract`. Both engines still pass 11/11.
- [x] **Nothing publishes to the catalog.** `catalog.Publish` has no caller outside tests — not the API, not the CLI, not the git mirror. So a plugin's declaration can be read through `GetPlugin` and there is no supported way to put one there; the e2e seeder has a `/plugin` route for exactly this reason. Found building Task 27b. DONE: `PublishPlugin` is an additive RPC on `PipelineService` — the same place `GetPlugin` reads from, because a publish only a process holding the plane's database could perform is a capability the GUI and an agent can never have (ADR 0013). `dhole plugin publish <manifest|@file|@->` is its CLI surface, which `TestCLICoversEveryRPC` obliges. The seeder's `/plugin` back door is GONE and `panel.spec.ts` publishes through the contract.
- [x] **The quota enforcer and the CAS guard are built and unwired.** Task 58's `Enforcer.AdmitRun`/`AdmitStep` and `GuardCAS` are tested but have no call sites: `internal/scheduler` and `internal/cas` belonged to other agents that round. Wire `AdmitStep` into the dispatch loop and `GuardCAS` around the blob store. Note `tenancy` deliberately does not import `scheduler` — the dependency runs the other way — so it mirrors two persistence contracts, guarded by `TestMirroredSchedulerContractsHaveNotDrifted`.
- [x] **The fair queue and budgets are built and unwired.** Task 42 delivered `scheduler.Queue` and
      `scheduler.Budgets` fully tested, but `scheduler.go` was being edited concurrently so nothing calls them:
      ready steps are still dispatched inline, and no per-pipeline cap is enforced. Wire them — enqueue ready
      steps, drain with `Next(ctx, slots)` against the fleet's free capacity, `Acquire` before the lease claim,
      and release on EVERY terminal status, not only success. This is the same shape as the cache gap
      (Task 15b): everything built, one end unconnected.
- [x] **`Enforcer.AdmitRun` still has no caller.** Wiring the quotas closed `AdmitStep` — the scheduler
      asks it before every dispatch, against the in-flight count the budgets bucket holds — and left the daily
      run limit unenforced: nothing admits a RUN. It belongs where a run is created, which is `internal/api`
      and the triggers, and both belonged to other agents that round. Until it is wired, `max_runs_per_day` is
      a column nobody reads and the usage ledger has no `RUN_STARTED` rows to bill from. Found wiring Task 42
      and Task 58. DONE: both run-creating paths admit before anything is written — `api.StartRun`
      through a narrow `api.Quotas` (declared where the question is asked, exactly as
      `scheduler.Quotas` is, so `tenancy` still imports neither), and `Server.Submit`, which is what
      every trigger fires into. A refusal is `RESOURCE_EXHAUSTED` carrying the enforcer's own reason,
      which names `max_runs_per_day`, its limit and the usage; it leaves no run id, no `RUN_CREATED`
      and no revision behind. `AdmitRun` already recorded `KindRunStarted`, so the ledger now has the
      rows to bill from.
- [x] **A tenant's CAS usage only ever grows.** `MaxCASBytes` is measured by summing `KindCASBytes` usage
      records, and `cas.GC` deletes blobs without writing anything that offsets them, so a tenant that
      reclaims a terabyte is still charged for it and is eventually refused every write with a store that is
      nearly empty. It needs a compensating usage record on collection — which is a metering decision and a
      new usage kind, not a wiring one. Found wiring Task 58: `GuardCAS` is now in front of every deployment's
      blobs, so the drift is live rather than theoretical.
- [x] **The contract has no `CancelRun` and no `EngineService`.** Task 29's CLI therefore ships `run cancel`, `engine list` and `engine drain` as commands that exist and refuse, rather than reaching into `internal/registry` or the run store behind the API's back — a CLI able to do what the GUI cannot is the same ADR 0013 failure seen from the other side. Declare the RPCs and implement them; the CLI commands are already there waiting. Found building Task 29.
- [x] **`registry.Instance` drops the engine types an engine advertises.** `EngineRegistration` carries `engine_types`, and the registry does not keep them, so `api.Plan` cannot say which engine kind would run a step from the matched instance and reports the locally configured environment's kind instead. Carry `engine_types` on the instance and have Plan read it from the match. Found building Task 28.
- [x] **The editing head has nowhere to live.** Revisions are content-addressed and carry no parent, so `api.Heads` is an in-process interface whose only implementation is in memory. That is a real optimistic-concurrency check within one control plane and NOT one across several: two planes will each accept an edit against the same base. Store the head before any horizontal scale-out (Task 43).

## Task 28: Validate and plan endpoints

Files: `internal/api/validate.go`, `internal/api/plan.go`, `internal/api/plan_test.go`
Interfaces: produces `Validate(ctx, *ValidateRequest) (*ValidateResponse{repeated Diagnostic diagnostics})`, `Plan(ctx, *PlanRequest) (*PlanResponse{repeated PlannedStep steps})`; `PlannedStep{string step_id; bool cache_hit; string engine_kind; string non_cacheable_reason}`.

- [x] Write `internal/api/plan_test.go` asserting `TestPlanReportsCacheHitsAndEngineAssignment`: plan a pipeline whose first step is already cached and require `steps[0].cache_hit == true` and every step carries a non-empty `engine_kind`. Run — expect FAIL with "undefined: api.Plan".
- [x] Add `TestPlanReportsNonCacheableReasonForPoolLease` asserting a `LeasePool` step returns the exact reason string from Task 16.
- [x] Add `TestValidateSurfacesTypeErrorsWithPositions` asserting the Task 3 type error is returned with the step id and port name populated.
- [x] Add `TestPlanDoesNotDispatchAnything` asserting no message is published to any `job.dispatch.*` subject during a `Plan` call.
- [x] Implement `internal/api/validate.go` delegating to `dag.TypeCheck` plus plugin schema validation, and `internal/api/plan.go` resolving the DAG, computing cache keys, consulting `cache.Lookup` and `scheduler.Match` without side effects.
- [x] Run `go test ./internal/api` — expect PASS. Commit.

## Task 29: CLI at full API parity

Files: `cmd/dhole/cli/`, `cmd/dhole/cli/gen.go`, `cmd/dhole/cli/coverage_test.go`, `cmd/dhole/cli/run.go`, `cmd/dhole/cli/pipeline.go`
Interfaces: produces commands `dhole pipeline get|apply|validate|plan|revisions|approve`, `dhole run start|watch|logs|cancel`, `dhole engine list|drain`, `dhole policy test`, `dhole local run`.

- [x] Write `cmd/dhole/cli/coverage_test.go` asserting `TestCLICoversEveryRPC`: reflect over the `PipelineService` and `RunService` protobuf descriptors and require every RPC name maps to a registered cobra command, failing with the list of uncovered RPCs. Run — expect FAIL with "undefined: cli.Root".
- [x] Add `TestLocalRunExecutesWithoutServer` asserting `dhole local run testdata/pipelines/two-step.yaml` completes using an in-process embedded server and prints step outputs.
- [x] Add `TestCLIOutputIsJSONWhenRequested` asserting `--output json` emits parseable JSON for `plan`.
- [x] Implement `cmd/dhole/cli/gen.go` generating a command stub per RPC from the descriptors at build time, and the hand-written commands for the friendlier surfaces.
- [x] Run `go test ./cmd/dhole/...` — expect PASS. Commit.

## Task 30: Catalog of step, plugin, engine and trigger types

Files: `internal/catalog/catalog.go`, `internal/catalog/manifest.go`, `internal/catalog/catalog_test.go`, `internal/runstore/migrations/0006_catalog.sql`
Interfaces: produces `catalog.Store` with `Publish(ctx, tenantID string, m catalog.Manifest) error`, `Resolve(ctx, tenantID, ref string) (catalog.Entry, error)`, `List(ctx, tenantID string) ([]catalog.Entry, error)`; `Manifest{Namespace, Name, Version string; Digest *dholev1.Digest; Kind step|trigger|engine; EffectClass dholev1.EffectClass; Capabilities []dholev1.Capability; InputSchema, OutputSchema []byte; EngineTypes []string}`.

- [x] Write `internal/catalog/catalog_test.go` asserting `TestManifestDeclaresEffectClassAndCapabilities`: publishing a manifest and resolving it returns the declared effect class and capability set unchanged. Run — expect FAIL with "undefined: catalog.New".
- [x] Add `TestStepInheritsEffectClassFromManifest` asserting a step with no explicit effect class resolves to its plugin's, and that an explicit widening override is flagged in the returned `Entry.OverrideWarnings`.
- [x] Add `TestCatalogSurvivesControlPlaneRestart` asserting entries persist across a store reopen, unlike the runtime registry.
- [x] Add `TestManifestWithInvalidJSONSchemaIsRejected` asserting `Publish` returns an error naming the offending schema.
- [x] Write `0006_catalog.sql` creating `catalog_entries(tenant_id, namespace, name, version, digest, kind, effect_class, capabilities, input_schema, output_schema, engine_types)`.
- [x] Implement `internal/catalog/manifest.go` validating schemas with `santhosh-tekuri/jsonschema/v6` and `catalog.go`.
- [x] Run `go test ./internal/catalog` — expect PASS. Commit.

## Task 31: Runtime engine registry and lifecycle

Files: `internal/registry/registry.go`, `internal/registry/kv.go`, `internal/registry/registry_test.go`
Interfaces: produces `registry.Registry` with `Register(ctx, r *dholev1.EngineRegistration) error`, `Heartbeat(ctx, h *dholev1.EngineHeartbeat) error`, `Instances(ctx, tenantID string) ([]registry.Instance, error)`, `Drain(ctx, engineID string) error`; `Instance{ID string; State registering|ready|draining|gone; Capabilities []dholev1.Capability; OS, Arch string; Slots int; ProtocolVersions []uint32}`.

- [x] Write `internal/registry/registry_test.go` asserting `TestInstanceAgesOutWithoutHeartbeat`: register with a 200ms TTL, stop heartbeating, and require `Instances` omits it after the TTL. Run — expect FAIL with "undefined: registry.New".
- [x] Add `TestDrainStopsNewWorkButFinishesInFlight` asserting a drained instance receives no new dispatch while its in-flight step still completes.
- [x] Add `TestRegistrationWithUnsupportedProtocolIsRefused` asserting a registration advertising only version 0 is rejected with an error containing "unsupported protocol".
- [x] Add `TestSchedulerRoutesOnlyToInstancesSatisfyingResolvedRequirements` asserting a fleet on mixed protocol versions receives work only where the version and capabilities match.
- [x] Implement `internal/registry/kv.go` over NATS KV bucket `dhole-engines` with TTL, and `registry.go` mapping lifecycle transitions.
- [x] Run `go test ./internal/registry ./internal/scheduler` — expect PASS. Commit.

## Task 32: Plugin resolver for oci:// and cas://

Files: `internal/plugins/resolver.go`, `internal/plugins/oci.go`, `internal/plugins/casref.go`, `internal/plugins/resolver_test.go`
Interfaces: produces `plugins.Resolver` with `Resolve(ctx, tenantID, ref string) (plugins.Artifact, error)`, `Fetch(ctx, tenantID string, a plugins.Artifact) (io.ReadCloser, error)`; `Artifact{Ref string; Digest *dholev1.Digest; Scheme oci|cas; MediaType string}`.

- [x] Write `internal/plugins/resolver_test.go` asserting `TestResolverHandlesBothSchemesUniformly`: an `oci://` reference against a local registry and a `cas://` reference against the filesystem CAS both resolve to an `Artifact` with a populated digest and both `Fetch` successfully. Run — expect FAIL with "undefined: plugins.NewResolver".
- [x] Add `TestTagIsResolvedToDigestAtSaveAndNeverAtDispatch` asserting `Resolve` on `oci://reg/img:v1` returns a digest, and that dispatch with a tag-only reference returns an error containing "unresolved tag".
- [x] Add `TestMovedTagDoesNotChangeExistingRevision` asserting that after retagging the registry image, an existing lockfile still fetches the original digest.
- [x] Implement `internal/plugins/oci.go` with `google/go-containerregistry` (digest-pinned pulls, no tag resolution at fetch), `casref.go` delegating to `cas.Store`, and `resolver.go` dispatching on scheme.
- [x] Add `zot` to `docker-compose.test.yml` on port 55000 as the test registry.
- [x] Run `make test-integration` — expect PASS. Commit.

## Task 33: Detached signature records and verification

Files: `internal/plugins/signature.go`, `internal/plugins/cosign.go`, `internal/plugins/signature_test.go`, `internal/runstore/migrations/0007_signatures.sql`
Interfaces: produces `plugins.Signatures` with `Record(ctx, tenantID string, d *dholev1.Digest, sig plugins.Signature) error`, `Verify(ctx, tenantID string, d *dholev1.Digest, allowed []string) error`; `Signature{Identity, Issuer string; Payload []byte; Source cosign|manual}`.

- [x] Write `internal/plugins/signature_test.go` asserting `TestVerificationIsUniformAcrossSchemes`: the same signature record verifies for an `oci://` and a `cas://` artifact with identical code paths. Run — expect FAIL with "undefined: plugins.NewSignatures".
- [x] Add `TestUnsignedPluginIsNotDispatchedRegardlessOfCachedResolution` asserting that after a successful resolve, removing the signature record causes dispatch to fail with "signature verification failed" and marks the catalog entry untrusted.
- [x] Add `TestSignatureFromDisallowedIdentityIsRejected` asserting verification against an `allowed` list not containing the signer's identity fails.
- [x] Write `0007_signatures.sql` creating `artifact_signatures(tenant_id, digest, identity, issuer, payload, source)`.
- [x] Implement `internal/plugins/cosign.go` populating records from cosign attestations and `signature.go` verifying against the tier's allowed identities from policy.
- [x] Run `go test ./internal/plugins` — expect PASS. Commit.

## Task 34: Federated upstreams and local mirroring

Files: `internal/plugins/upstream.go`, `internal/plugins/mirror.go`, `internal/plugins/upstream_test.go`
Interfaces: produces `plugins.Upstreams` with `Add(ctx, tenantID string, u Upstream) error`, `Sync(ctx, tenantID, namespace string) (int, error)`; `Upstream{Namespace, URL string; AllowedIdentities []string; MirrorPolicy always|on_demand}`.

- [x] Write `internal/plugins/upstream_test.go` asserting `TestNamespacingPreventsCollisionBetweenUpstreams`: two upstreams both publishing `docker-build` resolve independently as `a/docker-build` and `b/docker-build`. Run — expect FAIL with "undefined: plugins.NewUpstreams".
- [x] Add `TestSyncMirrorsArtifactsLocallyAndSurvivesUpstreamOutage` asserting that after `Sync`, stopping the upstream registry still allows resolve and fetch from the local mirror.
- [x] Add `TestUnmirroredPluginBlocksDispatchWithClearDiagnostic` asserting dispatch of an unmirrored plugin from an unreachable upstream fails with an error naming the plugin and the upstream.
- [x] Add `TestPerUpstreamAllowedIdentitiesAreEnforced` asserting a plugin signed by an identity allowed on upstream `a` is rejected when pulled from upstream `b`.
- [x] Implement `internal/plugins/upstream.go` and `mirror.go` copying artifacts into the local CAS or registry mirror and recording signatures at sync time.
- [x] Run `make test-integration` — expect PASS. Commit.

## Task 35: Lockfile resolution at definition save

Files: `internal/defstore/lockfile.go`, `internal/defstore/lockfile_test.go`
Interfaces: produces `defstore.ResolveLockfile(ctx, tenantID string, p *dholev1.Pipeline, r plugins.Resolver) (map[string]string, error)` returning plugin ref to digest.

- [x] Write `internal/defstore/lockfile_test.go` asserting `TestLockfilePinsPluginDigestsAgainstMovedTag`: save a pipeline referencing `oci://reg/img:v1`, retag the registry to different content, and require a run of the saved revision fetches the original digest. Run — expect FAIL with "undefined: defstore.ResolveLockfile".
- [x] Add `TestSaveFailsWhenAPluginCannotBeResolved` asserting `Save` returns an error naming the unresolvable ref rather than persisting a partial lockfile.
- [x] Add `TestLockfileChangeProducesVisibleDiff` asserting a re-save after an intentional plugin upgrade yields a diff listing the old and new digests.
- [x] Add `TestCacheKeyChangesWhenLockfileChanges` asserting the Task 15 key differs between two revisions differing only in lockfile.
- [x] Implement `internal/defstore/lockfile.go` resolving every plugin ref during `Save` and storing the map on the revision.
- [x] Run `go test ./internal/defstore ./internal/cache` — expect PASS. Commit.

## Task 36: containerd/OCI executor

**Unblocked by running it on a node.** This machine still has no containerd,
but k3s's own is reachable from a privileged pod with the socket, the FIFO
directory and the host CA bundle mounted in, and that is where the whole
shared contract was run: nine subtests, all passing in 10s against
containerd 2.1.5 on an arm64 k3s node, plus the four tests below. Refusing to
write it against a fake was right — every defect found here was invisible to
one: a custom registry-hosts config silently drops containerd's authorizer and
every pull comes back 401; a k3s node advertises a stargz snapshotter with no
init error that cannot create a container; `oci.WithImageConfig` temp-mounts
the rootfs on the CLIENT, so it fails for any executor not sharing the
daemon's filesystem; containerd holds stdin open until `CloseIO`, so `cat >
file` never sees EOF; and containerd cannot tear down an exec whose stdio a
step's abandoned grandchild still holds, so a cancellation that sweeps the
process tree only after reading the exit status never sweeps at all and Exec
returns fifteen seconds late.

Files: `internal/executor/containerd/containerd.go`, `internal/executor/containerd/identity.go`, `internal/executor/containerd/containerd_test.go`
Interfaces: produces `containerd.New(cfg containerd.Config) (executor.Executor, error)` satisfying Task 9's interface; `Executor.EnvironmentIdentity() (string, error)` returning the image digest.

- [x] Write `internal/executor/containerd/containerd_test.go` calling Task 9's `executorContract` as `TestContainerdExecutorContract` against a containerd socket from `DHOLE_TEST_CONTAINERD_SOCK`, skipping when unset. Run — expect FAIL with "undefined: containerd.New".
- [x] Add `TestEnvironmentIdentityIsImageDigestNotTag` asserting `EnvironmentIdentity` returns the resolved digest and changes when the image content changes under the same tag. It pushes two different images to one tag on a real registry running in the test process, so it needs no containerd and runs everywhere; mutating the digest to a tag, and to a constant digest, both fail it.
- [x] Add `TestPrivilegedIsRefusedUnlessCapabilityAdvertised` asserting a spec requesting `PRIVILEGED` against an executor not advertising it returns an error containing "capability not advertised". The socket it names does not exist, so the refusal is proved to come from the capability check and not from an unreachable daemon.
- [x] Add `TestRootlessByDefault` asserting the container's uid inside the sandbox is non-zero unless `PRIVILEGED` is granted, and `TestPrivilegedGrantsRootWhenItIsAdvertised` for the other half. Both run against the k3s node's containerd; dropping `oci.WithUIDGID` fails the first.
- [x] Implement `internal/executor/containerd/containerd.go` using `containerd/containerd/v2` client — pull by digest, create a rootless container, stream stdout/stderr, propagate `SIGTERM` then `SIGKILL` after a grace period — and `identity.go` resolving image digests. Signals sweep the container's process table rather than the task's cgroup, so a signalled sandbox survives to run the next step of a pipeline lease.
- [~] Add containerd to CI as a service and run `make test-integration` — expect PASS. Commit. **PARTIAL.** `.github/workflows/ci.yml` gained a `containerd executor` job on both architectures — it starts the daemon the ubuntu runners already ship (a service container cannot expose its socket and FIFO directory at the paths the shim opens them by), runs the package as root, and fails the job if the contract only skipped. `Makefile` passes `DHOLE_TEST_CONTAINERD_SOCK` through `test-integration`. What is NOT closed: that job has never executed — GitHub Actions cannot be run from here — and `make test-integration` as a whole was not run, only `go test ./internal/executor/containerd/...` against the k3s node's containerd. Tick this when a CI run is green.

## Task 37: Kubernetes executor

Files: `internal/executor/kubernetes/kubernetes.go`, `internal/executor/kubernetes/kubernetes_test.go`
Interfaces: produces `kubernetes.New(cfg kubernetes.Config) (executor.Executor, error)`; `Config{Kubeconfig, Namespace string; ServiceAccount string; PodTemplate *corev1.PodSpec}`.

- [x] Write `internal/executor/kubernetes/kubernetes_test.go` calling `executorContract` as `TestKubernetesExecutorContract` against a kind cluster from `DHOLE_TEST_KUBECONFIG`, skipping when unset. Run — expect FAIL with "undefined: kubernetes.New".
- [x] Add `TestPodPerStepLeaseIsDeletedOnRelease` asserting no pod remains after `Release` for a `LeaseStep` sandbox.
- [x] Add `TestPipelineLeaseReusesOnePodAcrossSteps` asserting two `Exec` calls under one `LeasePipeline` sandbox run in the same pod.
- [x] Add `TestPodEvictionSurfacesAsStepFailureNotSuccess` asserting a deleted pod mid-exec yields a non-zero exit and an error mentioning eviction.
- [x] Implement `internal/executor/kubernetes/kubernetes.go` with `client-go`, creating pods from the template, attaching via the exec subresource, and cleaning up on release with a finalizer-free delete.
- [x] Add a kind cluster to CI and run `make test-integration` — expect PASS. Commit.

## Task 37b: Environment identity belongs to the sandbox, not the executor

Files: `internal/executor/executor.go`, `internal/executor/executortest/contract.go`, `internal/executor/kubernetes/`, `internal/executor/process/`, `internal/cache/`, `internal/scheduler/`
Interfaces: moves `EnvironmentIdentity()` from `executor.Executor` to the sandbox, or returns it from `Acquire`.

Task 37 was the first real test of ADR 0006's claim that the executor
interface is not Docker-shaped, and the claim held: the Kubernetes backend
passes the identical `executortest.Contract` the process backend does, with
nothing weakened or excused. It found one genuine mismatch.

`EnvironmentIdentity()` is a method on the EXECUTOR, but the image is on the
`Spec`. For a container backend, identity is a property of _(executor, spec)_
— so one Kubernetes executor running several different images reports the
digest of its configured template, and a cache key would describe the
template rather than the step's actual image. Task 37 honours `Spec.Image`
for the pod and documents the limitation on the method rather than bending
the interface to hide it.

- [x] Move environment identity to where the image actually is: a `Sandbox.EnvironmentIdentity()`, or an identity returned from `Acquire`. Update `cache.Key`'s caller so the key describes the image a step really ran under. DONE: `executor.Sandbox` gained `EnvironmentIdentity()`, resolved per sandbox from the image its pod was actually created with; `Executor.EnvironmentIdentity()` stays and is now documented as the identity of a sandbox acquired with an EMPTY `Spec`, which is what registration means and what ADR 0021 needs before anything is dispatched. `cache.StepEnvironment(step, tierIdentity)` is what both cache-key callers — `Scheduler.cacheKey`/`dispatch` and `api.Plan`/`Validate` — now hash against: the image the step named when it named one, the tier's identity when it did not, and NOTHING when the step named a mutable tag, since the plane resolves no tags and a name whose meaning can change is not an identity.
- [x] Write the failing test first: one Kubernetes executor, two steps with different images, must produce different cache keys. It must fail on the tree as it stands. DONE: `TestTwoStepsOnDifferentImagesGetDifferentCacheKeys` against the live cluster. It failed to compile on the tree as it stood (`sb.EnvironmentIdentity undefined`), and reverting the sandbox to the executor-wide answer by hand makes it fail on the assertion itself: one key for two images.
- [x] The contract's 2s SIGTERM window is a local-process budget; a remote backend spends ~80ms per exec round trip and has far less headroom. Decide whether the contract should scale that per backend, or stay strict deliberately. DECIDED: stays strict, by measurement rather than argument — the Kubernetes executor passes the whole shared contract, this subtest included, against a live cluster in 18s. The window is a promise to the scheduler (a lease expires 30s after it is claimed), so a backend that cannot meet it has told us something the scheduler needs, not something to widen the number for. Recorded on `executortest.sigtermWindow`.

## Task 38: Lease scopes, warm pools and lazy image pull

Files: `internal/executor/pool/pool.go`, `internal/executor/pool/pool_test.go`, `internal/executor/containerd/lazypull.go`
Interfaces: produces `pool.Manager` with `Acquire(ctx, key string, mk func() (executor.Sandbox, error)) (executor.Sandbox, error)`, `Reap(ctx, idle time.Duration) (int, error)`.

- [x] Write `internal/executor/pool/pool_test.go` asserting `TestPoolReusesSandboxAcrossRuns`: two runs with the same pool key receive the same sandbox id, and a file written by the first is visible to the second. Run — expect FAIL with "undefined: pool.New".
- [x] Add `TestReapReleasesIdleSandboxes` asserting a sandbox idle beyond the threshold is released and the next acquire creates a new one.
- [x] Add `TestPooledSandboxIsReportedNonCacheable` asserting Task 16's `cache.Eligible` is consulted and returns false for every step run from the pool.
- [ ] Add `TestLazyPullFetchesFewerBytesThanFullImage` — STILL SKIPPED, and honestly. Task 36's `containerd.New` now exists to drive the pull, so the missing halves are a containerd whose stargz snapshotter WORKS and an eStargz fixture image big enough for the byte count to mean anything. Working is the operative word: the one real containerd this was run against (a k3s node) advertises a stargz snapshotter that cannot create a container, which is why Task 36's executor demotes it empirically instead of trusting the plugin list. `SelectPullMode` and its fallback warning ARE tested. A byte count against a mock registry would prove nothing, so none was written — the skip names exactly what is missing.
- [x] Implement `internal/executor/pool/pool.go` keyed on `(tenant, engine kind, spec hash)` with an idle reaper, and `lazypull.go` enabling stargz snapshotter when available and falling back to a full pull with a logged warning.
- [x] Run `make test-integration` — expect PASS. Commit.

## Task 39: Engine conformance suite

Files: `conformance/suite.go`, `conformance/cases.go`, `conformance/main.go`, `testdata/engines/minimal-python/engine.py`, `Makefile`
Interfaces: produces `conformance.Run(ctx, cfg conformance.Config) (conformance.Report, error)`; `make conformance ENGINE=<cmd>` runs the suite against any engine binary.

- [x] Write `conformance/cases.go` with one case per contract obligation: registration and version negotiation, dispatch and success, non-zero exit, cancellation within 2s, timeout enforcement, 10MB log throughput, binary artifact round-trip, secret reference redemption without the value appearing in logs, lease renewal during a long step, and refusal of a fenced-out attempt.
- [x] Write `testdata/engines/minimal-python/engine.py`: a NATS client that registers, pulls dispatches, runs the command with `subprocess`, streams logs, and publishes status — deliberately not Go.
- [x] Write `conformance/suite_test.go` asserting `TestConformanceMinimalPythonEngine` runs every case against the Python engine and requires `Report.Failed == 0`. Run — expect FAIL with "undefined: conformance.Run".
- [x] Add `TestConformanceDetectsAnEngineThatIgnoresCancellation` asserting a deliberately broken engine variant fails exactly the cancellation case, proving the suite can fail.
- [x] Implement `conformance/suite.go` and `main.go`, and add `make conformance` to the Makefile.
- [x] Run `make conformance ENGINE="python3 testdata/engines/minimal-python/engine.py"` — expect PASS. Commit.

## Task 40: Trigger interface and schedule trigger

Files: `internal/trigger/trigger.go`, `internal/trigger/schedule/schedule.go`, `internal/trigger/schedule/schedule_test.go`
Interfaces: produces `trigger.Trigger` with `Start(ctx, sink trigger.Sink) error`, `Kind() string`; `trigger.Sink` with `Fire(ctx, tenantID, pipelineID string, inputs map[string]*structpb.Value) error`; `trigger.Binding{PipelineID string; InputMapping map[string]string}`.

- [x] Write `internal/trigger/schedule/schedule_test.go` asserting `TestScheduleFiresAtCronBoundary`: a `* * * * * *` schedule fires at least twice within 3s with the bound inputs populated. Run — expect FAIL with "undefined: schedule.New".
- [x] Add `TestMissedScheduleWindowFiresOnceOnRecovery` asserting a schedule whose window elapsed entirely during downtime fires exactly once on restart, reusing Task 20's timer store.
- [x] Add `TestConcurrencyBudgetOfOneSkipsOverlappingFire` asserting a second fire while the previous run is active is recorded as skipped with a reason, not queued indefinitely.
- [x] Implement `internal/trigger/trigger.go` and `internal/trigger/schedule/schedule.go` using `robfig/cron/v3` with persistence through the durable timer store.
- [x] Run `go test ./internal/trigger/...` — expect PASS. Commit.

## Task 41: HTTP, git webhook and pipeline-completion triggers

Files: `internal/trigger/http/http.go`, `internal/trigger/git/git.go`, `internal/trigger/completion/completion.go`, `internal/trigger/http/http_test.go`, `internal/trigger/git/git_test.go`, `internal/trigger/completion/completion_test.go`
Interfaces: produces `http.New(cfg)`, `git.New(cfg)` (GitHub, Gitea and Forgejo payloads), `completion.New(cfg)` satisfying `trigger.Trigger`.

- [x] Write `internal/trigger/http/http_test.go` asserting `TestHTTPTriggerMapsBodyToTypedInputs`: a POST whose JSON body has `{"ref":"main"}` fires with input `ref` set, and a body failing the pipeline's input schema returns HTTP 400 with the validation error. Run — expect FAIL with "undefined: http.New".
- [x] Write `internal/trigger/git/git_test.go` asserting `TestGitWebhookVerifiesSignatureAndRejectsForgery`: a GitHub push payload with a valid HMAC fires; one with a wrong signature returns 401 and does not fire.
- [x] Add `TestGitWebhookMarksPayloadTainted` asserting the fired run's inputs carry the taint marker from Task 51.
- [x] Write `internal/trigger/completion/completion_test.go` asserting `TestCompletionTriggerFiresDownstreamPipeline` and that a failed upstream run does not fire it.
- [x] Write `internal/trigger/trigger_test.go` asserting `TestAllFourTriggersStartSamePipeline` — one unchanged pipeline definition fired by schedule, HTTP, git webhook and completion.
- [x] Implement the three triggers, sharing input validation against the pipeline's declared input schema.
- [x] Run `go test ./internal/trigger/...` — expect PASS. Commit.

## Task 42: Weighted fair queuing and concurrency budgets

Files: `internal/scheduler/fairness.go`, `internal/scheduler/budget.go`, `internal/scheduler/fairness_test.go`
Interfaces: produces `scheduler.Queue` with `Enqueue(ctx, item QueueItem) error`, `Next(ctx, slots int) ([]QueueItem, error)`; `scheduler.Budgets` with `Acquire(ctx, tenantID, pipelineID string) (release func(), ok bool)`.

- [x] Write `internal/scheduler/fairness_test.go` asserting `TestWeightedFairQueuingUnderSaturation`: with tenant `a` enqueuing 10000 steps and tenant `b` enqueuing 10, `b`'s steps are all dispatched within the one-second target and `a` does not occupy more than its weighted share. Run — expect FAIL with "undefined: scheduler.NewQueue".
- [x] Add `TestConcurrencyBudgetCapsPipelineInFlight` asserting a pipeline with a budget of 2 never has 3 steps dispatched simultaneously.
- [x] Add `TestBudgetReleaseOnStepFailureNotOnlyOnSuccess` asserting a failed step releases its budget slot.
- [x] Add `TestQueueIsDeterministicUnderEqualWeights` asserting equal-weight tenants interleave one-for-one.
- [x] Implement `internal/scheduler/fairness.go` as a deficit round-robin over per-tenant queues and `budget.go` as a counting semaphore persisted in NATS KV so budgets survive a control-plane restart.
- [x] Run `go test ./internal/scheduler` — expect PASS. Commit.

## Task 43: Control-plane scale-out and backpressure

Files: `internal/server/partition.go`, `internal/server/partition_test.go`, `internal/bus/backpressure.go`
Interfaces: produces `server.PartitionFor(runID string, n int) int`, `server.ClaimPartitions(ctx, instanceID string) ([]int, error)`; `bus.WithMaxAckPending(n int) bus.SubOption`.

- [x] Write `internal/server/partition_test.go` asserting `TestSingleWriterPerRun`: two control-plane instances consuming the same stream never both advance the same run, verified by asserting no duplicate sequence is written for 1000 concurrent runs. Run — expect FAIL with "undefined: server.ClaimPartitions".
- [x] Add `TestPartitionRebalanceOnInstanceLoss` asserting that killing one of three instances results in its partitions being claimed by the survivors within 5s.
- [x] Add `TestBackpressureStopsPullingWhenAckPendingReached` asserting the consumer stops fetching once `MaxAckPending` is outstanding and resumes after acks.
- [x] Implement `internal/server/partition.go` hashing run ids into partitions claimed via NATS KV leases, and `internal/bus/backpressure.go`.
- [x] Run `make test-integration` — expect PASS. Commit.

## Task 44: Observability

Files: `internal/obs/tracing.go`, `internal/obs/metrics.go`, `internal/obs/obs_test.go`
Interfaces: produces `obs.Init(ctx, cfg obs.Config) (shutdown func(context.Context) error, err error)`, `obs.StepSpan(ctx, runID, stepID string) (context.Context, trace.Span)`.

- [x] Write `internal/obs/obs_test.go` asserting `TestEachStepEmitsOneSpanWithRunAndStepAttributes`: running the two-step pipeline against an in-memory exporter yields two step spans carrying `dhole.run_id` and `dhole.step_id`, parented to a run span. Run — expect FAIL with "undefined: obs.StepSpan".
- [x] Add `TestStepResourceMetricsAreRecorded` asserting `dhole_step_cpu_seconds` and `dhole_step_max_rss_bytes` are exported per step.
- [x] Add `TestBuildDurationRegressionMetricIsExported` asserting `dhole_step_duration_seconds` carries a `cache_hit` label so slow-down analysis can separate the two.
- [x] Implement `internal/obs/tracing.go` and `metrics.go` with OpenTelemetry, propagating trace context through the `JobDispatch` message so engine-side spans join the run trace.
- [x] Run `go test ./internal/obs` — expect PASS. Commit.

## Task 45: Web app scaffold and generated client

Files: `web/package.json`, `web/vite.config.ts`, `web/src/main.tsx`, `web/src/api/client.ts`, `web/buf.gen.web.yaml`, `web/src/api/client.test.ts`, `web/playwright.config.ts`
Interfaces: produces `web/src/api/client.ts` exporting `pipelineClient`, `runClient` built from generated Connect-Web stubs; `npm run gen` regenerates them; `make web-check` runs typecheck, lint and unit tests.

- [x] Write `web/src/api/client.test.ts` asserting `TestClientIsGeneratedNotHandWritten`: importing `pipelineClient` exposes a method for every RPC listed in the generated service descriptor, failing with the missing names. Run `npm test` — expect FAIL with "Cannot find module '../gen/dhole/v1/api_connect'".
- [x] Write `web/buf.gen.web.yaml` emitting `bufbuild/es` and `connectrpc/es` into `web/src/gen`, and add `npm run gen` invoking it.
- [x] Scaffold Vite + React 19 + TypeScript strict, TanStack Query, and set `web/package.json` engines to Node 22.
- [x] Implement `web/src/api/client.ts` wiring the Connect transport with the `Authorization` header from the stored token.
- [x] Add `make web-check` to `make check` and Playwright to CI with `playwright.config.ts` pointing at a `dhole serve --mode embedded` fixture.
- [x] Run `make check` — expect PASS. Commit.

## Task 46: Canvas with typed ports

Files: `web/src/canvas/Canvas.tsx`, `web/src/canvas/StepNode.tsx`, `web/src/canvas/edges.ts`, `web/src/canvas/layout.ts`, `web/e2e/canvas-authoring.spec.ts`
Interfaces: produces `<Canvas pipelineId revisionId />`; `layout.autoLayout(steps, edges): NodePositions` (deterministic, derived from the DAG, never persisted).

- [x] Write `web/e2e/canvas-authoring.spec.ts` asserting `TestCanvasAuthorsPipelineEndToEnd`: add two nodes, drag from `a.out` to `b.in`, set a property, save, and require the resulting revision from the API contains the edge. Run `npx playwright test` — expect FAIL with "locator not found: [data-testid=add-step]".
- [x] Add a case asserting dragging from a `blob` output to a `structured` input is refused at drop time with a visible message, and no `ApplyOperation` call is made.
- [x] Add a case asserting node positions are not sent in any `ApplyOperation` request body, proving layout stays out of the document.
- [x] Implement `web/src/canvas/` with React Flow: `StepNode` rendering one handle per declared port, `edges.ts` validating type compatibility before allowing a connection, `layout.ts` computing deterministic positions from `dag` levels.
- [x] Run `npx playwright test` — expect PASS. Commit.

## Task 47: Schema-driven property panel and diff review

Files: `web/src/panel/PropertyPanel.tsx`, `web/src/panel/schemaForm.tsx`, `web/src/review/DiffView.tsx`, `web/e2e/panel.spec.ts`
Interfaces: produces `<PropertyPanel stepId />` rendering a form from the plugin's `InputSchema`; `<DiffView revisionId />` showing the diff returned by `ApplyOperation`.

- [x] Write `web/e2e/panel.spec.ts` asserting `TestPanelIsRenderedFromPluginSchemaNotHardcoded`: publishing a new plugin with an added `retries` integer field causes that field to appear in the panel with no web code change. Run — expect FAIL with "locator not found: [name=retries]".
- [x] Add a case asserting a value violating the schema is rejected in the form with the schema's own error message before any request is sent.
- [x] Add a case asserting every save shows a diff the user must confirm, and cancelling it makes no `ApplyOperation` call.
- [x] Add a case asserting an effect-class override that widens capability is highlighted in the diff as a policy decision point.
- [x] Implement `schemaForm.tsx` rendering JSON Schema draft 2020-12 to controls, `PropertyPanel.tsx`, and `DiffView.tsx`.
- [x] Run `npx playwright test` — expect PASS. Commit.

## Task 48: Run view with realised graph and streamed logs

Files: `web/src/run/RunView.tsx`, `web/src/run/LogStream.tsx`, `internal/api/stream.go`, `web/e2e/run-view.spec.ts`
Interfaces: produces `GET /v1/runs/{id}/events` as SSE emitting run and step transitions; `GET /v1/runs/{id}/steps/{step}/logs` as SSE for live tail, falling back to the stored object once complete.

- [x] Write `web/e2e/run-view.spec.ts` asserting `TestRunViewShowsRealisedGraphCacheHitsAndLogs`: run the two-step pipeline, and require each node shows a duration, the second run shows both nodes marked cached, and log lines appear without a page reload. Run — expect FAIL with "locator not found: [data-testid=run-graph]".
- [x] Add a case asserting the view switches from the live log subject to the stored object when the run completes, and the full log is present after reload.
- [x] Add a case asserting a non-cacheable step displays the exact reason string from Task 16.
- [x] Add a case asserting a bounded loop renders as a container node that expands to its unrolled iterations in the run view.
- [x] Implement `internal/api/stream.go` with SSE (not WebSocket) plus `Last-Event-ID` resume, and the two React components.
- [x] Run `npx playwright test` — expect PASS. Commit. 16 passed, 1 skipped; the four that had been failing were an unrunnable seed shape, an SSE helper handing tests the frame instead of the payload, a step too fast to be caught mid-flight, and a real product bug — the app never re-rendered when the URL changed.

## Task 49: LLM step on go-ai-sdk

Files: `internal/steps/llm/llm.go`, `internal/steps/llm/fingerprint.go`, `internal/steps/llm/record.go`, `internal/steps/llm/llm_test.go`, `internal/runstore/migrations/0008_llm_calls.sql`
Interfaces: produces `llm.Step` implementing the step-type interface; `llm.Config{Provider, Model string; Temperature float32; OutputSchema []byte; MaxTokens int}`; `llm.Fingerprint(resp ai.Response) string`.

- [x] Write `internal/steps/llm/llm_test.go` asserting `TestLLMStepSchemaFingerprintAndBudgetCeiling` against a stub `ai.LanguageModel`: the step returns an object validated against `OutputSchema`, records model fingerprint, prompt, response, tokens and latency, and halts the run when the token ceiling is exceeded. Run — expect FAIL with "undefined: llm.New".
- [x] Add `TestMalformedObjectIsRetriedThenFailsWithProviderError` asserting a model returning unparseable JSON three times fails the step with the provider error recorded and never returns a partial object.
- [x] Add `TestFingerprintIncludesResolvedModelNotAlias` asserting two responses from the same alias but different underlying model ids produce different fingerprints, and that the Task 15 cache key changes accordingly.
- [x] Add `TestLLMStepIsPureOnlyWithPinnedModelAndZeroTemperature` asserting `EffectClass` resolves to `PURE` only when temperature is 0 and the model is digest-pinned, otherwise `IDEMPOTENT`.
- [x] Write `0008_llm_calls.sql` creating `llm_calls(tenant_id, run_id, step_id, attempt, model_fingerprint, prompt, response, prompt_tokens, completion_tokens, latency_ms)` with its own retention setting.
- [x] Implement `llm.go` using `ai.GenerateObject` from `github.com/azrtydxb/go-ai-sdk`, `fingerprint.go`, and `record.go`.
- [x] Run `go test ./internal/steps/llm` — expect PASS. Commit.

## Task 50: Bounded loops and agent steps

Files: `internal/steps/loop/loop.go`, `internal/steps/agent/agent.go`, `internal/steps/agent/actionspace.go`, `internal/steps/loop/loop_test.go`, `internal/steps/agent/agent_test.go`
Interfaces: produces `loop.Node{Subgraph *dholev1.Pipeline; MaxIterations int; ExitCondition string}` evaluated by CEL; `agent.Step{GrantedSteps []string; MaxSteps int}` exposing granted steps through `agent.AsTool`.

- [x] Write `internal/steps/loop/loop_test.go` asserting `TestBoundedLoopCeilingAndActionSpaceRefusal`: a loop whose exit condition never holds stops at `MaxIterations` and records an event whose payload contains "iteration ceiling reached". Run — expect FAIL with "undefined: loop.New".
- [x] Add `TestTopLevelGraphRemainsAcyclicWithLoopNode` asserting `dag.Build` succeeds on a pipeline containing a loop node and that the loop's subgraph is validated independently.
- [x] Write `internal/steps/agent/agent_test.go` asserting `TestAgentCannotInvokeStepOutsideGrantedSet`: an agent granted only `format` attempting `deploy` is refused with an error naming both.
- [x] Add `TestAgentCannotInvokeAtMostOnceStepWithoutApproval` asserting the call is routed through the Task 20 approval gate rather than executing.
- [x] Implement `loop.go` unrolling iterations into the run's event log so the run view can expand them, and `agent.go` wrapping `go-ai-sdk`'s `agent` package — `maxSteps` bounding iteration, `AsTool` exposing only granted steps, and its tool-call approval hook delegating to the approval step.
- [x] Run `go test ./internal/steps/...` — expect PASS. Commit.

## Task 51: Taint tracking

**Reconciliation required, from Task 41.** `internal/trigger/taint.go` already
marks webhook payloads, because the git trigger needed the marker before this
task existed. It represents a taint as a one-field wrapper,
`{"$dhole.taint": {"source": ..., "value": ...}}` — a wrapper rather than
metadata alongside the value, because a trigger hands the scheduler only
`map[string]*structpb.Value` and anything kept beside the value is lost at the
first store and replay. This task MUST keep that representation: a taint
package that looks for a different shape reads every payload already fired as
clean, which is the one failure mode taint tracking exists to prevent. Move
the four functions here under this task's planned names and either delegate
from `internal/trigger/taint.go` or update the three triggers. Nothing in
Task 41 clears a taint; the sanitisation gate is still this task's work.

Files: `internal/taint/taint.go`, `internal/taint/propagate.go`, `internal/taint/taint_test.go`
Interfaces: produces `taint.Mark(v *structpb.Value, source string) *structpb.Value`, `taint.IsTainted(v *structpb.Value) bool`, `taint.Propagate(in []*dholev1.OutputRef, out []*dholev1.OutputRef)`, `taint.Gate` step type clearing marks after explicit sanitisation.

- [x] Write `internal/taint/taint_test.go` asserting `TestTaintBlocksEffectfulStepUntilSanitised`: data from an untrusted git webhook reaching an `AT_MOST_ONCE` step is refused with an error naming the trigger source; inserting a `taint.Gate` step allows it. Run — expect FAIL with "undefined: taint.Mark".
- [x] Add `TestTaintPropagatesThroughPureSteps` asserting a `pure` step consuming tainted input produces tainted output.
- [x] Add `TestTaintReachingPrivilegedEngineIsRefused` asserting dispatch to an engine advertising `PRIVILEGED` with tainted inputs is denied by policy with a reason naming the taint.
- [x] Add `TestGateRecordsWhoSanitisedWhat` asserting the gate writes an event naming the principal and the fields cleared.
- [x] Implement `taint.go` storing marks in a reserved `structpb` field and on CAS object metadata, `propagate.go` called by the scheduler on every step completion, and wire the check into Task 21's policy input.
- [x] Run `go test ./internal/taint ./internal/policy` — expect PASS. Commit.

## Task 52: The three acceptance pipelines

Files: `acceptance/ci/pipeline.yaml`, `acceptance/automation/pipeline.yaml`, `acceptance/agent/pipeline.yaml`, `acceptance/acceptance_test.go`, `Makefile`
Interfaces: produces `make acceptance-ci`, `make acceptance-automation`, `make acceptance-agent`.

- [x] Write `acceptance/acceptance_test.go` asserting `TestAcceptanceCICacheHit`: run `acceptance/ci/pipeline.yaml` twice against a real containerd engine and require the second run reports `cache_hit` for the build step and a wall time under 20% of the first. Run — expect FAIL with "no such file: acceptance/ci/pipeline.yaml".
- [x] Write `acceptance/ci/pipeline.yaml` building a small container image from a checked-in Dockerfile with declared inputs and outputs.
- [x] Add `TestAcceptanceAutomationTriggersAndWait` running `acceptance/automation/pipeline.yaml` from both a cron schedule and an HTTP call, holding a 5s durable wait across a deliberate control-plane restart, with one step on the process engine and one on Kubernetes.
- [x] Add `TestAcceptanceAgentLoopAndApproval` running `acceptance/agent/pipeline.yaml`: an LLM step producing schema-validated output, a bounded loop capped at 3, an approval gate decided through the API, and a token cost assertion greater than zero.
- [x] Add the three `make acceptance-*` targets and run them in CI nightly. The targets existed; the nightly job did not — `.github/workflows/nightly.yml` now runs them with the infrastructure actually provisioned.
- [x] Run `make acceptance-ci acceptance-automation acceptance-agent` — expect PASS. Commit.

## Task 53: VM executor with snapshot restore

Files: `internal/executor/vm/vm.go`, `internal/executor/vm/firecracker.go`, `internal/executor/vm/qemu.go`, `internal/executor/vm/proc.go`, `internal/executor/vm/vsock_linux.go`, `internal/executor/vm/vsock_other.go`, `internal/executor/vm/vmwire/vmwire.go`, `internal/executor/vm/agent/main.go`, `internal/executor/vm/vm_test.go`, `internal/executor/vm/qemu_internal_test.go`, `cmd/dhole-engine/executor.go`, `proto/dhole/v1/common.proto`, `docs/executors/vm.md`, `.github/workflows/ci.yml`
Interfaces: produces `vm.New(cfg vm.Config) (executor.Executor, error)`; `Config{Backend firecracker|qemu; BinaryPath, KernelImage, RootfsImage string; SnapshotDir string; VCPUs int; MemMiB int; KernelArgs string}`; `EnvironmentIdentity()` returns the rootfs snapshot digest; `vm.Snapshotter`/`vm.Snapshot` for restore. `BinaryPath` and `KernelArgs` were not in the original interface line and are added here: a hypervisor found only on PATH cannot be pinned by an operator, and a guest kernel's command line is not the same on every machine model.

- [x] Write `internal/executor/vm/vm_test.go` calling Task 9's `executorContract` as `TestVMExecutorContract` against Firecracker from `DHOLE_TEST_FIRECRACKER_BIN`, skipping when unset. Run — expect FAIL with "undefined: vm.New". Confirmed failing for that reason, then run against Firecracker v1.13.1 with `/dev/kvm` on an arm64 node: the whole contract PASSES in 3.5s, the 2s SIGTERM window included (0.26s).
- [x] Add `TestSnapshotRestoreBootsUnderOneHundredMilliseconds` asserting a restored microVM reaches the guest agent in under 100ms, measured over 20 iterations. Passes on hardware: 20 restores plus the snapshot itself in 2.0s. The restore path is serialised, because the vsock socket's path is part of the snapshotted machine state and every restore binds the same one.
- [x] Add `TestEnvironmentIdentityIsSnapshotDigest` asserting the identity changes when the rootfs changes and is stable otherwise, so Task 15's cache key is honest for VM steps. It hashes CONTENT, not the path: a rootfs is rebuilt in place far more often than an image tag is republished. `TestASandboxOnASecondRootfsNamesThatRootfsAndNotTheDefault` covers the ADR 0021 half — a sandbox booted from a second initramfs (the default with a cpio member concatenated on, so it still boots) reports that one and not the executor's default.
- [x] Add `TestNestedVirtCapabilityIsAdvertisedOnlyWhenAvailable` asserting `Capabilities()` omits nested virt on a host without `/dev/kvm`. It needs more than `/dev/kvm`: a host that virtualises perfectly well may still refuse to nest, so the kvm module's `nested` parameter has to say yes. Verified in both directions — omitted on macOS with no `/dev/kvm`, and omitted on the arm64 node that HAS `/dev/kvm` but no `nested` parameter. This added `CAPABILITY_NESTED_VIRT = 5` to `dhole.v1.Capability`, which is additive.
- [x] Implement `firecracker.go` with the Firecracker API over its unix socket including snapshot create and load, `qemu.go` as the portable fallback, and `vm.go` selecting a backend and exposing a guest agent for `Exec`, `Put` and `Get`. The agent is a separate static binary that is the guest's init (`internal/executor/vm/agent`), speaking one multiplexed protocol (`internal/executor/vm/vmwire`) over vsock — multiplexed because the contract reads a file out of the sandbox WHILE a command runs in it.
- [ ] `qemu.go` is written and its command line is unit-tested, but the CONTRACT has never been run against it, so this half is not closed. QEMU launches from these arguments and the guest resets before printing a line: the Firecracker CI kernel does not boot on QEMU's machine models, with or without KVM, with or without earlycon. A stock Alpine kernel boots the same initramfs and runs the agent, and then the agent cannot open a vsock socket because that kernel ships vsock as a module the initramfs does not carry. What is missing is one guest kernel with `CONFIG_VIRTIO_VSOCKETS=y` that also boots under `virt`/`microvm`; `TestQEMUExecutorContract` skips until there is one, and CI deliberately leaves `DHOLE_TEST_QEMU_BIN` unset rather than turning a known gap into a red job.
- [x] Register the backend as `DHOLE_EXECUTOR=vm` in `cmd/dhole-engine/executor.go` and add it to `executorKinds`, and add a `vm executor` job to `.github/workflows/ci.yml` that builds a guest rootfs and refuses a run that only skipped. The CI job is WRITTEN AND NOT RUN — it targets amd64 GitHub runners and nothing in this task executed it; the equivalent was executed by hand on arm64.
- [x] Run `make conformance ENGINE="dhole-engine --executor vm"` — expect PASS. DONE on kw, 2026-09-11, as a Job on a node with a real `/dev/kvm`: the suite brings its own plane, embedded bus and filesystem object store, so one pod with the kvm device is the whole harness. The image is the engine image plus the hypervisor, the guest kernel and the rootfs, and the engine reads `DHOLE_EXECUTOR=vm` from the pod. **The first run was 10 passed, 1 failed** — `secret-redemption`, with "vm executor: CAPABILITY_SECRETS: capability not advertised by this backend (it advertises [CAPABILITY_PRIVILEGED])". That was a real defect and not a harness artefact: `CAPABILITY_SECRETS` describes the AGENT, which redeems over the bus before any sandbox exists, but the agent handed the executor the step's WHOLE capability set. The vm and containerd backends refuse a capability they do not advertise; process and kubernetes never check, so every previous run — both local conformance suites and the whole unit suite — was green on backends that could not see it. Fixed by `engine.sandboxCapabilities`, the mirror of `advertisedCapabilities`, and guarded by `TestASandboxIsNotAskedForACapabilityTheAgentItselfSatisfies` with a strict executor that checks what it was asked for.

## Task 54: macOS and Windows process engines

Files: `internal/executor/process/process_darwin.go`, `internal/executor/process/process_windows.go`, `internal/executor/process/process_platform_test.go`, `.github/workflows/ci.yml`
Interfaces: produces build-tagged implementations of the Task 9 signal and process-tree behaviour for `darwin` and `windows`.

- [x] Write `internal/executor/process/process_platform_test.go` asserting `TestProcessTreeIsKilledOnCancelOnEveryPlatform`: a command spawning a child that outlives its parent is fully terminated within 2s. Run on Windows — expect FAIL with "signal: not supported by windows".
- [x] Add `TestExitCodeOnOOMIsReportedConsistently` asserting a memory-exhausting command yields a documented exit code on each platform rather than a silent success.
- [x] Implement `process_windows.go` using a Job Object to kill the process tree, and `process_darwin.go` using process groups with `SIGTERM` then `SIGKILL`.
- [x] Extend `executorContract` with the two new cases so all executors are held to them, and update `docs/wire-contract.md` with the documented OOM exit codes.
- [x] Add `macos-latest` and `windows-latest` engine jobs to CI running `make conformance`. Both existed; LINUX did not run conformance at all, which is where the suite's own protocol-negotiation defect would have been caught. Added to the gate.
- [x] Run `make conformance` on all three platforms — expect PASS. Commit. Linux and macOS run the reference engine; Windows deliberately does not and says why in the workflow. The nightly job additionally runs the GO engine against the suite, which no push job did.

## Task 55: Dynamic pipelines

Files: `internal/dynamic/generator.go`, `internal/dynamic/generator_test.go`, `web/src/canvas/GeneratorNode.tsx`, `internal/scheduler/generator.go`, `internal/scheduler/generator_test.go`, `web/src/canvas/Canvas.tsx`, `web/src/run/events.ts`, `web/src/run/RunView.tsx`, `web/e2e/generator-node.spec.ts`, `web/e2e/seed/main.go`
Interfaces: produces `dynamic.Generator` step type emitting a `*dholev1.Pipeline` fragment; `dynamic.Splice(parent *dholev1.Pipeline, at string, fragment *dholev1.Pipeline) (*dholev1.Pipeline, error)`; the scheduler derives a run's graph from the pinned definition PLUS every `GENERATOR_FRAGMENT_REALISED` record in its log.

- [x] Write `internal/dynamic/generator_test.go` asserting `TestGeneratorEmitsSubgraphSplicedIntoRun`: a generator emitting three steps results in those three executing and the run completing. Run — expect FAIL with "undefined: dynamic.Splice".
- [x] Add `TestSpliceRejectsDuplicateStepID` asserting a fragment reusing an existing step id fails with an error naming the collision rather than silently overwriting.
- [x] Add `TestSplicedGraphIsStillAcyclic` asserting a fragment introducing a cycle is rejected at splice time.
- [x] Add `TestAuthoredGraphShowsGeneratorAsOpaque` (Playwright) asserting the editor renders the generator as an opaque "expands at runtime" node while the run view shows the realised steps. Both halves are the SHIPPING screens: the canvas routes a generator step to its own node, and the run view folds the realised record out of the event stream. The harness module that stood in for both is gone, and with it the checked-in record fixture the browser used to read — the suite now seeds a run whose fragment `internal/dynamic` realised against the pipeline under test.
- [x] Implement `generator.go` and `Splice`, recording the realised fragment in the event log so replay is deterministic, and `GeneratorNode.tsx`.
- [x] Fold the record in the SCHEDULER, which is where determinism is observable: `runState` collects every realised fragment and `Advance` splices them into the pinned definition before building the DAG. Without it a restarted plane derived the authored graph, saw the generator already succeeded, and COMPLETED the run while the steps it realised had never run — green and wrong. A fragment that no longer splices is an error, never a fall back to the authored graph.
- [x] Run `go test ./internal/dynamic && npx playwright test` — expect PASS. Commit.

## Task 56: Multiplayer editing

Files: `internal/api/presence.go`, `web/src/canvas/Presence.tsx`, `internal/api/presence_test.go`, `web/e2e/multiplayer.spec.ts`
Interfaces: produces `WatchPresence` streaming RPC emitting `PresenceEvent{principal, selection, cursor}`; operations already carry `base_revision` from Task 27.

- [x] Write `internal/api/presence_test.go` asserting `TestConcurrentOperationsOnDifferentStepsBothApply`: two clients applying `SetProperty` to different steps from the same base revision both succeed and the final revision contains both changes. Run — expect FAIL with "undefined: api.WatchPresence".
- [x] Add `TestConcurrentOperationsOnSameStepConflict` asserting the second returns `CodeAborted` with the newer revision attached so the client can rebase.
- [x] Write `web/e2e/multiplayer.spec.ts` asserting two browser contexts see each other's selections and that a conflicting edit surfaces a rebase prompt rather than silently overwriting.
- [x] Implement `presence.go` over an ephemeral bus subject per pipeline, and `Presence.tsx` rendering remote cursors and selections.
- [x] Run `go test ./internal/api && npx playwright test` — expect PASS. Commit.

## Task 57: Importers for GitLab CI, GitHub Actions, Woodpecker and n8n

Files: `internal/importers/gitlab.go`, `internal/importers/actions.go`, `internal/importers/woodpecker.go`, `internal/importers/n8n.go`, `internal/importers/importers_test.go`, `testdata/import/`
Interfaces: produces `importers.Importer` with `Import(ctx, src []byte) (*dholev1.Pipeline, importers.Report, error)`; `Report{Unsupported []string; Warnings []string}`.

- [x] Write `internal/importers/importers_test.go` asserting `TestGitLabCIImportProducesRunnablePipeline`: importing `testdata/import/gitlab-ci.yml` yields a pipeline that passes `dag.Build` and `dag.TypeCheck` with no diagnostics. Run — expect FAIL with "undefined: importers.NewGitLab".
- [x] Add `TestImportReportsUnsupportedConstructsRatherThanDroppingThem` asserting a GitLab file using `rules:changes` produces a `Report.Unsupported` entry naming it, and that `Import` never silently discards a job.
- [x] Add `TestSharedWorkspaceIsTranslatedToExplicitArtifacts` asserting a Woodpecker pipeline relying on the implicit workspace produces explicit input and output ports between its steps.
- [x] Add `TestN8NNodesMapToStepsAndConnectionsToTypedEdges` asserting an n8n export round-trips to a pipeline whose edges carry structured types.
- [x] Implement the four importers, each emitting `at-most-once` for any step it cannot prove pure, so an import is never more permissive than the original.
- [x] Run `go test ./internal/importers` — expect PASS. Commit.

## Task 58: Tenant provisioning, quotas and billing metering

Files: `internal/tenancy/provision.go`, `internal/tenancy/quota.go`, `internal/tenancy/meter.go`, `internal/tenancy/tenancy_test.go`, `internal/runstore/migrations/0009_tenancy.sql`
Interfaces: produces `tenancy.Provision(ctx, name string) (Tenant, error)`, `tenancy.Quota{MaxConcurrentSteps, MaxRunsPerDay int; MaxCASBytes int64}`, `tenancy.Meter.Record(ctx, tenantID string, u Usage) error`.

- [x] Write `internal/tenancy/tenancy_test.go` asserting `TestProvisionCreatesIsolatedTenant`: a provisioned tenant receives its own NATS account credentials, and Task 22's isolation assertions hold against it. Run — expect FAIL with "undefined: tenancy.Provision".
- [x] Add `TestQuotaExceededRejectsNewRunsWithoutAffectingRunning` asserting a tenant at its daily run quota gets a clear rejection while its in-flight runs complete.
- [x] Add `TestCASQuotaBlocksWriteBeforeExceeding` asserting a blob write that would exceed `MaxCASBytes` fails with a quota error rather than partially writing.
- [x] Add `TestMeteredUsageMatchesActualStepSeconds` asserting recorded usage for a known 2s step is within 10% of 2 step-seconds.
- [x] Write `0009_tenancy.sql` creating `tenants`, `quotas`, `usage_records`.
- [x] Implement `provision.go`, `quota.go` enforced in the scheduler and CAS, and `meter.go` deriving usage from the run event log so metering is reconstructible.
- [x] Run `go test ./internal/tenancy` — expect PASS. Commit.

## Task 59: Documentation, release engineering and packaging

Files: `docs/`, `.goreleaser.yaml`, `.github/workflows/release.yml`, `charts/dhole/`, `docs/docs_test.go`
Interfaces: produces `dhole` and `dhole-engine` binaries for linux/darwin/windows on amd64/arm64, multi-arch container images, and a Helm chart.

- [x] Write `docs/docs_test.go` asserting `TestEveryStepTypeAndTriggerIsDocumented`: enumerate registered step types, trigger kinds and executor kinds and require a matching page under `docs/`, failing with the undocumented names. Run — expect FAIL with "no such file or directory: docs/steps".
- [x] Add `TestQuickstartCommandsRunAsWritten` extracting fenced `bash` blocks from `docs/quickstart.md` and executing them against a scratch directory, requiring exit 0.
- [x] Write `docs/` covering quickstart, the wire contract, writing an engine, writing a plugin, policy authoring in CEL, deployment topologies, and the upgrade and version-skew policy.
- [x] Write `.goreleaser.yaml` producing both binaries for all platform pairs and multi-arch images, `charts/dhole/` deploying control plane, Postgres, NATS and engines, and `.github/workflows/release.yml` publishing on `v*` tags with cosign signing and SBOM attachment.
- [x] Add `TestReleaseArtifactsAreSignedAndHaveSBOM` verifying the published image with `cosign verify` and requiring an SPDX attestation.
- [x] Run `goreleaser release --snapshot --clean && go test ./docs` — expect PASS. Commit.
