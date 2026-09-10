# Writing an engine

An engine is a separate process, in any language, that pulls self-contained jobs
off a message bus, runs them, and publishes status and logs back. It dials
outbound and nothing dials it, so it runs on a laptop, on a build Mac, on a GPU
box or inside a customer's network behind CGNAT
([ADR 0004](../.procoder/adr/0004-control-plane-and-data-plane-split-with-polyglot-engines.md)).

The path is three steps, in this order.

## 1. Read the contract

**[docs/wire-contract.md](wire-contract.md) is the contract.** It says what each
message means, which subject carries it, and which rules are load-bearing. The
schema is `proto/dhole/v1/engine.proto`; generate your language's bindings from
it with `buf` or `protoc`.

Do not read the Go engine in `internal/engine/` and copy what it does. An engine
that infers behaviour from the reference implementation breaks when the
reference implementation changes — and the contract's own "What this document
does not yet specify" section is the honest list of places where the document is
still thinner than the code. Those gaps are named, with the choice the
conformance suite makes for each, so a second implementer makes the same choice
rather than a different one.

The three rules everything else follows from:

- **A `JobDispatch` is self-contained.** An engine never calls back to find out
  what to do. A control-plane restart is a replay, not a loss.
- **Engines are outbound-only.** There is no port to open. The one inbound path,
  `EngineControl`, arrives over the connection the engine itself opened.
- **The bus is transport, not truth.** A lost message is republished and a
  duplicate is deduplicated by fence token. Absence is never evidence.

## 2. Write the engine

The shape of the loop, in any language:

1. Connect to NATS with the credentials the deployment gave you.
2. Publish an `EngineRegistration` — framed in an `EngineMessage`, not bare —
   on `engine.registration`, advertising your platform, your capabilities,
   every protocol version you speak, and — if you have one —
   `environment_identity`, the digest of the environment you run steps in.
   Re-publish it unchanged every fifteen seconds; it is fire-and-forget, so a
   plane that restarts learns you exist from the next one.
3. Subscribe to `job.dispatch.<tier>.<caps>` for your tier and for each
   capability set you can satisfy. `<caps>` is a stable hash of the sorted
   capability set, so the bus does the filtering; see "Computing `<caps>`".
4. For each `JobDispatch`: fetch the input ports, run the command, stream
   `LogChunk`s on `job.logs.<run>.<step>`, write the output ports, and publish a
   terminal `JobStatus` on `job.status.<run>.<step>`.
5. Heartbeat on `engine.heartbeat.<engine-id>` while you hold work.
6. Honour `EngineControl` — cancel, drain — and refuse any control message whose
   fence token is not the one you hold.

`environment_identity` is what the control plane hashes the step cache against
(ADR 0021), and it is the one field the plane cannot work out for itself: on a
distributed deployment your environment is on a different machine. It must be
**stable** for identical environments, **different** for different ones, and
**absent** rather than invented — an image digest or a VM snapshot id, never a
hostname, a start time or a version string you made up. Leave it empty if you
have nothing reproducible to name; your tier then caches nothing, which is the
correct answer. Engines in one tier must agree on it, or the tier caches
nothing either.

Advertise only what you can honestly enforce. A capability you advertise is one
the scheduler will rely on; an engine that claims `PRIVILEGED` without being able
to grant or contain it has turned a policy decision into a lie.

`testdata/engines/minimal-python/engine.py` is a complete engine in Python,
written against the document rather than against the Go code, and it exists to
prove the contract is implementable by a stranger. It is a good starting point
and a short read.

## 3. Run the conformance suite

The suite is the executable half of the contract. It starts its own NATS and
object store, plays the control plane, and runs one case per obligation. It is
how you find out whether your engine complies, and it does not care what
language you wrote it in:

```
make conformance ENGINE="python3 testdata/engines/minimal-python/engine.py"
```

`ENGINE` is the command to launch, split on spaces. Add `-v` to
`CONFORMANCE_FLAGS` to see your engine's own stdout and stderr, which is what
you want while a case is failing.

The cases, each named after the obligation it holds you to:

| Case                               | What it proves                                                       |
| ---------------------------------- | -------------------------------------------------------------------- |
| `registration`                     | you announce yourself in an `EngineMessage` frame, with a platform   |
| `version-negotiation`              | you refuse a dispatch whose protocol version you do not speak        |
| `dispatch-and-success`             | the ordinary path, end to end                                        |
| `non-zero-exit`                    | a failing command is `PHASE_FAILED` carrying its `exit_code`         |
| `cancellation`                     | a `Cancel` stops the work rather than being acknowledged and ignored |
| `step-timeout`                     | you enforce the timeout you were given                               |
| `log-throughput-10mb`              | log streaming does not fall over or lose chunks under volume         |
| `binary-artifact-round-trip`       | an output port survives bytes that are not text                      |
| `secret-redemption`                | you redeem a handle rather than expecting a value                    |
| `lease-renewal-during-a-long-step` | a long step keeps its lease instead of being declared orphaned       |
| `fenced-out-attempt-refused`       | you refuse work fenced out by a newer attempt                        |

An engine that passes every case is an engine the control plane will treat as
one of its own. Two of these — `secret-redemption` and `step-timeout` — depend
on details the contract has not finished specifying; the suite's choices for
them are written down in the contract's last section, and they are the ones to
implement.

## Where the details live

- **Protocol version** — `internal/wire`. The control plane speaks
  `ProtocolVersion` and accepts engines one version behind. See
  [upgrades and version skew](upgrades.md).
- **Capabilities and effect classes** — `proto/dhole/v1/common.proto`, and
  [writing a plugin](writing-a-plugin.md) for what they oblige.
- **Executor backends** — an engine that runs steps somewhere other than a bare
  host process implements `executor.Executor`. The two in the tree are
  [process](executors/process.md) and [kubernetes](executors/kubernetes.md).

## Running the reference engine

`dhole-engine` is the Go engine as a standalone binary. Every knob is an
environment variable and every connection is outbound:

| Variable                     | Required | Meaning                                                      |
| ---------------------------- | -------- | ------------------------------------------------------------ |
| `DHOLE_BUS_URL`              | yes      | the NATS server to dial                                      |
| `DHOLE_ENGINE_ID`            | yes      | this engine's identity, and its control subject              |
| `DHOLE_TIER`                 | yes      | the trust tier whose work it takes                           |
| `DHOLE_SLOTS`                | no       | concurrent steps; default 1                                  |
| `DHOLE_EXECUTOR`             | no       | `process` (default) or `kubernetes`                          |
| `DHOLE_STATE_DIR`            | no       | where a filesystem object store lives                        |
| `DHOLE_OBJECT_STORE`         | no       | `filesystem` (default) or `s3`                               |
| `DHOLE_BLOB_DIR`             | no       | the directory, when the store is `filesystem`                |
| `DHOLE_S3_BUCKET`            | for s3   | the bucket logs and artifacts go in                          |
| `DHOLE_S3_ENDPOINT`          | no       | empty for AWS; set for MinIO or another S3-compatible server |
| `DHOLE_S3_REGION`            | no       | the bucket's region                                          |
| `DHOLE_S3_ACCESS_KEY_ID`     | no       | empty falls back to the ambient AWS credential chain         |
| `DHOLE_S3_SECRET_ACCESS_KEY` | no       | with the key id above                                        |

The content-addressed store is built on whatever `DHOLE_OBJECT_STORE` names,
so there is one place to configure and no way to end up with logs in a bucket
and artifacts on a disk.

**A distributed deployment needs `s3`.** The `filesystem` store is local to the
process: an engine writes a step's log and its outputs to its own disk, and the
control plane — a different process, usually on a different machine — looks for
them on its own and does not find them. Nothing errors. Every step succeeds and
everything it produced is unreachable. The engine warns at start-up when it is
in this position, because the symptom itself points nowhere near the cause.

The executor is also chosen here. `process` runs steps directly on the engine's
host and needs nothing; `kubernetes` runs each step in a sandbox pod and needs
a service account permitted to create pods and exec into them. An unrecognised
name is refused at start-up rather than falling back to `process`, because
falling back would run a step on the host that asked for a sandbox.
