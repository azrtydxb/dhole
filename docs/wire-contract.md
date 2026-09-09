# The engine wire contract

This document is the contract between the Dhole control plane and any engine,
in any language. An engine that obeys what is written here works; an engine
that infers behaviour from the Go implementation will break when the Go
implementation changes.

The schema lives in `proto/dhole/v1/engine.proto`. This document says what the
messages mean, which subjects carry them, and which rules are load-bearing.

## The three rules that shape everything else

**A `JobDispatch` is self-contained.** Everything an engine needs to run a step
is in that message or reachable from a reference inside it. An engine never
calls back into the control plane to find out what to do. This is what makes a
control-plane restart a replay rather than a loss: the dispatch is still sitting
on the bus, and it still means exactly what it meant when it was written.

**Engines are outbound-only.** An engine dials the bus. Nothing dials an engine.
There is no port to open and no inbound firewall rule to write, which is what
lets an engine live on a laptop, behind NAT, or in a customer's network. The one
inbound path is `EngineControl`, delivered over the same connection the engine
opened.

**The bus is transport, not truth.** Durable run state lives in the control
plane's store and reaches the bus through an outbox. A message that is lost is
re-published; a message that arrives twice is deduplicated by fence token. Never
treat the absence of a message as evidence that something did not happen.

## Subjects

Every subject is tenant-scoped — but the scope is not a segment in the name.
Each tenant is a separate NATS account, and an account is its own subject
namespace, so the same subject string in two accounts is two different
subjects. Isolation is enforced by the server against the connection's
credentials, not by a prefix an engine could get wrong or forge.

That is why no `<tenant>` appears in the table below. An engine never spells
its tenant and could not reach another one by spelling it differently: its
credentials place it in exactly one account, and its permissions within that
account are further limited to its own tier.

| Subject                        | Direction        | Message              |
| ------------------------------ | ---------------- | -------------------- |
| `job.dispatch.<tier>.<caps>`   | plane → engine   | `JobDispatch`        |
| `job.status.<run>.<step>`      | engine → plane   | `JobStatus`          |
| `job.logs.<run>.<step>`        | engine → viewers | `LogChunk`           |
| `engine.control.<engine-id>`   | plane → engine   | `EngineControl`      |
| `engine.heartbeat.<engine-id>` | engine → plane   | `EngineHeartbeat`    |
| `engine.registration`          | engine → plane   | `EngineRegistration` |

`<tier>` is the trust tier the work is dispatched to — `trusted`, `untrusted`,
and whatever else the deployment defines. An engine's bus credentials permit
subscribing to its own tier only. This is enforced by the bus, not by the
control plane's good manners: an engine in the untrusted tier that subscribes to
`job.dispatch.trusted.*` receives a permissions error.

`<caps>` is a stable hash of the sorted capability set a step requires. An
engine subscribes to the dispatch subjects matching capability sets it can
satisfy, so filtering happens at the bus rather than by receiving and rejecting
work it was never eligible for.

`job.dispatch.*` is a **work queue**: exactly one engine receives each dispatch,
and an unacknowledged message is redelivered. `job.status.*` is durable —
the control plane must not miss one. `job.logs.*` is **ephemeral and
best-effort**; see Logs below.

## The message flow of one attempt

1. The scheduler finds a step ready, claims a lease, and gets a fence token.
2. It enqueues a `JobDispatch` through the outbox. The event and the outbox row
   commit in one transaction, so a dispatch is never published for a step whose
   readiness was rolled back.
3. An engine pulls the dispatch, checks `protocol_version`, and publishes
   `JobStatus{PHASE_ACCEPTED}`.
4. The engine runs the step, streaming `LogChunk` messages as output appears and
   writing the authoritative log to the object store.
5. The engine publishes a terminal `JobStatus` — `SUCCEEDED`, `FAILED`, or
   `CANCELLED` — carrying `outputs` and `log_key`.
6. Only after the terminal status is published does the engine acknowledge the
   dispatch. Acknowledging first would let a crash between the two lose the
   step silently.

## Fence tokens

Every dispatch carries a `fence_token` from the step's lease. The engine echoes
it unchanged on every `JobStatus` and every `InFlight` entry.

The control plane ignores any status whose fence is older than the current
lease. This is what makes duplicate delivery safe: if an engine was presumed
dead and its step re-dispatched, the resurrected engine's late report is
recognised as stale rather than overwriting a newer attempt's result.

An engine must never invent, reuse, or omit a fence token.

## Protocol version negotiation

`EngineRegistration.protocol_versions` lists every version the engine speaks.
The control plane picks the highest version both sides support, and refuses
anything more than one version behind its own. An engine that is refused should
exit with a clear message rather than retry — the operator has to upgrade it.

`JobDispatch.protocol_version` states the version that dispatch is written in.
An engine that receives a version it does not speak must reply with
`JobStatus{PHASE_FAILED}` and an error containing "unsupported protocol". It
must not drop the message: silence is indistinguishable from a dead engine, and
the step would hang until its lease expired.

Within a major version, schema changes are additive only. Fields are never
renumbered, never removed, and never change meaning.

## Trace context

`JobDispatch.trace_context` carries the W3C trace context of the RUN this step
belongs to, as carrier headers: `traceparent`, and `tracestate` when one is
set. It is a `map<string, string>` rather than a named field precisely so that
an engine which copies the whole map forward keeps working when the W3C spec
grows another header.

An engine that wants its work to appear in the run's trace must start its
step span FROM this context rather than from a root of its own. A step runs in
a different process from the scheduler that dispatched it, so this map is the
only thing joining the two halves: an engine that ignores it still runs the
step correctly, and still emits perfectly good spans, but they land in a
second, disconnected trace and the run's timeline silently stops being
answerable. Nothing fails, which is what makes it worth writing down.

An engine that emits no telemetry ignores the field. It must not echo it back
on a `JobStatus`, invent one, or treat an absent one as an error: a control
plane with no tracing configured sends no trace context, and that is a normal
deployment rather than a fault.

The field is additive within the major version, like everything else here: an
engine built before it existed sees an unknown field, preserves it, and is
unaffected.

## Secrets

`JobDispatch.secrets` carries `SecretRef`s, never values. A dispatch is durable,
replayable, and archived; a secret value inside one would be a secret at rest in
the run history.

An engine redeems a handle for the value at the moment it needs it. Handles are
short-lived and single-use. An engine must not log a redeemed value, write it to
the object store, or include it in an error message.

An engine that does not advertise `CAPABILITY_SECRETS` will never be sent one.

## Logs

Two copies of a step's output exist, and they have different jobs.

`LogChunk` on `job.logs.<run>.<step>` is the **live** copy: ephemeral,
best-effort, for a GUI watching a run in progress. Chunks carry a monotonic
`seq` so a viewer can see that it missed some. Dropping these is acceptable.

The object written under `JobDispatch.output_prefix` and named by
`JobStatus.log_key` is the **authoritative** copy: complete, durable, and what
anyone reads after the run. An engine must write it even when nobody is
watching, and must finish writing it before publishing the terminal status.

## Heartbeats and orphans

An engine publishes `EngineHeartbeat` every five seconds listing everything it
is holding. A lease that stops being renewed expires, and the control plane
treats the step as orphaned and re-dispatches it under a new fence.

An engine that finds itself holding a job whose fence is no longer valid must
stop that job. It has been superseded, and its output would be discarded anyway.

## Control

`EngineControl` is the only message an engine receives besides dispatches.

- `Cancel` stops one in-flight job. The engine terminates the sandbox and
  publishes `JobStatus{PHASE_CANCELLED}`.
- `Drain` tells the engine to accept no new work and exit once idle, or once
  `deadline_seconds` elapses — whichever comes first. This is how a rolling
  upgrade proceeds without killing running steps.
- `Attach` asks the engine to serve an interactive session against a running
  job on the given ephemeral reply subject.
