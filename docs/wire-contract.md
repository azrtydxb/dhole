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

| Subject                        | Direction        | Message                          |
| ------------------------------ | ---------------- | -------------------------------- |
| `job.dispatch.<tier>.<caps>`   | plane → engine   | `JobDispatch`                    |
| `job.status.<run>.<step>`      | engine → plane   | `JobStatus`                      |
| `job.logs.<run>.<step>`        | engine → viewers | `LogChunk`                       |
| `engine.control.<engine-id>`   | plane → engine   | `EngineControl`                  |
| `engine.heartbeat.<engine-id>` | engine → plane   | `EngineHeartbeat`                |
| `engine.registration`          | engine → plane   | `EngineRegistration`             |
| `secret.redeem`                | engine → plane   | a handle, raw (reply: the value) |

`<tier>` is the trust tier the work is dispatched to — `trusted`, `untrusted`,
and whatever else the deployment defines. An engine's bus credentials permit
subscribing to its own tier only. This is enforced by the bus, not by the
control plane's good manners: an engine in the untrusted tier that subscribes to
`job.dispatch.trusted.*` receives a permissions error.

`<caps>` is a stable hash of the sorted capability set a step requires. An
engine subscribes to the dispatch subjects matching capability sets it can
satisfy, so filtering happens at the bus rather than by receiving and rejecting
work it was never eligible for.

### Message framing

The two engine-to-plane subjects carry an `EngineMessage`, not a bare payload:

```proto
message EngineMessage {
  oneof body {
    EngineRegistration registration = 100;
    EngineHeartbeat heartbeat = 101;
  }
}
```

An engine publishes `EngineMessage{registration}` on `engine.registration` and
`EngineMessage{heartbeat}` on `engine.heartbeat.<engine-id>`. Everything else
in this document — `JobDispatch`, `JobStatus`, `LogChunk`, `EngineControl` — is
published bare, because each of those subjects carries exactly one type and
that type is not confusable with another.

These two are. **An `EngineHeartbeat` decodes cleanly as an
`EngineRegistration`**: both begin with `engine_id`, and protobuf cannot tell a
packed `repeated uint32` from a `repeated message` on the wire. A control
plane that decided by content therefore admitted engines advertising no
platform and no capabilities, and every step after that was unschedulable with
nothing in any log saying why. The subject can distinguish them, but a subject
is a routing decision — it can be forwarded, bridged, renamed, or mapped by an
account import — and the type of a message must not depend on how it was
delivered. The frame puts the type in the bytes.

The body's field numbers start at 100, above every number either payload uses,
and that is part of the contract rather than an accident of drafting. It means
a bare payload parses as a frame with an UNSET body rather than as a frame with
a garbled one. So:

- A frame with a body is an engine speaking this framing, and its type comes
  from the oneof.
- A frame with no body is an engine speaking the earlier framing, and its type
  comes from the subject. The control plane accepts it, because it accepts
  engines one version behind.
- A frame read by a plane that predates the framing yields an empty
  `engine_id`, which is refused outright rather than admitted as a plausible
  instance.

Engines being written now must frame. An engine that publishes bare payloads is
relying on the compatibility path and will stop working when this major version
does.

### Computing `<caps>`

This is the one value an engine cannot guess, and guessing it fails **silently**:
the engine subscribes to a subject nothing is published on, so it sees no work,
no error, and no indication that anything is wrong. The control plane meanwhile
believes the capability set has no engine.

Given the capability set a step requires:

1. Drop `CAPABILITY_UNSPECIFIED`.
2. De-duplicate, and sort ascending by **enum number**.
3. For each remaining capability, write its enum number in decimal followed by a
   single `\n` — the NUMBER, not the name, because that is what the wire
   carries and it is the same in every language.
4. SHA-256 those bytes.
5. Lowercase-hex the digest and take the **first 16 characters**.

An empty set hashes the empty input: `e3b0c44298fc1c14`.

An engine subscribes to one dispatch subject per capability set it can satisfy —
that is, per SUBSET of what it advertises, including the empty one — because a
step requiring nothing must reach an engine that offers everything.

### JetStream mechanics

`job.dispatch.*` is carried by a work-queue stream named `DISPATCH`. An engine
binds a durable pull consumer filtered to its own dispatch subject, and
acknowledges by publishing to the message's reply subject.

A pull delivers under the **original subject**, not the reply inbox. An engine
that routes by the subject it subscribed with will drop every dispatch while the
server believes it is working on them.

`max_ack_pending` on that consumer must be at least the engine's slot count. Set
lower, one stuck job stalls the whole queue for that capability set — including
work other engines could have taken.

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

A dispatch is addressed to a TIER, not to an engine — the work queue decides
which member takes it — so the control plane writes one at the highest version
**every** engine that could take it speaks, which is not necessarily its own.
One engine a version behind holds its tier at that version until it is
upgraded. That is what makes the window usable: a plane that stamped its own
maximum on every dispatch would fail every step on every engine it had just
admitted for being one version behind.

Within a major version, schema changes are additive only. Fields are never
renumbered, never removed, and never change meaning.

## Environment identity

`EngineRegistration.environment_identity` is the digest of the environment the
engine runs steps in: a resolved sandbox image digest, a VM snapshot id.
Everything the step cache is keyed on comes from the pipeline except this, and
this is the one thing the control plane cannot see for itself — on a
distributed deployment the environment is on another machine entirely, and a
plane that answered the question from its own configuration answered it wrong
in every deployment anyone runs (ADR 0021).

Three obligations, and they are the whole contract:

- **Stable** for identical environments. Two engines running the same image
  report the same string, on every restart, in any order.
- **Different** for different environments. Anything a step's result could
  depend on which is not already in the cache key must change it. An image
  digest satisfies this; an image TAG does not.
- **Absent** rather than invented. An engine with nothing reproducible to name
  — a host process engine, which runs against whatever the host happens to
  carry — leaves the field empty. It must never substitute a hostname, a
  start-up timestamp, a version string or a constant.

The plane hashes cache keys against the identity of the **tier**, agreed by its
members. A tier caches nothing at all when its engines disagree, when any
member reports none, or when no member has registered yet. Disagreement is a
misconfiguration — usually a half-finished rollout of two different sandbox
images — and the plane logs it rather than degrading quietly. It is not an
error and nothing is refused: the tier simply runs every step for real until it
agrees again.

An engine that omits the field is treated as having none, which turns its
tier's cache off rather than poisoning it. That is what makes the field
additive: an engine written against version 1 registers without one, works, and
does not cache.

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

### Who advertises `CAPABILITY_SECRETS`

An engine that does not advertise `CAPABILITY_SECRETS` will never be sent one,
because the capability is part of the `<caps>` hash the dispatch subject is
named for.

The capability means "may redeem secret references", and it belongs to the
ENGINE, not to its sandbox backend. `NETWORK`, `PRIVILEGED` and `HOST_MOUNT` are
isolation guarantees only the backend can make or decline; redemption happens in
the agent, over the bus it dialled, before any sandbox exists. An engine
advertises `CAPABILITY_SECRETS` exactly when it has a redemption endpoint, and
refuses a dispatch carrying a secret when it does not.

Sourcing it from the backend instead is a mistake that hides: no honest sandbox
backend advertises it — a bare process cannot promise anything, and a container
grants what its template grants — so every engine refused every step carrying a
secret, always, and nothing in the product redeemed anything. Teaching one
backend to advertise it is the same mistake with the answer inverted.

### The redemption exchange

Request/reply on `secret.redeem`, and both bodies are RAW BYTES rather than
protobuf messages:

- The **request** body is the `SecretRef.handle`, UTF-8, and nothing else.
- The **reply** body is the value's bytes, and nothing else — unless it begins
  with the four ASCII bytes `ERR `, in which case it is a refusal and the rest
  is a reason.

There is no message type because the value is opaque bytes and every field of a
wrapper would be one more copy of it: in a decoder's arena, in a reflection
path, in anything that logs an undecodable message by dumping what it got. The
reply carries the value alone.

The `ERR ` prefix costs one thing, and it is stated here rather than discovered:
a VALUE whose bytes begin with `ERR ` cannot be told from a refusal. An issuer
must therefore refuse to ISSUE such a value — at issue time, where a person can
see it, rather than at redemption, where an engine would fail a step it could
have run. Dhole's own broker refuses it.

Rules that bind both ends:

- **The control plane serves it.** A reference nothing can redeem is not a
  feature. The plane answers on this subject for as long as it is running.
- **Single use.** The second redemption of a handle is refused, whatever the
  outcome of the first. A dispatch redelivered after a lost ack must not be able
  to read a value the earlier attempt already took.
- **The issuer enforces `expires_at`.** Only the issuer knows when it issued. An
  engine may pre-check the field but must not rely on it.
- **A refusal names neither the handle nor the value.** It travels back over the
  bus and an engine puts it in a `JobStatus` error, which is durable and
  archived.
- **An engine bounds the request.** One that waited forever on a plane that is
  not answering holds a slot and a lease until the lease expires, and the step
  is re-dispatched to an engine that waits forever in the same way. Dhole's
  engines wait ten seconds.
- **A redeemed value reaches the step's process and nothing else.** Never a
  `LogChunk`, never the authoritative log, never an `OutputRef`, never a
  `JobStatus` error — and never a sandbox specification a backend might persist,
  which is why Dhole's engine binds secrets to the exec environment and not to
  the environment it acquires the sandbox with: the Kubernetes backend turns
  that into a pod template the API server keeps.
- **An engine that cannot redeem fails the step**, with an error naming the
  BINDING — the environment variable the step expected — and never the handle.

The subject is a deployment's to move: an engine that is told a different one
uses that instead. Dhole's engine reads `DHOLE_SECRET_SUBJECT` and falls back to
`secret.redeem`.

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

**An engine re-announces itself every third heartbeat** — every fifteen seconds
— by publishing `EngineRegistration` again, unchanged. This is not optional.

`EngineRegistration` is fire-and-forget on a core subject, so a registration
published while the control plane was down is simply gone, and a heartbeat
cannot replace it: a heartbeat carries only an engine id and an in-flight list,
so a plane rebuilding an engine from one would have a fleet member advertising
no capabilities, no platform and no slots, which can never be matched to any
step. The plane therefore refuses to resurrect an engine from a heartbeat, and
the re-announce is how an engine that started first, or outlived a restart,
becomes visible again.

An engine that registers once and never again works perfectly until the first
time the plane restarts, and is then invisible until the engine itself is
restarted — with nothing anywhere reporting a fault.

A re-announcement updates what an engine IS and never what it has PROVEN. An
engine that is already ready stays ready across it, and one that is draining
stays draining; only an engine the plane does not hold, or holds as gone, has
to prove liveness with a heartbeat again. A plane that reset the lifecycle on
every re-announcement dropped every healthy engine out of the dispatchable
fleet three times a minute, which showed up as steps recorded unschedulable
against a warm idle fleet.

## Control

`EngineControl` is the only message an engine receives besides dispatches.

- `Cancel` stops one in-flight job. The engine terminates the sandbox and
  publishes `JobStatus{PHASE_CANCELLED}`.
- `Drain` tells the engine to accept no new work and exit once idle, or once
  `deadline_seconds` elapses — whichever comes first. This is how a rolling
  upgrade proceeds without killing running steps.
- `Attach` asks the engine to serve an interactive session against a running
  job on the given ephemeral reply subject.

## Exit codes

`JobStatus.exit_code` is the step's exit status, and it decides the phase: zero
is `PHASE_SUCCEEDED`, anything else is `PHASE_FAILED`. Three rules bind every
engine.

**Never negative.** A negative int32 sign-extends to ten bytes on the wire, and
a varint decoder that stops early never terminates on one. An engine with no
status of its own to report — a platform that gives it nothing — reports a
non-zero positive code, not -1.

**A step the engine killed reports 137.** Cancellation, a `Cancel` control
message, a step timeout, a drain that ran out of patience: all of them report
137 on every platform and every backend. On unix that is the POSIX 128+SIGKILL;
on Windows the job object is deliberately terminated with the same number, so
one code means one thing wherever the step ran. A step killed by some other
signal follows the same convention: SIGTERM is 143, SIGINT is 130.

**A step that ran out of memory is never a success.** This is the one an engine
author cannot guess, because the platforms genuinely differ — there is no
portable "OOM exit code", and an engine that reported 0 because it could not
tell would have its result cached and shipped. What Dhole's own engines report:

| Platform / backend                      | What memory exhaustion does                                                                                                                                                                                                                                            | `exit_code`                                                       |
| --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| Linux, process engine                   | The kernel's OOM killer picks a victim and sends SIGKILL.                                                                                                                                                                                                              | `137`                                                             |
| Linux, container and Kubernetes engines | The cgroup's memory limit is hit; the container is `OOMKilled`, which is SIGKILL.                                                                                                                                                                                      | `137`                                                             |
| macOS                                   | There is no OOM killer for an ordinary process. The allocation is refused and the program fails on its own terms — the Go runtime aborts with `2`, CPython raises `MemoryError` and exits `1`. Only under system-wide pressure does Jetsam step in, with SIGKILL.      | The program's own non-zero status, or `137` when Jetsam killed it |
| Windows                                 | There is no OOM killer either. The commit limit refuses the allocation and the program fails on its own terms; a hard abort raises `STATUS_NO_MEMORY` (`0xC0000017`), which is far wider than an exit byte and is clamped rather than truncated into a different code. | The program's own non-zero status, or `255`                       |

The clamp is general: an exit status above 255 is reported as 255, so a
Windows NTSTATUS cannot wrap into a small number that reads as an ordinary
failure — or, worse, as a success.

One platform limitation belongs here because it changes what a step can rely
on: **Windows has no signal a running step can handle**. `SIGTERM` and `SIGINT`
from `EngineControl` terminate the job object immediately, exactly as `SIGKILL`
does. A step that needs to flush state before it dies cannot be written to do
so on Windows.

## What this document does not yet specify

A conformance suite (`make conformance`) runs an engine written in Python
against every obligation here. Writing it found these gaps: each is something
the reference Go engine does that this document does not say, so a stranger
cannot implement it. They are listed rather than hidden, and the conformance
suite's own choices are named so a second implementer makes the same ones.

- **The object store protocol.** `JobDispatch.output_prefix` and
  `JobStatus.log_key` name objects in a store this document never describes:
  no protocol, no addressing, no credentials. The conformance suite uses a
  directory named by `DHOLE_BLOB_DIR`. It also has to guess at the SHAPE of a
  key: Dhole's own stores scope every object by tenant structurally, so their
  keys resolve under `<tenant>/<key>`, and the suite now tries that before the
  flat path the reference Python engine writes. Neither is the contract, and a
  third engine will guess a third way until this is written down. The
  content-addressed layout is unspecified in the same way — Dhole writes
  `<algo>/<first two hex>/<hex>`, the suite also accepts `cas/<algo>/<hex>`.
- **Step timeouts.** Neither `JobDispatch` nor `Step` carries one, so the
  obligation to enforce a timeout cannot be met from the schema. The conformance
  suite passes `DHOLE_STEP_TIMEOUT_SECONDS` in `JobDispatch.env`.
- **Engine configuration.** Bus URL, engine id, tier and slot count are not
  described, so an engine cannot be started from this document alone.
- **Port layout on disk.** For a step executed as a process, nothing says where
  an input port's bytes appear or where an output port's are read from. The
  conformance suite uses `inputs/<port>` and `outputs/<port>`.
- **Fence ordering.** Fence tokens are described as opaque, and the control
  plane compares them by age. Ordering an opaque string is undefined; an engine
  only ever needs equality, and this document should say so explicitly.
- **The engine's half of the fence rule.** Nothing states what an engine does
  with a `Cancel` whose fence is not the one it holds. It must refuse it.
- **`PHASE_RUNNING`.** Defined in the schema; the message flow never says when
  to send it.

One implementation detail worth writing down here because it bit the Python
engine: a step killed by a signal has no exit status of its own, and an engine
that passes the platform's -1 straight through publishes a NEGATIVE exit code,
which protobuf sign-extends to ten bytes. A varint decoder that stops early
never terminates on one. Exit codes above says what to report instead.
