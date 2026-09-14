# 0029 — An engine confirms its fence before it starts a step

Status: accepted
Date: 2026-09-14

## Context

ADR 0004 made fencing the answer to duplicate delivery, and ADR 0002 required an
`at-most-once` step to hold an exclusive lease claimed before execution. What was built
enforces the fence only on the way BACK: an engine starts whatever it pulls off the work
queue, echoes the fence on its statuses, and the plane discards a stale report. The result
of a superseded attempt is thrown away, but the attempt itself still runs.

On kw, 2026-09-14, a step's attempt was recorded `STEP_ATTEMPT_LOST` and re-dispatched, and
the engine then started the superseded dispatch as well: a third sandbox pod ran `build`
for nobody for about five minutes, holding a slot. The cause that day was a plane-side race,
but the engine's behaviour is the same after any genuine loss — a dispatch redelivered after
the engine that accepted it died is still on the queue under a fence that is no longer
current. For a `pure` step that is waste. For an `at-most-once` step the external effect
happens twice, and discarding the second report afterwards does not un-send anything.

Nothing on the wire could tell an engine. Statuses go into a durable stream the plane reads
at its own pace, heartbeats are fire-and-forget, and the one existing signal — "an engine
that finds itself holding a job whose fence is no longer valid must stop that job" — named
an obligation with no way to find out.

## Decision

A plane that can answer says so in the dispatch: `JobDispatch.confirm_acceptance`. An engine
handed such a dispatch asks, on `job.accept.<run>.<step>`, before it starts anything — a
core request/reply carrying the `JobStatus{PHASE_ACCEPTED}` it is about to publish — and the
plane answers `AcceptReply` with `ACCEPTANCE_CURRENT` (the lease is renewed, which accepts
it, and the engine starts) or `ACCEPTANCE_FENCED` (a newer attempt holds the step, or its
lease is gone). A fenced dispatch is acknowledged off the queue, publishes no status and is
never started; its slot is given back at once.

When no answer comes — no plane serving, a timeout, a plane that could not read its leases
— the engine starts a `pure` or `idempotent` step anyway and keeps asking for an
`at-most-once` one, holding the delivery, until an answer arrives. The fence on the way back
still protects the first two; only the third has an effect that cannot be taken back, and it
is the class ADR 0002 already required to hold its lease before executing.

A running attempt whose lease is superseded is stopped by the plane: a heartbeat naming a
fence the lease manager refuses is answered with `EngineControl{Cancel}` for that attempt,
under that fence, on the engine's control subject — the same message and the same engine
path an operator's cancel uses — so it lands within one heartbeat interval.

Rejected: waiting for the plane's handling of the durable `PHASE_ACCEPTED` status. The
status consumer applies terminal statuses and advances runs on the same queue, so its
latency is the backlog's; a bounded wait for it times out exactly under load, which is when
losses happen. Rejected: sending the heartbeat as a request. An older plane subscribes to
the heartbeat subject and never replies, so every step on every new engine would wait out
the bound. Rejected: an engine asking unconditionally. Against an older plane its request
either meets no responder or a permission the older plane never granted, and the second
costs the whole timeout on every step; the dispatch flag means an engine only asks a plane
that wrote it.

## Consequences

A superseded dispatch no longer runs, whether it was superseded by a race or by a genuine
loss, and a running one is stopped within a heartbeat instead of running to completion.

The wire gains one subject, one dispatch field and one message, all additive. An engine that
predates them never asks, and is still stopped mid-run by the plane's cancel, which it
already obeys. A new engine against an older plane never sees the flag and behaves exactly
as before. No protocol version is bumped: the plane decides nothing based on whether an
engine asks, and the flag, not the version, tells an engine whether anybody will answer.

An engine now calls the plane before it starts a step, which ADR 0004 described engines as
never needing to do once dispatched. What to run is still entirely in the dispatch; whether
it may still run is the one question the dispatch cannot answer, because the answer changes
after it was written. With the plane down, `pure` and `idempotent` work proceeds as before
and `at-most-once` work waits for it — a queued `at-most-once` step is pending work, which
ADR 0004 already says queues while the plane is down.

A slot is held for the length of one round trip before a refused dispatch gives it back.
Asking before taking a slot would accept the lease while the dispatch still waits for one,
and a lease accepted by a step nobody lists in a heartbeat is declared lost one TTL later.
