# The `schedule` trigger

`kind: schedule` — fires on a cron expression. `internal/trigger/schedule`.

It is the simplest event source there is and the one that exposes every hard
problem in the subsystem, so its shape is the shape the others follow
([ADR 0007](../../.procoder/adr/0007-triggers-are-pluggable-and-bind-to-typed-pipeline-inputs.md)).

A trigger does not start a pipeline. It supplies that pipeline's declared
inputs, and the run follows — so the same definition is reachable from a cron
boundary, a webhook, an API call, a person or an agent with no change to it.

## Configuration

| Field        | Meaning                                                    |
| ------------ | ---------------------------------------------------------- |
| `ID`         | the schedule's identity within the tenant, and its row key |
| `TenantID`   | the tenant every fire lands in                             |
| `Expression` | a cron expression, five or six fields                      |
| `Binding`    | which pipeline, and which event field fills which input    |
| `Pipeline`   | the definition the binding is checked against; required    |

Two processes running the same `ID` are two control planes running one schedule.
That is the point, and it is what the claim discipline below is for.

## Event fields a binding may read

`scheduled_for`, `fired_at`, `expression`, `trigger_id`, `kind`.

Bindings are checked at **configuration time**: a binding that reads `commit_sha`
from a cron trigger is a mistake to catch while somebody is wiring it up, not at
3am inside the first step of a run that should never have been dispatched.

## The three rules, each a production incident somewhere else

**A schedule is a row holding the next due time, not a queue of intervals.**
Coming back after three hours of downtime fires the missed window **once**,
because firing advances the due time past everything already elapsed. The
catch-up loop this refuses is how a minutely job runs 180 times at once at the
exact moment a system is least able to take it.

**An occurrence is claimed before it is fired**, under the same discipline the
wait timers and the outbox use: `FOR UPDATE SKIP LOCKED` on Postgres, the
immediate write lock on SQLite, with the due time advanced inside the claim. Two
control planes poll the same row every second, and an occurrence handed to both
starts the pipeline twice.

**An occurrence arriving while the last is still in flight is skipped**, with a
reason, and the schedule moves on. A queue growing without bound behind a slow
run is the other way a minutely schedule takes a system down.

## Timing you should expect

`DefaultPollInterval` is one second, so a cron boundary is served to about a
second. That is the trade the durable row buys: a schedule that survives a
restart is worth more than one that fires to the millisecond and is lost on the
next deploy.

`DefaultActiveLease` is an hour — how long an in-flight fire is believed. Past
it the mark is treated as stale, because the plane that set it may have died
mid-fire, and a schedule that never fires again is worse than one that overlaps.
