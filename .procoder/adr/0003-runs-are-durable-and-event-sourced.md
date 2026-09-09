# 0003 — Runs are durable and event-sourced

Status: accepted
Date: 2026-09-09

## Context

A CI run lasts minutes and can acceptably die with the process that owns it. An
automation run waits three days for an approval, sleeps until Tuesday, or blocks on an
inbound webhook. Because Dhole hosts both, run state cannot live in memory. The obvious
Go implementation — one goroutine per run, with the run's position held in its call stack
— makes a control-plane restart lose every in-flight run, and makes resumption impossible
in principle rather than merely unimplemented.

## Decision

A run is a state machine persisted per transition to an append-only event log. Goroutines
are ephemeral workers that advance a machine and can be lost at no cost. Recovery is
replay from the last acked position. Event application is idempotent, deduplicated on
`(run, step, attempt, sequence)`, because replay will re-present events that were already
applied. When the control plane scales out, consumers partition by run id so exactly one
instance advances a given run at a time.

The event log lives in our own store, not in the bus (see 0005). Storage sits behind an
interface: SQLite for single-node and homelab, Postgres for scale.

Rejected: goroutine-per-run with periodic checkpointing, which cannot represent a wait
that outlives the process. Rejected: using the JetStream stream as the log, which
conflates delivery retention with domain history.

## Consequences

Easier: long waits, human approval steps, and restart tolerance are properties of the
model rather than features. The realised-graph view, the audit trail, per-step
observability, and deterministic replay all fall out of the log for free.

Harder: every state transition must be modelled as an event and every handler written to
be idempotent, which is more discipline than mutating a struct. Debugging becomes reading
a log rather than reading a stack. Schema evolution of persisted events needs a
versioning story from the first migration.
