# The `wait` step type

`plugin_ref: builtin:wait`

A step that waits. Reaching it arms a durable timer; the run resumes when that
timer comes due. Implemented in `internal/steps/gate` over `internal/wait`.

```yaml
- id: hold
  name: wait for the timer
  plugin_ref: builtin:wait
  effect_class: EFFECT_CLASS_PURE
  config:
    duration: 30s
```

## A wait is a row, not a goroutine

The run's position lives in its event log
([ADR 0003](../../.procoder/adr/0003-runs-are-durable-and-event-sourced.md)), so a
step that waits three days costs one row in `run_timers` and nothing else. The
process that armed the wait is routinely not the process that ends it: a control
plane can be stopped and replaced underneath an outstanding wait, and the run
resumes on whichever plane is polling when the time comes.

A `time.AfterFunc` would pass every test that never restarts anything and lose
every outstanding wait on the first deploy.

## The gate is armed by the decision that found the step ready

This is the reason the step type exists rather than a helper somebody calls.

A wait used to be armed by whoever started the run, in a transaction of its own,
while the scheduler decided readiness in another. A sequence number is allocated
inside a transaction and its **visibility is not**, so the `STEP_AWAITING_TIMER`
event could hold a lower sequence than the `STEP_DISPATCHED` of the very step it
gated: the log read "gated, then dispatched anyway", and the wait had been
skipped entirely. Both gated acceptance pipelines worked around it by arming the
gate behind a five-second predecessor, which makes the window improbable rather
than closed.

Two things close it:

- **Gatedness is a fact about the definition.** The scheduler knows this step
  waits because its `plugin_ref` says so, and the pinned revision said so before
  the run existed. There is no commit that can arrive too late to be seen.
- **The arming runs in the transaction that would have dispatched.** No lease is
  claimed, no engine is matched, no outbox row is enqueued, and nothing about the
  step reaches the bus.

Two advances that both find one gate ready both arm it, and migration
`0023_gate_armed_once` makes the second append a no-op — the same shape
migrations 0020 and 0021 settled on for a run's terminal event and the two step
verdicts.

## Duration, not a deadline

`duration` is a Go duration (`30s`, `72h`) and is relative to **reaching** the
gate. An absolute instant in a definition would already have passed the second
time the pipeline ran. Zero is not "no wait configured yet" and a negative one is
not a wait; both are refused where the mistake was made.

## A gate produces no artifact

Firing appends `STEP_TIMER_FIRED` and the step's own `STEP_SUCCEEDED` with no
outputs. An edge out of a wait step exists to order the graph, not to carry data.

## Events

The values are stored verbatim and are a persistence contract: add new ones,
never rename these.

| Event                 | Means                                                         |
| --------------------- | ------------------------------------------------------------- |
| `STEP_AWAITING_TIMER` | the gate is armed, with the due time it was armed for         |
| `STEP_TIMER_FIRED`    | the wait ended, carrying both the due time and the fired time |

A run holding an armed gate is neither running nor finished: the scheduler
counts it as waiting, so the run is not completed out from under it.

## Refusals

| Error            | Means                                            |
| ---------------- | ------------------------------------------------ |
| `ErrNoDuration`  | the step declares no `duration` at all           |
| `ErrBadDuration` | the `duration` is unparseable, zero, or negative |
