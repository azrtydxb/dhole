# The `approval` step type

The human gate in a run. Implemented in `internal/steps/approval`.

A run waiting for a person is the case
[ADR 0003](../../.procoder/adr/0003-runs-are-durable-and-event-sourced.md) was
written for: the person is asleep, the deploy that was going to ask them has
been restarted twice, and the run has to be exactly where they left it when they
wake up.

So the gate is an **event**, not a blocked goroutine. `STEP_AWAITING_APPROVAL`
sits in the run log, the scheduler refuses to move past it, and a decision
appends the events that lift it. A three-day wait costs nothing and survives
every restart in between.

## The approver is recorded, always

An approval whose approver is not in the log is not an approval — it is a run
that advanced itself. "Who approved this" is the only question ever asked of
this trail, so `STEP_APPROVAL_DECIDED` carries the subject, the verdict and the
time, separately from the step's own verdict: one says the run may move on, the
other says a named person said so.

The approver must be a principal of the tenant. However plausible the string, a
subject this tenant does not know is refused — the same subject in another
tenant is a different person.

## A second decision is refused, not swallowed

Approvals get double-clicked. An idempotent no-op would answer "done" to a
person clicking DENY on a gate somebody else had already approved: the two
decisions disagree, and the second decider would be told theirs took effect.

`ErrAlreadyDecided` names the standing decision instead. That stays true when
the second click is not the same as the first, and it gives the interface
something to show.

## Events and payloads

| Event                     | Payload   | Carries                                    |
| ------------------------- | --------- | ------------------------------------------ |
| `STEP_AWAITING_APPROVAL`  | `Request` | the prompt the person is being asked        |
| `STEP_APPROVAL_DECIDED`   | `Decision`| approver, approved, time                    |
| `RUN_FAILED`              | `Denial`  | steps, approver, step id, reason            |

`Denial` names its steps field exactly as `scheduler.RunFailure` does, so a
refusal reads as an ordinary run failure to the scheduler's own reader as well
as naming the approver. One payload with two readers beats two formats that
drift.

## Refusals

| Error                  | Means                                                  |
| ---------------------- | ------------------------------------------------------ |
| `ErrApproverRequired`  | a decision by nobody                                    |
| `ErrUnknownApprover`   | not a principal of this tenant                          |
| `ErrAlreadyDecided`    | the gate has a standing decision; the error names it    |
| `ErrNotAwaiting`       | nobody asked for this approval                          |

`ErrNotAwaiting` is the one that matters for safety: without it, a caller could
mark any step of any run succeeded.
