# What a trigger's bound inputs become

Common to all four trigger kinds: [`schedule`](schedule.md), [`http`](http.md),
[`git`](git.md) and [`completion`](completion.md).

A trigger does not start a pipeline. It supplies that pipeline's **declared**
inputs — the ports no edge feeds — and the run follows
([ADR 0007](../../.procoder/adr/0007-triggers-are-pluggable-and-bind-to-typed-pipeline-inputs.md)).
This page is what happens to those values after the event arrives.

## They travel into the run, in the run's own log

The values a trigger bound are recorded on the run's `RUN_CREATED` event,
alongside the pipeline id and the revision the run pinned:

```json
{
  "pipeline_id": "nightly-report",
  "revision_id": "rev_9f2c…",
  "started_by": "http:deploy-hook",
  "inputs": {
    "event": { "$dhole.taint": { "source": "http:deploy-hook", "value": "refs/heads/main" } }
  }
}
```

The run log, not a table beside it. A run's position lives in its event log and
nowhere else
([ADR 0003](../../.procoder/adr/0003-a-run-is-a-state-machine-over-a-persisted-event-log.md)),
so the log is the only thing that can still answer "what was this run started
with" after the plane that started it is gone — and a second channel would be a
second thing to replay and a second thing to fall out of step with it.

`WatchRun` streams that payload unchanged, so the values are already on the
contract; nothing new was added to the wire for them.

`started_by` is the trigger, as `<kind>:<id>`. It is empty for a run somebody
started through `StartRun`, which is the distinction you want when a run fires
at 4am and the first question is what put it there.

## A value that came from outside still says so

A tainted value is recorded **wrapped**, exactly as the trigger produced it. The
`$dhole.taint` mark is what records that the value crossed a boundary
([ADR 0015](../../.procoder/adr/0015-untrusted-data-is-tainted-at-the-boundary.md)),
and storing "just the data" would flatten a value an anonymous POST supplied
into one a step produced. See [policy authoring](../policy.md) for what may then
be done with it.

## A binding the pipeline refuses starts no run

Two refusals happen on the fire path, before any run exists:

- **The binding produced nothing.** A trigger that names inputs and supplies
  none of them is refused. Starting the run anyway would leave bare ports, and
  the failure would surface inside a step long after the event that should have
  filled them is gone.
- **The value does not fit the port.** Every fire is checked with
  `trigger.ValidateInputs` against the pipeline's **active revision** — the
  definition the run is about to pin, not the one the trigger was configured
  with. A port retyped after the trigger was wired up is caught here; so is a
  `schedule` binding, whose event fields are all strings, pointed at a port
  whose schema demands an object.

A refusal leaves no run, no revision pinned and no event. The reason is logged
against the trigger's own id, which is where somebody asking "why is my webhook
doing nothing" is already looking. A webhook caller additionally gets the reason
in the response body when the refusal happened at the endpoint.

There is no operator-supplied input path to be inconsistent with: `StartRun`
takes a pipeline and a revision and no values. An event source is therefore not
a way around a check that exists elsewhere — and when a hand-supplied input
path arrives, this is the function it shares.

## What this does not do yet

A step does not yet **read** these values at its input port. Carrying a
structured value onto a port means materialising it in the content-addressed
store and resolving it into the same `InputRef` an edge produces; until that
lands, the values are the run's record of what it was started with and what a
taint decision is made against, and a step reaches its predecessor's data by
connecting a port to it exactly as before.
