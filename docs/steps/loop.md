# The `loop` step type

`plugin_ref: builtin:loop`

Iteration as a **bound, a condition, and a body spliced into the run**, with a
maximum iteration count. Implemented in `internal/steps/loop`, run by the
control plane in `internal/server/builtins.go`.

## The body is a fragment, spliced into the same run

`config.body` is a `dhole.v1.Pipeline` as JSON — the same document
`dhole pipeline create --definition` takes. Each iteration realises it into the
run's own graph through the generator machinery of `internal/dynamic`
([ADR 0022](../../.procoder/adr/0022-a-loop-body-is-spliced-into-its-own-run.md)),
so a body step is an ORDINARY step and may dispatch to an engine.

A body used to be one `builtin:` reference the plane ran itself, because the
definition format has no syntax for a subgraph and no run can contain another.
The obvious repair — a child run per iteration — is a change to what a run IS,
and every mechanism built on ADR 0003 assumes one run is one event log with one
terminal event.

```yaml
- id: refine
  plugin_ref: builtin:loop
  effect_class: EFFECT_CLASS_AT_MOST_ONCE
  config:
    max_iterations: "3"
    # `state` is keyed by the body step whose output it was.
    exit_condition: state.check.done
    body: |
      {"steps": [{
        "id": "check",
        "plugin_ref": "command:{\"args\":[\"/bin/sh\",\"-c\",\"...\"]}",
        "effect_class": "EFFECT_CLASS_IDEMPOTENT",
        "outputs": [{"name": "out", "type": {"blob": {"media_type": "application/json"}}}]
      }]}
```

## What an iteration is called

Step ids are unique within a run and `dynamic.Splice` refuses a duplicate by
name, so an iteration's steps carry the loop's id and the pass number: a body
step `build` inside loop `refine` is `refine.3.build` at the third pass, and
that is the name in the log and in the run view. It is uglier than a nested
run's naming would have been, and it is the price of one graph.

The controller that decides pass *n* is `refine.n` — the first is the AUTHORED
step, so a loop that never iterates grows no extra id. The fragment ENDS in the
next controller, wired after the body by a real edge: without that edge the
scheduler would run it beside the body and realise every iteration at once.
That is also why a body's exit step must declare an output — an edge needs a
port at both ends.

## The ceiling bounds the graph, not just the attempts

Since ADR 0022 `max_iterations` bounds **the size of the run's graph**: every
pass adds its body to the run. A loop stops emitting controllers when the
ceiling is reached, and behind that `internal/dynamic`'s run-wide expansion
ceiling bounds every generator and every loop in one run together, which is
what a loop inside a loop needs — per-loop ceilings multiply.

A loop that reaches its ceiling FAILS the controller step: it has not done what
it was asked, and an `EFFECT_CLASS_AT_MOST_ONCE` loop must not be retried into
spending its allowance again.

## What the exit condition is asked about

`state` is the previous pass's output, keyed by the body step that produced it:
a body ending in `check` is asked about `state.check`. A JSON object is that
object; anything else is its text. `iteration` and `max` are the pass that just
finished and the ceiling.

## Why not a cycle in the graph

An agent that thinks, acts, observes and goes round again is the one shape a DAG
cannot hold. The answer is not to allow a cycle: a cycle in the top-level graph
costs the cache its key derivation and the scheduler its topological order, and
both are load-bearing
([ADR 0001](../../.procoder/adr/0001-typed-content-addressed-dag-replaces-the-shared-mutable.md),
[ADR 0003](../../.procoder/adr/0003-runs-are-durable-and-event-sourced.md)).

So the graph above a loop stays acyclic and analysable, the subgraph is validated
on its own, and the realised-run view unrolls the container into the iterations
that actually happened.

## A loop is bounded at configuration

`MaxIterations` of zero is not "no limit configured yet", and a negative one is
not a limit. Both are the unbounded loop this package exists to prevent, and
both are refused where the mistake was made rather than discovered at three in
the morning by whoever is on call.

## An exit condition that errors stops the loop

An expression nobody can evaluate has not said "keep going". Carrying on would
turn a typo in a condition into a loop that runs to its ceiling every time — and
the ceiling is the last line of defence, not the design. `internal/policy` fails
closed for the same reason.

## The iteration budget is shared across a nest

This is how `loop.Loop` — the library form, which runs a body itself — bounds a
nest. The spliced form the plane runs shares one bound differently and for the
same reason: `internal/dynamic`'s expansion ceiling is counted per RUN off the
run's own log, so a loop realised inside a loop's body spends from the same
counter rather than multiplying ceilings.

A per-loop ceiling stops being a bound the moment loops nest: ten around ten is
a hundred body iterations, three levels is a thousand. The budget is therefore
**one counter**, carried on the context, seeded by the outermost loop and spent
by every loop inside it. The worst case of a nest is the seed, never the product
of the ceilings. A nested loop cannot lift it; it can only spend from it.

## Events

The values are stored verbatim and are a persistence contract: add new ones,
never rename these.

| Event                     | Means                                                         |
| ------------------------- | ------------------------------------------------------------- |
| `LOOP_ITERATION_STARTED`  | one pass was realised into the run                            |
| `LOOP_ITERATION_FINISHED` | one pass closed, carrying the state it produced               |
| `LOOP_EXITED`             | the good ending: the condition held                           |
| `LOOP_CEILING_REACHED`    | the bound doing its job                                       |
| `LOOP_FAILED`             | a body that failed, or a condition that could not be answered |

`LOOP_CEILING_REACHED` always carries the reason text `iteration ceiling
reached`, whether the loop ran out of its own iterations or the nest ran out of
shared budget, so a run view has one string to look for.

## Refusals

| Error                 | Means                                                                                |
| --------------------- | ------------------------------------------------------------------------------------ |
| `ErrUnbounded`        | no positive ceiling; refused at construction                                         |
| `ErrIterationCeiling` | the allowance ran out without the condition holding                                  |
| `ErrExitCondition`    | the condition does not compile, does not answer yes or no, or could not be evaluated |
| `ErrSubgraphInvalid`  | the body is not a valid pipeline in its own right                                    |

The body is validated **independently** of the graph containing the loop, which
is what keeps a cycle inside a body from being a cycle nobody checked.
