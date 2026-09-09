# The `loop` step type

`plugin_ref: builtin:loop`

Iteration as a **node containing a subgraph**, with a maximum iteration count and
an exit condition. Implemented in `internal/steps/loop`.

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
| `LOOP_ITERATION_STARTED`  | one pass opens, under its own unrolled step id                |
| `LOOP_ITERATION_FINISHED` | one pass closes, carrying the state it produced               |
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
