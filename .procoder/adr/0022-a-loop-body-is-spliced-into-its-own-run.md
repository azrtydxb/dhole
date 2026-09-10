# 0022 — A loop body is spliced into its own run

Status: accepted
Date: 2026-09-10

## Context

`builtin:loop` bounds an iteration count and evaluates an exit condition, and its body is
one `builtin:` reference in `config.body`. Anything larger than a single plane-hosted step
cannot be a loop body at all: the definition format has no syntax for a subgraph, and a body
that dispatches work to engines has nowhere to put it.

The obvious reading is that a body should be a pipeline and an iteration should start a
child run the parent waits on. That is a change to what a run IS. ADR 0003 makes a run a
state machine driven by one persisted event log, and every mechanism built on it assumes
that: the terminal-event index (migration 0020) is unique per run, the fair queue charges a
tenant's share per run, `open_runs` is the advance loop's whole worklist, and the SSE stream
ends a viewer's connection on the run's terminal event. Nested runs would mean a parent that
is neither running nor finished while a child works, a terminal event that does not mean the
end, and a viewer who must be told to follow a second stream.

Meanwhile the machinery for growing a graph at runtime already exists and is already
correct. `internal/dynamic` realises a fragment and records it in the event log, and the
scheduler splices every recorded fragment into the definition before building the DAG — so a
restarted plane reconstructs exactly the graph it had, because the graph comes out of the
log or it does not exist.

## Decision

A loop iteration splices its body into the SAME run, through the generator machinery.

`config.body` becomes a fragment rather than a single reference. Each iteration realises it
with the iteration's own step-id prefix, records `GENERATOR_FRAGMENT_REALISED` as any
generator does, and the scheduler picks the steps up on its next pass. The loop step itself
stays what it is: a bound, a condition, and a decision to realise one more iteration or
stop.

One run is still one graph, one event log and one terminal event. A loop body may therefore
dispatch to engines, because a spliced step is an ordinary step.

## Consequences

Everything already true of a spliced fragment becomes true of a loop body for free: replay
determinism, the cache, policy, the fair queue, and the run view drawing what actually ran.
A person watching a loop sees its iterations as steps, which is what they are.

A run's graph now grows while it executes, and grows repeatedly rather than once. The DAG is
rebuilt per advance, which it already was; what changes is that the number of fragments is
bounded by the loop's ceiling rather than by one. That ceiling is load-bearing in a way it
was not before — it now bounds the size of the graph, not merely the number of attempts —
and `max_iterations` stays required.

Step ids must be unique within the run, so an iteration's steps are prefixed with the loop's
id and the iteration number. A body that names a step `build` becomes `mybuild.3.build`, and
that name is what appears in the log and the run view. It is uglier than a nested run's
naming would have been, and it is the price of one graph.

A body that wants to run a whole existing PIPELINE is still not expressible, and
deliberately: that is a pipeline triggering another pipeline, which is what triggers are for.
