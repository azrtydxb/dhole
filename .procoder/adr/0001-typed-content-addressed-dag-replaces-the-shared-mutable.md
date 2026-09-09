# 0001 — Typed content-addressed DAG replaces the shared mutable workspace

Status: accepted
Date: 2026-09-09

## Context

Every mainstream pipeline tool in the GitLab CI / Drone / Woodpecker lineage models a
pipeline as an ordered list of steps sharing one mutable workspace volume. Step 3 sees
whatever steps 1 and 2 left on disk. That single choice is the root cause of four
separate problems we would otherwise have to solve individually: a step whose inputs are
"the whole filesystem, mutated by unspecified predecessors" cannot be cached, cannot be
safely parallelised, cannot be reproduced, and cannot be executed locally because the
volume state is unreconstructable. The cache plugins those tools ship — tar a directory
to S3 under a key you invent yourself — are a workaround for the model, not a feature.

## Decision

A step is `(environment identity, command, declared input digests) -> declared outputs`.
Steps declare their inputs and outputs explicitly; the DAG is derived from those
declarations rather than authored as an ordering. Wires between steps are typed artifact
edges connecting an output port to an input port, and execution order falls out of the
data dependencies.

Rejected: keeping the shared workspace and layering a cache on top (what Woodpecker
does) — the cache is then advisory and wrong at the edges. Also rejected: an explicit
`depends_on` ordering DAG without declared data flow, which buys parallelism but not
caching or reproducibility.

## Consequences

Easier: caching becomes correct by construction (see 0009); parallelism is free wherever
no dependency edge exists; local execution is possible because a step's inputs are
enumerable; drag-and-drop editing becomes meaningful because connecting two ports is
type-checked at edit time rather than failing at minute eight of a run.

Harder: every step author must declare inputs and outputs, which is more work than
"it's all just in the working directory" and will be the most common source of friction
when porting existing pipelines. Steps that genuinely need ambient filesystem state need
an explicit escape hatch, and that hatch must be marked non-cacheable.
