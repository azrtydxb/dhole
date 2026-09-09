# 0016 — v1 is three acceptance pipelines built in parallel

Status: accepted
Date: 2026-09-09

## Context

Dhole claims three profiles — CI, infrastructure and homelab automation, and LLM/agent
orchestration — over one core. A v1 needs a definition of done that proves the core rather
than the surface. The options were one spanning pipeline exercising all three, three
per-profile acceptance pipelines built in sequence, or three built together.

## Decision

Three acceptance pipelines, one per profile, developed in parallel rather than in
sequence. Each must run end to end for v1 to be done:

- CI — a container build that demonstrably hits the cache on a second run with unchanged
  inputs.
- Infrastructure and homelab automation — schedule and API triggers, a long wait, and
  execution across more than one engine type.
- LLM and agent orchestration — a schema-validated structured output, a bounded loop, a
  human approval gate, and recorded token cost.

## Consequences

Easier: the core gets feedback from three different directions early, which is the
fastest way to discover that a primitive is wrong while it is still cheap to change.
Nothing is designed against a single profile's assumptions.

Harder: parallel development means a core change ripples through three surfaces at once,
so churn is higher and the schema and wire contract (0004, 0013) become the critical path
— they need to be right early rather than iterated into shape, because three consumers
depend on them simultaneously. v1 is larger than any single-profile scope would have been,
and it carries the content-addressed cache (0009) at the same time.
