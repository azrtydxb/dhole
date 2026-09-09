# 0015 — Agent loops are bounded nodes and untrusted data is tainted

Status: accepted
Date: 2026-09-09

## Context

Dhole hosts LLM and agent steps as a first-class profile, which breaks two assumptions.
First, agent loops are inherently cyclic — think, act, observe, repeat — while the
execution model is a DAG whose acyclicity the cache and scheduler depend on. Second, once
an LLM step can read an untrusted webhook payload and influence which step runs next,
prompt injection becomes a remote code execution path into the engines. That is a
categorical change from CI's threat model, not an increment on it.

## Decision

Cycles: no cycles in the graph. Iteration is an explicit bounded loop node containing a
subgraph, with a maximum iteration count and an exit condition. The top-level graph stays
acyclic and analysable; the GUI renders the loop as a container and the realised-run view
unrolls it.

Untrusted data: data entering from an untrusted trigger is tainted at the boundary, the
taint propagates through typed ports, and tainted data reaching an effectful step or a
privileged engine is blocked unless an explicit sanitisation gate clears it.

Agent action space: an agent step may only invoke steps explicitly granted to it, and may
never invoke an `at-most-once` step without passing an approval gate.

LLM steps are `pure` only under a pinned model, temperature zero and explicit opt-in, and
their cache key includes a recorded model fingerprint rather than the alias requested,
because providers swap models behind stable names. Every call is recorded — prompt,
response, model, params, tokens, latency — for replay and diffing, and token cost is a
first-class metric with per-run and per-pipeline ceilings that hard-stop execution.

## Consequences

Easier: the taint model is only possible because ports are typed (0001) and effect classes
are declared (0002), so this is the payoff for that earlier work — and it is something
n8n and Zapier structurally cannot offer. Bounded loops keep scheduling and caching
analysable. Budget ceilings mean an agent-loop bug is not an invoice.

Harder: taint propagation must be implemented through every port, artifact and cache
lookup, and false positives will block legitimate pipelines until sanitisation gates are
easy to express. Bounded loops are less expressive than free recursion and some agent
patterns will feel constrained. Call recording is a meaningful storage cost and holds
prompt content, which has its own retention and privacy implications.
