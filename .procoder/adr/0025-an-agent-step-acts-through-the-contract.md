# 0025 — An agent step acts through the contract

Status: accepted
Date: 2026-09-10

## Context

`internal/steps/agent` has the taint tracking and the per-action approval gate, and no way
to take an action: an agent step needs an invoker for the actions it may perform, and
nothing supplies one. The subsystem is a library with tests and no caller, which is the
shape of defect this project has spent a week removing.

The question is not how to call an action. It is what an agent is ALLOWED to do, and that is
a trust boundary rather than an interface. An agent's inputs are model output — untrusted by
construction (ADR 0015 taints them) — so whatever it can reach, a prompt can reach.

A general tool-invoking agent, able to run arbitrary commands in a sandbox, is the largest
possible answer and puts an untrusted planner in front of the executor. That is a product in
its own right, and it needs the strong-isolation tier to be safe, which is only now being
built (S-22).

## Decision

An agent step acts only through Dhole's own public API, as a principal of its tenant.

The action space is the contract: start a run, read a run, decide an approval gate, apply an
operation to a pipeline. Nothing else. The agent authenticates like any other client, its
calls are subject to the same policy, quota and audit as a human's, and it holds no
capability a person with the same token would not have.

Concretely: the agent's invoker is a Connect client against the plane's own API, carrying a
credential minted for the agent's subject, and every action it takes is already covered by
`TestEveryRPCRejectsACallWithoutAuthorization` and by the policy engine, because it is an
ordinary call.

## Consequences

An agent cannot do anything a token cannot do, which makes the blast radius of a prompt
injection exactly the blast radius of that token — a bounded, auditable, revocable thing,
and one an operator already knows how to reason about. The per-action approval gate becomes
meaningful rather than theoretical: it gates real calls.

Every action an agent takes appears in `policy_audit` and in the run log under the agent's
own subject, so "what did it do" has an answer that does not depend on trusting the agent's
own account of itself.

The agent cannot run a command, and that is the deliberate limit. A pipeline that wants an
agent to run something expresses it as a pipeline the agent STARTS — which is a step in a
graph, in a sandbox, under an effect class, cached and retried like everything else. The
agent decides what to run; it does not become the thing that runs it.

Taint follows the credential rather than the value here: an agent's token is marked
untrusted, so the policy engine can refuse an at-most-once effect or an unsigned plugin to
an agent while allowing it to a person, without every call carrying provenance.

If a tool-invoking agent is wanted later it supersedes this record rather than extending it,
and it should wait for the strong-isolation tier to be real.
