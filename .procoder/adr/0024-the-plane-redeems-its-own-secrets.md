# 0024 — The plane redeems its own secrets

Status: accepted
Date: 2026-09-10

## Context

`builtin:llm` needs a model client, and a model client holds an API key. Nothing in this
system leases the control plane a secret. ADR 0010 gives engines short-lived references they
redeem and never values, and the broker that issues those handles lives on the plane — so
the plane is the one component with no way to obtain a credential except to be handed one at
start-up. A plane with no model factory fails an LLM step with exactly that reason rather
than pretending, which is honest and useless.

The tempting answer is a configuration field: put the key in `server.Config`, sourced from
an environment variable or a mounted Secret. It works, and it makes the plane the one place
in Dhole where a credential sits at rest in a process for the lifetime of the deployment —
the precise thing SecretRef exists to avoid, reintroduced at the centre rather than the edge.

The other tempting answer is to move the model call to an engine step type, so the
credential is leased the way every other secret is. That is right for a step that runs
somebody's code. It is wrong here: a model call is bounded by the plane on purpose (an
iteration ceiling, a schema, a token budget), and handing it to a tenant-supplied engine
hands over the enforcement with it.

## Decision

The plane redeems its own secrets, through the broker it already serves.

A model configuration names a secret by reference, not by value. The plane resolves it at
CALL time through `internal/secrets`, as a principal of the tenant whose step is running,
and holds the value only for the duration of that call. The broker's existing rules apply
unchanged: single use, an expiry the issuer enforces, and a refusal that names neither the
handle nor the value.

There is therefore exactly ONE way a credential reaches a running thing in Dhole, and the
plane is not an exception to it.

## Consequences

An LLM step works on a deployment that configured a secret, and fails with a named reason on
one that did not — the same failure as today, reached honestly.

The plane now depends on its own broker to do work, which makes a start-up ordering rule:
the broker serves before the scheduler advances anything, or the first LLM step of a fresh
plane fails on a race. That is a real edge and it gets a test.

A secret is redeemed per call rather than cached, so a model provider's key rotates without
restarting the plane, and a run that takes an hour does not hold a value for an hour. The
cost is a redemption on every call, which is a local round trip against an in-memory broker.

Per-tenant model credentials become expressible, because the resolution is scoped to the
step's tenant. Nothing requires that yet, and the design no longer forecloses it.

What this does not solve is where the BROKER's own backing secrets come from on a
distributed plane — today they are issued in memory by the process that serves them. A
deployment wanting an external secret manager behind the broker is a separate decision.
