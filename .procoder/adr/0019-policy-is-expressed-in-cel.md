# 0019 — Policy is expressed in CEL

Status: accepted
Date: 2026-09-09

## Context

0012 established policy as a first-class subsystem keyed on trust tier, consulted by the
scheduler, the registry, the dispatcher and the secret resolver — and explicitly deferred
how policy is written. That deferral cannot outlive the first policy check, since the
expression language determines whether policy can sit in the dispatch hot path, whether
tenants can supply their own policy in a hosted deployment, and whether a policy can be
proven to terminate.

## Decision

CEL — the Common Expression Language, as used by Kubernetes for admission policy.

It is purpose-built for this shape of problem: sandboxed, non-Turing-complete so
evaluation always terminates, fast enough to sit in the dispatch path, and embeds natively
in Go. Because policies are data rather than code, tenants can supply their own without a
rebuild, which 0014's hosted offering requires.

Rejected: embedded Rego/OPA — more expressive with better audit tooling, but a substantial
dependency, a real learning curve for anyone writing a policy, and careful caching needed
to stay out of the hot path. Rejected: a built-in declarative DSL, which starts narrow and
reliably accretes conditionals and functions until it is an ad-hoc language with none of
the tooling. Rejected: compiled Go policies — fastest and type-safe, but a policy change
would need a rebuild and redeploy, ruling out per-tenant policy entirely.

## Consequences

Easier: policy evaluation is bounded and safe to run on every dispatch. Per-tenant policy
becomes configuration rather than a deployment. Kubernetes familiarity transfers, and CEL's
existing Go tooling covers parsing, type-checking and cost estimation, so a bad policy can
be rejected at save time rather than at evaluation.

Harder: CEL is deliberately less expressive than Rego, and some policies — anything
needing iteration over large sets or external data lookups — will be awkward or
impossible, forcing that logic into the Go host as custom functions. The set of variables
and functions exposed to policy becomes a versioned public contract, since tenant-authored
policies depend on it.
