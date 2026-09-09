# 0012 — Policy is a first-class subsystem keyed on trust tier

Status: accepted
Date: 2026-09-09

## Context

Trust tier accumulated decisions across several unrelated components: which engines are
reachable (0006), which upstreams and signing identities are acceptable (0011), which
plugin capabilities may be requested, which effect classes may run (0002), and whether
tainted data may reach an effectful step (0015). Left as scattered checks in the
scheduler, the registry and the dispatcher, this produces several subtly divergent notions
of "trusted" and no way to answer why a particular run was permitted.

## Decision

One policy evaluation point with one audit trail, consulted by the scheduler, the
registry, the dispatcher and the secret resolver. Policy input is the trust tier plus the
subject of the decision; output is a permit/deny with a recorded reason.

Plugin manifests declare intent — required capabilities (network egress, secrets,
privileged, host mounts), the effect class they operate under, input and output schemas,
and compatible engine types — so policy can refuse a plugin at definition-save time
rather than discovering its behaviour in production.

## Consequences

Easier: "why was this allowed" has one answer from one log. Adding a tier, or a new
dimension of restriction, is a policy change rather than edits across four components.
The declared-capability manifest is what makes effect classes and taint enforceable rather
than advisory.

Harder: a policy evaluation sits in the dispatch hot path and must be fast and
cacheable. Manifests become a mandatory, reviewed part of publishing a plugin. How policy
is expressed — a built-in DSL, embedded Rego, or compiled Go — is not settled by this
record and needs its own decision.
