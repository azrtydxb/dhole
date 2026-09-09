# 0010 — Runtime engine registry is separate from the durable catalog

Status: accepted
Date: 2026-09-09

## Context

"Register engines and plugins so the central system knows about them, and track versions"
sounds like one table. It is two things with opposite lifecycles. Which engine *instances*
are alive right now is ephemeral state that should evaporate on restart. Which step types,
plugin kinds, engine types and trigger types *exist*, with their schemas and provenance,
is durable state that must survive everything. Conflating them means either liveness
outlives reality or the catalog evaporates on reboot.

## Decision

Two registries.

Runtime registry — engine instances, in NATS KV with heartbeat TTL. An instance that stops
heartbeating ages out, which doubles as the orphan detection 0004 requires. Instances have
explicit lifecycle states: `registering -> ready -> draining -> gone`, where draining makes
rolling upgrades non-disruptive. Registration carries identity, capabilities, protocol
versions spoken, supported plugin and engine types, and resource capacity, validated
against schema and tier policy before subject subscriptions are granted.

Catalog — step, plugin, engine and trigger types with their schemas and versions, in the
database, versioned and audited.

Plugin versions resolve at definition-save time into a per-revision lockfile of exact
versions and digests, in the manner of a package lockfile. Tags are human-facing aliases
resolved at save; dispatch uses digests only. Version skew is first-class: the scheduler
routinely sees a fleet on several builds and routes only to instances satisfying a step's
resolved requirements.

## Consequences

Easier: liveness is self-healing and needs no reconciliation job. Cache keys are honest,
because a plugin upgrade is a visible new revision rather than a silent change under a
floating tag — which is exactly what the approval state in 0008 exists to gate.

Harder: two stores to reason about, and the boundary must be enforced or entries will
drift into the wrong one. Rolling upgrades need the drain state implemented properly
rather than by killing processes. The lockfile adds a resolution step to every save, and a
plugin upgrade becomes an explicit user action rather than something that happens by
itself — correct, but it will read as friction.
