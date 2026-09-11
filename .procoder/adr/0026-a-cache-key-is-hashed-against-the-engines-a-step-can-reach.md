# 0026 — A cache key is hashed against the engines a step can reach

Status: accepted
Date: 2026-09-11

Supersedes: [0021 — The environment identity behind a cache key comes from the engines](0021-the-environment-identity-behind-a-cache-key-comes-from-the-engines.md)

## Context

ADR 0021 moved the environment identity behind a cache key off the control plane and onto
the engines, and folded it over the TIER: engines in one tier are interchangeable, the
queue picks which of them runs a step, so an identity that varied per engine would key the
cache on something the scheduler does not get to pick. Members are expected to agree, and a
tier whose members disagree caches nothing.

That was right for the fleet as it was. Every tier held one kind of engine, so agreement
was the normal case and disagreement was a half-finished rollout.

Engine-kind routing changed the fleet. A step can now name `Step.engine_type`, `Match`
places it only on an engine advertising that kind, and the dispatch goes to a kind-suffixed
subject — so one tier holding two kinds of engine became an ordinary configuration rather
than a misconfiguration. On kw, tier `trusted` holds a Kubernetes engine and a VM engine.
They report a busybox image digest and a rootfs digest. Neither is wrong, they will never
agree, and the tier-wide fold read the pair as a conflict and turned the cache off for the
WHOLE tier — including steps naming `engine_type: vm`, which can only ever land on the VM
engine and whose environment is therefore not in doubt at all. The control plane said so in
its log, repeatedly, and every run re-executed every step.

The fold was over the wrong set. "The tier" was a correct description of the engines a step
could be dispatched to only while a tier was one kind.

## Decision

The identity is folded over the engines a step could actually be dispatched to: its tier,
narrowed to the engines offering the kind the step named, and the whole tier when it named
none. `registry.TierEnvironmentIdentity` takes the kind; the scheduler and `api.Plan` both
pass what the step named, so a plan and a run resolve the same set.

The kind filter is the one `Match` already applies, including its rule that an engine which
advertised no kind at all satisfies no NAMED kind — unstated is unknown, not universal. An
engine that cannot be handed the step has no say in what the step is cached against, and
cannot disable it.

Everything ADR 0021 decided about the resulting set is unchanged, and is why this is a
narrowing rather than a reversal: it is still not per-engine, because the scheduler still
does not choose which member of the set runs the step. It only now includes the one thing
the scheduler DOES choose — the kind — which the step itself states. Disagreement within
the reachable set still means no identity and no cache key, an absent identity still means
the same, and both are still reported rather than degraded through silently.

## Consequences

A mixed tier caches the work it can account for. A step naming a kind is keyed against that
kind's environment; a step naming none stays uncached in a mixed tier, because it really
can be handed to either engine and the plane cannot say which environment produced the
result. That refusal is ADR 0021 still working, not a remaining half of the defect.

The operator-facing log becomes per set. It says which engines disagreed — "the vm engines
of this tier" rather than "this tier" — because a busybox digest and a rootfs digest listed
side by side read as a fault when they are two engine kinds behaving correctly. The
report is latched per (tier, kind) for the same reason it was latched at all: the identity
is resolved for every ready step of every run, and one tier-wide latch over a mixed tier
would alternate between the kinds' states and log both every time.

Two engines of ONE kind that disagree still cache nothing for that kind. A rollout is still
a rollout, and narrowing the fold to a kind must never narrow it to an engine.

The cost is that a tier's cacheability is no longer a single fact about the tier. An
operator asking "is this tier caching" now has to ask per kind, and the log answers in
those terms.
