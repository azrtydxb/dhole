# 0021 — The environment identity behind a cache key comes from the engines

Status: accepted
Date: 2026-09-10

## Context

ADR 0009 made the step cache correct by construction: a result may be reused only when the
key covering it captures everything the step depended on. One of those things is the
environment the step ran in, because the same command against a different image is a
different computation. `cache.Eligible` therefore refuses to cache anything at all when it
has no stable environment identity, and refusing is the right default: caching against an
unknown environment is how a cache returns yesterday's answer for today's toolchain.

That identity was taken from the control plane's own executor. On a real deployment it is
always empty, so nothing is ever cached — anywhere, in any shipped configuration:

- The single binary runs steps on a host process executor, which honestly reports it has no
  stable identity, because a host is whatever the host happens to carry.
- A distributed plane has no executor at all. It falls back to the process one purely to
  answer this question, and gets the same empty answer.
- Engines on the Kubernetes backend DO have a stable identity — the sandbox image resolved
  to a digest — and the plane never learns it.

Two runs of a pure two-step pipeline on a live cluster re-executed every step and left
`cache_entries` empty. The end-to-end cache test did not catch it because it injects an
executor that returns a hardcoded identity, which is a configuration no deployment can
produce.

The mismatch is structural rather than a missing assignment. The plane decides what may be
cached; the engines own the environment. In a distributed deployment those are different
machines, and the plane cannot inspect an environment it does not have.

## Decision

An engine reports its environment identity when it registers, and the control plane hashes
cache keys against the identity of the **tier** it is dispatching to.

The tier, not the individual engine. A tier exists to be a set of interchangeable workers —
the plane chooses a tier and the queue chooses the engine — so an identity that varied per
engine would key the cache on something the scheduler does not get to pick. Engines in one
tier are therefore expected to agree, and when they do not the plane caches nothing for
that tier and says so. A disagreement is a misconfiguration (a half-finished rollout, two
different sandbox images), and refusing to cache while it lasts is the same instinct that
refuses to cache against no identity at all.

This adds `environment_identity` to `EngineRegistration` and bumps the wire protocol. An
engine that does not send one is treated as having none, which disables caching for its
tier rather than poisoning it — the pre-existing behaviour, reached honestly.

## Consequences

Caching starts working in the deployments people actually run, and starts working for the
right reason: a Kubernetes tier caches against its sandbox image digest, and a host-process
tier still caches nothing, because a host process still has nothing stable to hash.

The plane gains a dependency on the fleet for a decision it used to make alone. A cache
lookup now needs the registry to have heard from at least one engine in the tier, so a
plane that has just started caches nothing until its fleet checks in. That is a cold cache,
not a wrong one.

Rolling two different sandbox images through one tier turns its cache off until the rollout
finishes. That is the price of not keying on something the scheduler cannot choose, and it
is visible — the plane logs the disagreement rather than silently degrading.

The wire contract grows a field, so an engine written against the older version registers
without one. It works, and its tier does not cache. Anyone writing an engine now has one
more thing to report, and `docs/wire-contract.md` says what it must mean: stable for
identical environments, different for different ones, and absent rather than invented.
