# 0009 — Content-addressed cache is a v1 core primitive

Status: accepted
Date: 2026-09-09

## Context

0001 makes a step a function of declared inputs, which makes content-addressed caching
possible. The question was whether to build it in v1 or design for it and defer. Deferring
is cheaper up front, but the cache is the strongest technical claim Dhole has — it is what
distinguishes it from n8n, Temporal and Woodpecker alike — and its requirements reach into
the engine wire contract, which is exactly the surface that becomes expensive to change
once third-party engines exist.

## Decision

The content-addressed store and cache-keyed execution ship in v1. Engines compute and
report input and output content digests as part of the wire protocol from the first
version. A step whose input digest set has been seen before, under the same resolved
environment identity and plugin lockfile, is skipped and its recorded outputs reused.

Only `pure` steps (0002) are cache-eligible, so the automation and LLM profiles are
unaffected rather than compromised.

Reclamation is by refcount from retained runs: anything referenced by a run inside the
retention window is pinned, everything else is collectable. Rejected: TTL plus LRU with a
quota, which is simpler and billable but can silently evict a blob a visible run needs to
display or replay, forcing every UI surface to handle missing artifacts. Rejected: never
evicting, which is fine for a homelab and unusable as a cost model.

## Consequences

Easier: incremental pipelines become the default rather than a plugin someone configures.
Rebuild times drop from minutes to seconds on unchanged inputs with no cache keys authored
by hand. Provenance and SLSA attestation are nearly free once artifacts are already
content-addressed.

Harder: v1 grows substantially, and it is growing at the same time as three acceptance
pipelines (0016 records the parallel build order). Purity discipline must hold everywhere —
declared inputs, pinned plugin versions, honest environment identity — or the cache
returns wrong results, which is worse than having no cache. Garbage collection must be
correct against a live refcount, and the retention window becomes a user-visible policy.
