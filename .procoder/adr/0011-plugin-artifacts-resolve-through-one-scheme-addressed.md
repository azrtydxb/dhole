# 0011 — Plugin artifacts resolve through one scheme-addressed resolver

Status: accepted
Date: 2026-09-09

## Context

Plugins are third-party code that will run with credentials to production systems, so
their distribution path is the supply-chain surface. Container-shaped plugins are best
served by OCI registries, which already provide digests, signing, mirroring and airgap
workflows that we would otherwise have to build. But not every plugin is a container — a
WASM module or a schema-only trigger definition should not require one — and the homelab
single-binary path should not mandate standing up a registry.

## Decision

A plugin reference is a scheme-addressed URI — `oci://harbor.example/...@sha256:...` or
`cas://<hash>` — behind one resolver interface that must provide digest addressing,
signature verification and mirroring identically for both schemes.

Signatures are stored as detached records in the catalog keyed by artifact digest, with
cosign as one source that populates them. We deliberately do not depend on OCI-specific
signature conventions as the mechanism, because they have no equivalent on the `cas://`
path and the policy layer must not care which scheme an artifact came from.

The index is federated: multiple upstreams, each mirrorable into a tenant's local catalog,
each carrying its own allowed signing identities and mirror policy. Plugin identity is
namespaced per upstream (`<namespace>/<name>@<version>` plus digest) so two registries can
both ship `docker-build`. Resolution order is built-in types, then the tenant's local
catalog, then configured upstreams.

Rejected: hosting a central community index, which would make us the trust anchor and
abuse desk for third-party code running with production credentials. Rejected: OCI only
(excludes lightweight artifacts, mandates a registry for homelab) and CAS only (rebuilds
mirroring, signing and GC that OCI gives away).

## Consequences

Easier: existing registries — including a Harbor already in place — work as mirrors with
no new infrastructure. Airgapped and edge deployments have a supported path. Policy reads
one uniform signature record regardless of artifact format.

Harder: two distribution paths to secure, mirror and garbage-collect rather than one.
Because job messages are self-contained, every engine needs reachability to a registry
holding the artifact, so mirroring is per-tier or per-network rather than global — painful
to retrofit once engines are deployed across networks. Federation requires namespacing and
per-upstream trust configuration that a single index would not.
