# 0014 — Tenancy exists in the data model from the first commit

Status: accepted
Date: 2026-09-09

## Context

Dhole targets three audiences at once: a single-operator homelab (the first user),
open-source self-hosters, and an eventual commercial hosted offering. These have opposite
pressures. The homelab must install as one binary with no dependencies; the hosted
offering needs hard isolation between tenants who do not trust each other. Retrofitting a
tenant scope into a running system — every table, every bus subject, every artifact key,
every query — is one of the most reliable ways for a project like this to stall.

## Decision

Tenancy is in the data model from the first commit, even while exactly one tenant exists.
Every table, bus subject and artifact key carries a tenant scope. Tenants map onto NATS
accounts (0005), so isolation is enforced at the transport layer rather than by remembering
to add a WHERE clause.

Deployment topology is a backend choice over identical code paths: single binary with
embedded NATS, SQLite and an in-process engine for homelab; external NATS cluster and
Postgres for scale. The embedded engine speaks the identical protocol over a loopback bus
rather than taking a direct in-process shortcut, so there is one dispatch path and the
wire contract is exercised on every development run.

Rejected: single-tenant now with a migration later. Rejected: an in-process fast path for
the embedded engine, which would create a second dispatch path that inevitably drifts from
the contract third parties implement.

## Consequences

Easier: the hosted offering needs no data-model migration. Isolation bugs surface as
subject-permission errors rather than as leaked rows. Running both topologies daily keeps
the storage and bus abstractions honest instead of aspirational.

Harder: a tenant scope on everything is friction in every query and every subject name
while there is only one tenant, and it will feel like ceremony for a long time before it
pays. The loopback bus makes local single-binary execution marginally slower than a direct
call would be.
