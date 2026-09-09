# Upgrades and version skew

The wire schema is a public contract. Within a major version changes are
**additive only**, and the control plane must keep talking to engines one version
behind — so a fleet upgrades gradually rather than in a flag day.

That is the whole policy. The rest of this page is what it obliges.

## The window: N and N-1

`internal/wire` holds two constants and one function:

```go
const ProtocolVersion uint32 = 1  // what this build of the control plane speaks
const SupportedWindow uint32 = 1  // how many versions back it accepts
```

`Negotiate` picks the **highest version both sides speak**, and never one above
the control plane's own: an engine advertising a future version is talked down,
not deferred to. An engine that speaks nothing in the window is refused with a
message naming the range, and it should exit rather than retry — no amount of
reconnecting will make it compatible, and the operator has to upgrade it.

Widening `SupportedWindow` is a deliberate decision, not a convenience. Every
extra version is another shape the control plane must keep handling correctly,
for ever, including in the code paths nobody exercises any more.

## What "additive only" forbids

Within a major version:

- Fields are never renumbered.
- Fields are never removed.
- A field never changes meaning.
- An enum value is never repurposed.

A new field must be optional in effect: an engine one version behind will not
set it, and the control plane has to work anyway. If a change cannot be made
additively, it is a new major version, and that is a decision with a migration
plan attached — not a patch release.

`make check` runs `buf breaking` against `main`, so a change that breaks the
schema fails the gate rather than a fleet.

## Upgrading a deployment

**Control plane first, then engines.** The plane accepts N and N-1, so a plane at
N and engines at N-1 is a supported state. The reverse — engines at N against a
plane at N-1 — is not: the plane talks them down to N-1, which works, but only
because the engine still speaks N-1. An engine that has dropped N-1 cannot be
deployed before the plane.

The order in practice:

1. Upgrade the control plane. Existing engines keep working; negotiation lands
   on the older version until they are replaced.
2. Roll the engines. Each restart renegotiates upward on its own.
3. Only once every engine is at N may the next release drop N-1.

Two control-plane processes may share a database — that is what lets two of them
drain the same outbox backlog — but they must **not** share a `--deployment-id`
with a plane that is a different deployment. An outbox row is addressed to one
plane's bus; a claim that does not name the plane makes each publish the other's
dispatches to engines that never heard of the run.

## What survives a restart

A control-plane restart is a non-event for work already in flight. Runs are
durable and event-sourced ([ADR 0003](../.procoder/adr/0003-runs-are-durable-and-event-sourced.md)),
the dispatch is still on the bus and still means what it meant when it was
written, and unacknowledged dispatches return to the queue.

SIGINT and SIGTERM **stop** the plane rather than killing it. Give it time to
finish: unsent outbox rows are still owed, and an unacknowledged dispatch that
goes back to the queue is a step someone else picks up rather than a step lost.

An engine stopped mid-step does not silently drop the work — the dispatch is
redelivered, and a newer attempt fences out the older one, which is why an
engine must refuse a control message whose fence token is not the one it holds.

## Database migrations

Store migrations run at start-up. They are forward-only and additive for the
same reason the wire schema is: during a rolling upgrade, two plane versions
read the same database. A migration that removes or repurposes a column breaks
the version that has not been replaced yet.

The safe shape is the usual one — add the new column, write both, backfill,
switch reads, and only in a **later** release stop writing the old one.

## Version stamping

`make build` stamps the version and commit into `internal/version` through
`-ldflags`. `dhole version` and `dhole-engine`'s start-up line both print them,
and the plane's start-up line carries them too. When a fleet is mid-upgrade,
that string is how you tell which engines are still on the old build.
