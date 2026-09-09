# 0020 — Editing operations are closed under inversion

Status: accepted
Date: 2026-09-09

## Context

0013 established one API contract serving the GUI, the CLI and agents equally, and named
five editing operations: `AddStep`, `Connect`, `SetProperty`, `RemoveEdge`, `Rename`. Each
`ApplyOperation` returns the inverse of what it applied, which is what makes undo a
property of the API rather than a feature the GUI reimplements — and what lets a concurrent
editor rebase rather than resend a whole document.

Building that API showed the five are not closed under inversion. `AddStep` has no inverse
in the set: nothing removes a step. Two of the five also have edges around what an inverse
means. `RemoveEdge` applied to an edge that never existed could be treated as a no-op,
whose "inverse" would then create an edge that was never there. `Rename` needs to express
naming a step back to nothing, or the inverse of naming a previously-unnamed step is not
expressible.

An operation set that is not closed under inversion fails quietly. Undo appears to work
until it reaches the one operation with no true inverse, and then it silently does
something else — which is worse than refusing, because the editor has already told the user
it undid their change.

## Decision

The operation set is closed under inversion, and that property is a constraint on the set
rather than a quality of each implementation. Concretely:

A sixth operation, `RemoveStep`, joins the five. It refuses a step that still has edges,
naming them. That refusal is what keeps its own inverse a plain `AddStep` rather than an
add plus an order-dependent set of reconnections — the disconnection is the caller's
explicit act, expressed as `RemoveEdge` operations they can undo one at a time.

`RemoveEdge` of an edge that does not exist is refused, not treated as a no-op.

`Rename` accepts the empty name.

`SetProperty` covers `plugin_ref`, `effect_class` and `lease_scope`, and carries enum values
as their declared names, so an unspecified value round-trips as itself rather than as an
absence.

A test drives every operation kind off the oneof descriptor and requires the inverse to
land back on the original revision id. Because revisions are content-addressed, that is
exact equality of content rather than a resemblance, and an operation added to the oneof
later fails this test until it has a fixture and a true inverse.

## Alternatives considered

**Return a best-effort inverse where no exact one exists.** Rejected: this is the failure
mode being avoided, not a mitigation of it. An undo that mostly works is indistinguishable
from one that works until the day it matters.

**Let the GUI implement undo over document snapshots.** Rejected by 0013 already — it puts
the behaviour in one client, leaves the CLI and agents without it, and reintroduces
whole-document editing, which cannot merge concurrent edits.

**Make `RemoveStep` cascade its edges.** Rejected: the inverse would have to restore the
edges in an order that reproduces the original graph, which makes the inverse of one
operation depend on graph state the operation did not name. Refusing a connected step keeps
every inverse a function of the operation alone.

## Consequences

Undo is total: every operation the API accepts can be undone exactly, and the property is
enforced by a test that a new operation cannot quietly skip.

Removing a connected step is now two or more operations rather than one. That is more work
for a client, and it is the price of each step of the undo being exact.

The operation oneof gained a member. This is additive on the wire, so engines and clients
on the previous version are unaffected, and `buf breaking` stays clean.

Anything added to the oneof later inherits the obligation: a true inverse, or it does not
belong in the set. That is a real constraint on future editing features — a bulk operation,
for instance, has to be expressible as a sequence whose inverses compose.
