# 0028 — A step's declarations are edited one element at a time

Status: accepted
Date: 2026-09-14

## Context

ADR 0020 made the editing vocabulary closed under inversion and ADR 0013 made that vocabulary
the only way anything edits a pipeline. Neither said the vocabulary had to REACH every field
of a `Step`, and it did not. Running a real pipeline on kw, declaring the `nexus-push` secret
(ADR 0027) on an existing `image` step took eight revisions: three `remove_edge`, a
`remove_step`, an `add_step` carrying the new fields, and three `connect`. `Step.secrets` and
`Step.capabilities` had no operation at all, and neither did `image`, `engine_type` or
`timeout_seconds`. The only edit that could touch them was replacing the step — which is a
worse audit trail (the revision history says a step was deleted), breaks undo granularity
(undoing the secret is eight undos), and conflicts with every concurrent edit to the step's
edges.

Three questions had to be answered for the new operations, and each has a tempting answer that
breaks ADR 0020.

How a list element is named. `secrets` and `capabilities` are repeated fields, and the order
of a repeated field is part of the definition's bytes and therefore of its content hash. An
inverse that re-adds a removed element at the END restores the same set and a DIFFERENT
revision id — exactly the "resembles the original" inverse 0020 refuses.

Duplicates. `add_step` accepts any step, so a list can already hold the same capability twice
or bind two secrets to one environment variable. An operation naming such an element cannot
say which occurrence it means, and any choice makes some inverse land on the wrong one.

Where the secret rules are enforced. ADR 0027 requires a step declaring secrets to declare
`CAPABILITY_SECRETS`, each environment variable to be a valid and unique name, and each
declaration to name a secret. Refusing an operation whose RESULT breaks those rules looks
like the helpful answer and is not closed under inversion: a step that is already invalid
(added whole by `add_step`, by an agent, or by an importer) could then be made valid by an
operation whose inverse — making it invalid again — is refused. Undo would fail on precisely
the edit that fixed something. The only rule that is both closed and lets an author repair a
step is one that does not refuse on the cross-field invariant at all.

## Decision

Two operations join the oneof, each editing ONE element of one step's list, each with an
exact inverse expressed as the same operation:

`SetStepSecret{step_id, env, name, remove, optional index}` binds, rebinds or unbinds the
secret an environment variable receives. The binding's identity is its `env`. Binding an env
the step does not have inserts it at `index` (unset appends) and inverts to removing it;
rebinding one it has keeps its place and inverts to rebinding the previous name; removing one
inverts to binding it back AT THE INDEX IT HELD. An `index` on a rebind is refused rather than
ignored, because a rebind that silently did not move would not be what was asked.

`SetStepCapability{step_id, capability, remove, optional index}` declares or withdraws one
capability, with the same positional rule. Declaring one the step already has is refused, as
`connect` refuses an existing edge: its "inverse" would withdraw a capability the step had
before the edit.

The refusals are those that hold identically in both directions, so an accepted operation
always has an accepted inverse: a step that does not exist; an env that is not a valid
environment variable name (it is the binding's key, and every operation — its inverse
included — carries it); `CAPABILITY_UNSPECIFIED` or an undeclared enum number; removing what
is not there; an index past the end of the list; and an element the step's list already holds
more than once, which names no single element.

The ADR 0027 rules are enforced where they are a property of a DEFINITION rather than of an
edit: `Validate` reports each as an error on the step, in the words the scheduler uses when it
refuses to dispatch the same step (one function, `secrets.ValidateDeclarations`, answers both).
The scheduler's refusal at dispatch stays the backstop. An author may therefore pass through
an invalid intermediate — add the secret, then the capability — and is told so before running.

`SetProperty` extends to the step's remaining scalars: `image` and `engine_type` (their text,
empty meaning "unset" as the fields already define) and `timeout_seconds` (a decimal, `0`
meaning unbounded; anything that does not parse as a `uint32` is refused, and the inverse
carries the old value's decimal text so an unset timeout round-trips as `0`).

Ports (`inputs`, `outputs`) and `file_inputs` remain reachable only through
`remove_step`/`add_step`. A port is a typed interface that edges and file bindings name, so
removing or retyping one either cascades into those (which 0020 rejects for `remove_step`) or
needs refusals whose design is a decision of its own. It is recorded as an open item rather
than decided here by accident.

## Alternatives considered

**A `SetStepSecrets` operation carrying the whole list.** Rejected: it is whole-document
editing at the scale of one field. Two people editing different bindings on one step would
overwrite each other, and a diff would show "secrets changed" rather than which one.

**Name a position by the element it goes before, not by index.** Rejected: with a duplicated
element in the list, "before X" is ambiguous in exactly the way the operation is trying to
avoid, and the inverse of a removal next to a duplicate would land in the wrong place. An
index is unambiguous, and concurrent edits to one step already conflict (the rebase in
`presence.go` claims the whole step), so an index cannot be moved under the operation.

**Refuse an edit whose result breaks the ADR 0027 rules.** Rejected for the reason in the
context: it is not closed under inversion, and it would refuse the edit that repairs a step
another client added broken.

**Canonicalise the lists (sort them) on every edit.** Rejected: the first edit to a step
authored in another order would reorder it, and that edit's inverse could not restore the
original bytes.

## Consequences

A secret or capability on an existing step is one revision and one undo, and the revision
history names the binding that changed. The web inspector and an agent use the same two
operations; the CLI reaches them through `pipeline apply --operation`, as it reaches every
other kind.

A definition can be saved in a state the scheduler will refuse to run: a secret with no
`CAPABILITY_SECRETS`. It was already reachable through `add_step`; it is now reported by
`Validate` rather than discovered at dispatch.

A list holding a duplicate element can only be repaired by replacing the step. Nothing in the
editing vocabulary creates one, so it comes only from an `add_step` that carried it.

The oneof gained two members and two messages with new field numbers, one of them `optional`.
All additive: `buf breaking` stays clean, and a client on the previous schema never sends them.
