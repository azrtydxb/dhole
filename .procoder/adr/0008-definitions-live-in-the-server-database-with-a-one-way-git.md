# 0008 — Definitions live in the server database with a one-way git mirror

Status: accepted
Date: 2026-09-09

## Context

The GUI is a drag-and-drop canvas that edits pipeline definitions, and agents edit the
same definitions through the same API. If the canonical document were hand-editable YAML
in a repository, every GUI save would have to be a surgical, comment-and-format-preserving
edit to a concrete syntax tree — the round-trip problem that killed Jenkins Blue Ocean's
editor and every WYSIWYG tool before it. But git is also not a hard requirement for this
system: many deployments will have no repository at all.

## Decision

The server database is the canonical store. YAML remains the definition *format*; where it
is stored is a pluggable backend. Git is maintained as a one-way mirror, DB to git, so the
repo stays a readable, diffable, backup-able copy; edits made directly in git are not
authoritative and are not merged back.

Every definition carries a revision identity — a content hash — and a run pins an exact
revision. Revisions carry an in-app approval state (`draft -> reviewed -> active`) with
the approver recorded, because DB-primary forfeits forge-native PR review and the
self-host and SaaS audiences need an equivalent. No backend-specific data lives in the
document, so export and import between backends stays lossless; layout, permissions and
history live outside it.

Rejected: git-primary with a DB cache (forces CST-preserving editing, and fails
deployments with no repo). Rejected: bidirectional sync, which is where these tools
historically lose data.

## Consequences

Easier: the entire CST-preservation problem disappears — nobody hand-edits the canonical
document, so the GUI may serialise freely. Versioning, approval and history are ours to
model coherently rather than inherited from a forge. The eventual SaaS has no repository
prerequisite.

Harder: we own review, history, diffing and approval UI that git would have given us free.
"Pipelines live next to your code" is no longer strictly true, which is a real pitch cost.
Users who do live in git must accept that the repo is an output, and we must be explicit
about that or they will edit it and lose work.
