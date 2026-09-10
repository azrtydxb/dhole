# 0023 — A definition carries the files its steps need

Status: accepted
Date: 2026-09-10

## Context

A pipeline cannot reference a file. The CI acceptance pipeline needs a Dockerfile, so the
Dockerfile's text is embedded in the step, and a test keeps that copy equal to the one
checked into the repository — a test whose existence is the report of a missing feature.

"Read it from the repository" names something this system does not have. ADR 0008 makes the
definition store canonical and git a one-way mirror OUT of it: nothing in the tree clones,
there is no credential for a source remote, and the git trigger parses a webhook without
fetching. Reading a step's inputs from the mirror was considered and rejected outright —
anyone with push access to the mirror would change what runs, and the approval state and the
run history would stop meaning anything.

A source-fetching step type would be the CI-shaped answer, and is a bigger thing than it
looks: source credentials the plane must hold, a step that is not pure, and a cache key that
is only stable against a commit sha, so a branch name reintroduces the mutable-tag problem
`Step.image` already has.

## Decision

A file a step needs is part of the DEFINITION, not of a repository.

A revision may carry files. They are content-addressed, stored with the revision in the
definition store, and declared by a step as an input like any other. `dhole pipeline push`
uploads them beside the definition; the mirror exports them; a run pins the revision and
therefore pins the bytes.

This keeps ADR 0001's rule that a step declares its inputs and inherits no ambient state,
and it lands in the cache key for free, because the file's digest is already what the key
is built from. It also keeps ADR 0008's direction: the store is canonical and the mirror is
an export.

## Consequences

The acceptance pipeline's embedded Dockerfile becomes a declared file, and the test that
kept the two copies equal goes away — which is the point.

A definition is now something that can be large. That needs a ceiling, and the ceiling is a
tenant quota rather than a constant: files count against `MaxCASBytes` like every other
stored byte, so a tenant that attaches a disk image is refused by the limit that already
exists rather than by a number invented here.

Editing a pipeline in the GUI now has a second kind of change — attaching or replacing a
file — and `ApplyOperation` must express it, closed under inversion like every other
operation (ADR 0020). A file removed by an operation is not deleted from the CAS: it is
unreferenced, and the collector reclaims it when nothing points at it.

What this does NOT give anyone is a checkout. A pipeline that wants a whole repository at a
commit still cannot have one, and if that turns out to be the common case, a source-fetching
step type supersedes this rather than extending it.
