---
inclusion: always
---

# Working on Dhole

Instructions for any AI coding agent — or human — picking up this repository.

Dhole is in the **design phase**. There is no implementation yet: no Go module, no source
tree. What exists is a complete, gated chain of design artifacts, and the first job is to
execute them in order rather than to start writing code from the README.

## Read these, in this order

1. **`README.md`** — what Dhole is and why.
2. **`docs/design-history.md`** — how every decision was reached, including the options
   rejected and the two positions that were reversed mid-design. Read this before
   proposing an architectural change; most obvious objections were already raised and
   answered.
3. **`.procoder/adr/`** — 19 accepted architecture decision records. Each carries the
   constraint that forced it, the alternatives, and what it costs.
4. **`.procoder/specs/dhole.md`** — the system spec: 21 scope items, explicit out-of-scope
   list, constraints, interfaces, data, edge cases, failure modes, and 36 testable
   acceptance criteria.
5. **`.procoder/plans/dhole.md`** — 59 implementation tasks in dependency order, each with
   its files, the interfaces its neighbours consume, and literal failing tests.
6. **`.procoder/ask/decisions.md`** — the raw decision log from the design conversation:
   every question put to the project owner and the answer given.

## The rules

**ADRs are immutable.** A changed mind writes a new record marked as superseding the old
one. Never edit an accepted record. `procoder adr check` enforces the references.

**The chain runs one way: ADR → spec → plan → code.** If reality contradicts the plan
mid-build, update the plan and re-run `procoder plan check` _before_ writing the code that
diverges. If it contradicts the spec, update the spec and re-run `procoder spec check`
first. Do not let the artifacts drift behind the tree.

**Work one plan task at a time, in order.** Each task in the plan is a complete unit: write
the failing test exactly as written (the plan states the expected failure message),
implement minimally, run to pass, commit. A task's implementer is assumed to see only that
task — which is why each carries an `Interfaces:` line. If you need something a neighbour
produces and it is not on that line, the plan has a gap: fix the plan, do not guess.

**Every task ends gate-clean.** `make check` (gofmt, go vet, golangci-lint, buf lint) and
`make test` must pass before the commit.

**Never add attribution trailers to commit messages** — no `Co-Authored-By`, no
tool-generated sign-offs.

**Do not skip the tests.** The plan's value is that each task has a test that fails first.
An implementation whose test never failed has not been shown to test anything.

## Where to start

**Task 1** in `.procoder/plans/dhole.md` — repository scaffold and quality gate. Then 2, 3,
and so on. Tasks 1–17 are all justified by **Task 18**, which is the first point where the
architecture either works or does not: single binary, embedded bus, in-process engine over
a loopback bus, a two-step pipeline running end to end. Treat Task 18 as the first real
checkpoint; if the design is wrong somewhere, that is where it will surface.

## Constraints you inherit on every task

Taken from the spec's Constraints section — these apply to all work without being restated:

- Go 1.26+ (required by `github.com/azrtydxb/go-ai-sdk`); Node 22+ for the web app.
- Module path `github.com/azrtydxb/dhole`, protobuf package `dhole.v1`, binary `dhole`.
- Linux amd64 and arm64 for control plane and engines through Task 52; macOS and Windows
  engines arrive only at Task 54.
- Apache-2.0. No per-file licence headers; the repository `LICENSE` governs.
- The wire schema is a public contract: additive changes only within a major version, and
  the control plane must accept engines speaking version N and N-1.
- Every stored record and every bus subject carries a tenant scope. There is no unscoped
  query and no unscoped subject, even while only one tenant exists.
- Engines never receive secret values, only short-lived references they redeem.
- Policy evaluation completes within 10ms including cache lookup; a ready step reaches an
  engine within one second at target load.
- Postgres and clustered NATS are the tuned target; SQLite and embedded NATS must keep
  working for development and homelab.
- Go tests use standard `testing` with `testify/require`; web tests use Playwright.

## The traps this design exists to avoid

Four mistakes would each undo a large part of the architecture. They are easy to make and
expensive to reverse:

1. **Modelling a run as a goroutine.** Runs are state machines driven by a persisted event
   log. A goroutine holding run position in its call stack cannot survive a restart or
   represent a three-day wait. See ADR 0003.
2. **Treating the bus as the source of truth.** NATS carries transport; durable run state
   lives in the store, bridged by an outbox. See ADR 0005.
3. **Shaping the executor interface around Docker.** It must fit a bare process and a VM as
   first-class citizens, or every non-container backend becomes a fake. See ADR 0006.
4. **Letting a step inherit ambient filesystem state.** Steps declare inputs and outputs;
   the DAG is derived from that. Reintroducing a shared mutable workspace silently breaks
   the cache. See ADR 0001.
