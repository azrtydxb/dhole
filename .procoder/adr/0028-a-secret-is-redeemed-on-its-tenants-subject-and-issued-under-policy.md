# 0028 — A secret is redeemed on its tenant's subject and issued under policy

Status: accepted
Date: 2026-09-14

## Context

ADR 0027 gave a step its secrets and left five things open, each of which weakens the
property the design exists for — that a credential reaches exactly one process, for one
attempt, of one tenant.

The redemption subject names no tenant. Every other subject is tenant-scoped by the NATS
account the connection sits in (ADR 0014), but the plane serves redemption from ONE
connection and ONE in-memory broker, and the broker answers by handle alone. Wherever
tenants share an account — the single binary, a chart whose engines dial the shared URL —
nothing on the redemption path knows whose request it is, so nothing can refuse a handle
presented by the wrong tenant.

A handle an attempt never spent lives until its ten-minute expiry after the attempt ends,
and a cancelled run's still-queued dispatch can be picked up and redeemed after nobody wants
it.

Two plane passes racing each record `STEP_SECRET_UNAVAILABLE`, which reads as two refusals.

A `builtin:` step runs on the plane and receives no `JobDispatch`, so secrets it declares
were silently ignored: the author believes a credential was delivered and nothing was.

ADR 0012 names the secret resolver as a policy caller and nothing calls policy for a secret.

## Decision

**The redemption subject is `secret.redeem.<tenant>`**, where `<tenant>` is
`JobDispatch.tenant.id`. The plane serves `secret.redeem.*`, takes the tenant from the
subject, and refuses — with the same one refusal as every other — a handle issued for a
different tenant. A handle presented on the wrong tenant's subject is spent by being
presented, as it would be on the right one. An engine's tier credential
(`bus.TierPermissions`) allows requesting on its own tenant's subject and not on
`secret.redeem.*`; the plane's own `builtin:llm` resolution uses the same scoped subject.
An engine's `DHOLE_SECRET_SUBJECT` is now the BASE the tenant token is appended to.

**The unscoped `secret.redeem` stays served, deprecated**, for as long as the plane's
compatibility window includes protocol version 3 — the version this change was made
under. The constraint is that the plane accepts engines at version N and N-1, and an
engine written before this record redeems there. On it the tenant is taken from the handle,
which is exactly what it was before; in a deployment with an account per tenant the
account still scopes it. No protocol bump is made: the plane serves both subjects, so no
engine must do anything different to keep working, and an engine at version 4 or later is
by construction written against a contract that names the scoped subject, so the first
plane whose window excludes 3 stops serving the legacy one. Tier credentials keep the
legacy permission for the same window. An engine that finds no responder on the scoped
subject — a plane older than this record — falls back to the legacy one, so engines may
still be upgraded before the plane.

**An attempt's unspent handles are revoked when the attempt ends**: on its terminal status
(succeeded, failed, cancelled), when it is recorded lost or fenced out, when a dispatch
that issued handles does not commit, and — for a cancelled run — every handle of the run,
including a queued dispatch no engine has taken. The broker records the attempt each handle
was issued for; revocation is by exact attempt, never by step, so it cannot reach a retry
already issued. Handles live in one plane's memory, so a plane that did not issue them
revokes nothing, and the expiry remains the backstop.

**`STEP_SECRET_UNAVAILABLE` is unique per (tenant, run, step)**, by a partial unique index,
the way migrations 0021 and 0023 made the other one-time step verdicts unique.

**A `builtin:` step declaring secrets is refused** before it is served from cache, taken
by a plane worker or armed as a gate, with `STEP_SECRET_UNAVAILABLE` saying a plane-hosted
step cannot be given step secrets, and the run fails. A builtin that needs a credential
names it in its own configuration (ADR 0024).

**Policy is evaluated per declared secret where the step is judged at dispatch**, after
the step's own decision permits it and with the same facts, plus `input.secret_name` (a new
key, always present, empty for a decision about anything else) and `input.subject`
`secret:<name>`. A denial is recorded as the step's `STEP_POLICY_DENIED`, naming the secret,
and fails the run. Rules that do not read `secret_name` answer the secret's decision exactly
as they answered the step's, so no existing policy changes its outcome. A plane with no
dispatch policy evaluates nothing, as for steps. No default rule is introduced.

## Consequences

A handle leaked to another tenant is refused on the scoped subject, and the bus refuses the
request outright from a tier credential that is not that tenant's. Until the legacy subject
is withdrawn, a holder of a foreign handle on a shared account can still redeem it there;
that is the pre-existing exposure, bounded by the handle's 256 bits and its single use, and
it closes with the window.

An operator upgrades the plane before or after the engines, in either order, without a
flag day. A tier credential issued before this record lacks the scoped permission, and an
engine holding one falls back only when there is no responder, not when the bus refuses, so
such a credential is re-issued before the engines holding it are upgraded.

Revocation adds one map scan per attempt end in the issuing plane. A cancelled run's
queued dispatch now fails at redemption instead of running for nobody.

A policy author can write `input.secret_name != "prod-signing-key" || input.tier == "trusted"`.
Every secret costs one more evaluation and one more audit row per dispatch. Taint at
dispatch (ADR 0015) is not set by this record and remains open, and `dhole serve` still
wires no dispatch policy — which tier policy a deployment ships with is a decision this
record does not make.
