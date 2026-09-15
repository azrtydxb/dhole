# 0032 — The plane always evaluates a dispatch policy, over a floor it cannot be configured out of

Status: accepted
Date: 2026-09-15

## Context

ADR 0012 puts one policy evaluation point, with one audit trail, in front of every
dispatch, and ADR 0030 extended it to each secret a step declares. Neither was reachable
from the product: `dhole serve` passed no `scheduler.Config.Policy`, so on the single binary
and the chart no step and no secret was ever put to a rule, and `policy_audit` stayed empty.
The owner decided what a fresh install permits (.procoder/ask/decisions.md, "Which dispatch
policy `dhole serve` runs"): wire it and ship a permissive default — every existing step and
every secret allowed — that operators tighten, so the audit trail exists from day one.

Two things stand in the way of "every step allowed" meaning literally that.

The spec's security constraint is not a default somebody may relax: untrusted-tier work
cannot reach privileged engines, unsigned plugins, or at-most-once steps without an approval
gate. ADR 0015 says tainted data reaching an effectful step or a privileged engine is
blocked unless a sanitisation gate clears it. Those rules exist in CEL
(`taint.BuiltinPolicy`), but only `builtin:agent` evaluates them. A default document that
replaced the tier's rules would carry them only if its author remembered to, and an
operator's `--policy` would drop them the first time it was written from scratch.

And the dispatch path never sets `input.tainted`, `input.taint_sources` or
`input.engine_capabilities`, so even a rule that reads them sees untainted work on no
engine. A permissive default over those inputs would look like a floor that holds while
holding nothing.

## Decision

**`dhole serve` always wires a dispatch policy.** The scheduler is given a CEL engine over
the plane's tier, audited to `policy_audit` in the run database, with provenance from the
run database's signature store and upstream registry. There is no configuration in which
the plane dispatches without evaluating one.

**The default is a real document**, `internal/policy/default.yaml`, embedded in the binary
and printed by `dhole policy default`. It is one rule, `default.permit`, whose expression is
`true`, with the examples an operator tightens it with written beside it. It is not a nil
check that skips evaluation: every step and every declared secret is evaluated against it
and recorded.

**An operator replaces it whole** with `dhole serve --policy FILE` (`DHOLE_POLICY`), in the
same format `dhole policy test --policy` reads, or `controlPlane.policy` in the chart
(inline rules rendered into a ConfigMap, or an existing ConfigMap). A file that does not
parse, a rule that does not compile or returns a non-bool, and a file with no rules refuse
start-up naming the file and the rule; the last because a tier with no rules denies
everything, and a plane that refuses every step is not what anyone who wrote an empty file
meant. The file is re-read while the plane runs: a changed file is installed under a
revision that folds in a hash of its content, so the compiled-program cache (keyed on tier
and revision) cannot keep evaluating the rules it replaced; a changed file that is invalid
is reported and the policy in force stays.

**Beneath whichever document is in force sits a floor**, evaluated first and not
replaceable by configuration:

- ADR 0015's two taint rules, unchanged and under their existing ids:
  `taint.privileged-engine` (`!input.tainted || !("PRIVILEGED" in input.engine_capabilities)`)
  and `taint.effectful-step` (`!input.tainted || input.effect_class == "PURE"`).
- The spec's untrusted-tier constraint, keyed on the tier `untrusted`:
  `tier.untrusted-signed-plugins`, `tier.untrusted-unprivileged-engines` and
  `tier.untrusted-no-at-most-once`. A pipeline step has no route through an approval gate at
  dispatch — that route is `builtin:agent`'s, in code (ADR 0015, 0025) — so an at-most-once
  step in that tier is refused outright.

The floor only ever narrows. It is composed onto a tier that HAS rules, never onto one that
has none, so a missing policy still denies (the reason `taint.defaultingSource` is kept apart
from tier policy). Its revision is part of the composed revision, so a floor that changes
with the build is never evaluated from a program compiled against the old one.

**The dispatch path supplies the taint keys.** `input.tainted` and `input.taint_sources` are
derived from the run's own log at the moment of the decision: the taint marks on the
`RUN_CREATED` values bound to the step's free input ports, united with the taint of every
step feeding one of its ports through an edge, transitively. It is conservative in the way
`taint.Propagate` is — any tainted input taints everything downstream — and a step with a
recorded `TAINT_SANITISED` passes on its inputs' sources less the sources that record
cleared. Nothing is stored beside the log: a replayed run recomputes the same answer.
`input.engine_capabilities` is the union of what the engines this step matches advertise,
because a dispatch goes to a tier's work queue rather than to one engine, and any of them may
take it; a step the plane hosts itself reaches no engine and has none. Both are set for the
step's decision and for each of its secrets'.

**Every decision is on the audit trail, allows included**, as ADR 0012 and migration 0011
already intend, and an allow's reason names the revision that allowed it — an allow names no
rule, so without the revision its row could not say which policy permitted it. The row does
not gain a run id: SQLite has no idempotent `ADD COLUMN`, this schema is re-applied on every
open, and a column added to `policy_audit` would make every existing development database
fail to open (the cost migration 0015 accepted for the outbox, and not worth paying here).
A decision is correlated with its run by tenant, subject and time, and a refusal is on the
run's own log as `STEP_POLICY_DENIED`.

## Consequences

A fresh install behaves as it did — every step and secret runs — except where the floor
refuses, and it now leaves one audit row per step and per declared secret per dispatch. An
operator tightens the plane by writing rules, tests them with `dhole policy test`, and cannot
weaken the floor by doing so.

This deliberately departs from the literal "every existing step allowed". A pipeline whose
non-`PURE` step consumes data an untrusted trigger admitted — any `git` trigger, an `http`
trigger marked untrusted — is refused at dispatch with `STEP_POLICY_DENIED` naming
`taint.effectful-step`, where before this record it ran. That is ADR 0015 and the spec
holding, not a default being strict, and it is surfaced for the owner in
.procoder/ask/decisions.md because no sanitisation gate step type is wired in `dhole serve`
yet: until one is, such a pipeline must make the consuming step `PURE` or not bind
untrusted data to it.

A tainted step is refused if ANY engine it could reach advertises `PRIVILEGED`, even when an
unprivileged one would have taken it. Restricting placement instead would need the work
queue to route by capability, which it does not.

Dispatch costs one fleet listing and one log fold more per ready step, and two more CEL
programs per decision (five short-circuiting floor rules); the decision stays far inside the
10ms budget. Nothing yet reads `policy_audit` through the API; an operator reads the table.
