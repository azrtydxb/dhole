# Policy authoring

Policy is the one place a trust-tier decision is made and the one place it is
recorded ([ADR 0012](../.procoder/adr/0012-policy-is-a-first-class-subsystem-keyed-on-trust-tier.md)).
The scheduler, the plugin registry, the dispatcher and the secret resolver all
ask the same question here, so "why was this allowed" has one answer from one
log rather than four subtly divergent notions of trust.

Rules are written in CEL ([ADR 0019](../.procoder/adr/0019-policy-is-expressed-in-cel.md)):
data rather than code, so a tenant can supply its own without a rebuild, and not
Turing-complete, so evaluation always terminates.

## The policy `dhole serve` runs

`dhole serve` always evaluates a dispatch policy: every ready step, and then each
secret it declares, is decided before the cache or an engine sees it, and every
decision — allows included — is a row in the `policy_audit` table of the run
database ([ADR 0032](../.procoder/adr/0032-the-plane-always-evaluates-a-dispatch-policy-over-a-floor.md)).
There is no setting that skips evaluation.

With no configuration it is the **built-in default**, which permits every step
and every secret. It is a real document, not an absence; print it with:

```
dhole policy default
```

```yaml
revision: default/1
rules:
  - id: default.permit
    expression: "true"
    reason: the built-in default permits every step and every secret
```

An allow names no rule, so its audit row's reason names the revision instead —
`every rule of tier "trusted" (revision floor/1(builtin/1)+default/1) permitted this`.
Read the trail straight from the database:

```sql
SELECT at, subject, allow, rule, reason FROM policy_audit
 WHERE tenant_id = 'default' ORDER BY at DESC LIMIT 50;
```

### Tightening it

Copy the default, edit it, test it, and hand it to the plane. It **replaces** the
default whole:

```
dhole policy default > policy.yaml
$EDITOR policy.yaml
dhole policy test --policy policy.yaml --tier trusted \
  --subject secret:release-signing-key --secret-name release-signing-key --expect-allow=false
dhole serve --policy policy.yaml          # or DHOLE_POLICY=policy.yaml
```

In the chart, set `controlPlane.policy.rules` to the document, or point
`controlPlane.policy.existingConfigMap` (and `key`) at a ConfigMap you manage.

A file that does not parse, a rule that does not compile or does not answer a
bool, and a file with no rules all **stop `dhole serve` at start-up**, naming the
file and the rule. A file with no rules is refused rather than run because a tier
with no rules denies everything; to permit everything, say so with a rule.

The plane re-reads the file every ten seconds. A changed file is installed under
its revision with a hash of its content appended, so an edited rule takes effect
even if you forget to bump `revision`. A changed file that cannot work is logged
and **not** installed — the policy in force stays.

Restrict a secret to a tier:

```yaml
- id: signing-key-for-releases-only
  expression: 'input.secret_name != "release-signing-key" || input.tier == "release"'
  reason: only the release tier may read the signing key
```

Refuse tainted input on a privileged engine, and keep secrets away from it
entirely (the first half is already the floor; the second is yours to choose):

```yaml
- id: untrusted-off-privileged-engines
  expression: '!input.tainted || !("PRIVILEGED" in input.engine_capabilities)'
  reason: untrusted data may not run on an engine that holds host privilege
- id: no-secret-on-untrusted-data
  expression: '!input.tainted || input.secret_name == ""'
  reason: a step reading untrusted data may not be given a secret
```

### The floor

Beneath whichever document is in force, the plane evaluates a floor **first**,
and nothing in a policy file can permit what it refuses:

| Rule                                  | Refuses                                                       |
| ------------------------------------- | ------------------------------------------------------------- |
| `taint.privileged-engine`             | tainted data reaching any engine that advertises `PRIVILEGED` |
| `taint.effectful-step`                | tainted data reaching a step that is not `PURE`               |
| `tier.untrusted-signed-plugins`       | an unsigned plugin in the `untrusted` tier                    |
| `tier.untrusted-unprivileged-engines` | a `PRIVILEGED` engine in the `untrusted` tier                 |
| `tier.untrusted-no-at-most-once`      | an at-most-once step in the `untrusted` tier                  |

These are ADR 0015 and the spec's security constraint, not defaults. The
practical consequence: a step that is not `PURE` and reads a value a `git`
trigger or an untrusted `http` trigger bound is refused with `STEP_POLICY_DENIED`
naming `taint.effectful-step`. Make that step `PURE`, or do not bind the
untrusted value to it.

### Where the taint keys come from at dispatch

`input.tainted` and `input.taint_sources` are derived from the run's own log when
the decision is made: the taint marks on the values the run was started with that
are bound to the step's free input ports, plus the taint of every step that feeds
one of its ports, however far upstream. A step on which a sanitisation gate
recorded `TAINT_SANITISED` passes on its inputs' sources less the ones it cleared.

`input.engine_capabilities` is every capability advertised by an engine the step
could be placed on — a dispatch goes onto the tier's work queue and any matching
engine may take it — so a tainted step is refused if **any** engine it can reach
is privileged. A step the plane hosts itself (`builtin:`) reaches no engine.

## The shape of a policy file

```yaml
revision: 2026-09-01
rules:
  - id: signed-plugins-only
    expression: input.signed
    reason: this tier runs only plugins with a verified signature
  - id: no-privilege-on-untrusted-data
    expression: '!(input.tainted && "PRIVILEGED" in input.capabilities)'
    reason: a step reading untrusted data may not ask for privilege
```

A policy belongs to one **trust tier**. Every rule in it must hold: a rule that
evaluates to `false` denies, and its `id` is what the decision and the audit row
name. `reason` is what the refusal tells whoever hit it, so write it for the
person who has to fix their pipeline, not for the person who wrote the rule.

`revision` is part of the compiled-program cache key. Without it an edited
policy would go on being evaluated with the program compiled from the policy it
replaced.

## Everything fails closed

A tier with no policy, a policy that will not compile, a rule that errors at
evaluation, an audit write that fails: each one **denies**. Permission is
something a rule grants, never something the absence of an answer leaves behind.

This has a practical consequence worth internalising: a typo in a rule is not a
rule that is skipped. It is a tier that refuses everything. `dhole policy test`
exists so you find that out at your desk.

## The variables a rule may read

`input` is declared as a map, deliberately: a rule naming a field that does not
exist is an **evaluation error**, which denies, rather than being silently
rewritten to a zero value.

| Key                         | Type           | What it is                                          |
| --------------------------- | -------------- | --------------------------------------------------- |
| `input.tier`                | string         | the trust tier being evaluated                      |
| `input.tenant_id`           | string         | the tenant the decision is scoped to                |
| `input.subject`             | string         | what is being decided — a step, a plugin, a secret  |
| `input.capabilities`        | list of string | the capabilities the plugin manifest declares       |
| `input.effect_class`        | string         | `PURE`, `IDEMPOTENT`, `AT_MOST_ONCE`, `UNSPECIFIED` |
| `input.plugin_ref`          | string         | the reference the step names                        |
| `input.signed`              | bool           | the artifact carries a verified signature           |
| `input.upstream`            | string         | the registry or source it came from                 |
| `input.tainted`             | bool           | untrusted data reaches this subject                 |
| `input.taint_sources`       | list of string | the triggers that admitted it, sorted               |
| `input.engine_capabilities` | list of string | what the ENGINE the work would run on advertises    |
| `input.principal_kind`      | string         | who is asking: `user`, `service`, `agent`, or empty |
| `input.principal_untrusted` | bool           | the CREDENTIAL is untrusted, whatever the data is   |
| `input.secret_name`         | string         | the step secret being decided, or empty             |

Enums are the short name — `PRIVILEGED`, not `CAPABILITY_PRIVILEGED`; `PURE`,
not `EFFECT_CLASS_PURE` — because that is what a policy author writes and the
proto prefix adds nothing.

**The key set is a versioned public contract.** Keys are added, never renamed and
never removed, because tenant-authored policies depend on them.

`input.engine_capabilities` is not the step's own, and the distinction matters:
a privileged **engine** is a host-level foothold whatever the step asked for. A
rule that keeps untrusted work off privileged engines reads
`input.engine_capabilities`, not `input.capabilities`.

`input.principal_untrusted` is about the **credential**, where `input.tainted` is
about the **data**. An agent step acts through this system's own API as a
principal of its tenant, and its token is marked untrusted
([ADR 0025](../.procoder/adr/0025-an-agent-step-acts-through-the-contract.md)) —
so an agent that has read nothing at all is still an untrusted caller, and a
rule can refuse it an effect it allows a person:

```yaml
- id: agents-do-not-deploy
  expression: '!input.principal_untrusted || input.effect_class != "AT_MOST_ONCE"'
  reason: an agent may ask for an at-most-once action, and a person decides it
```

`input.secret_name` is set when the decision is about one secret a step
declares ([ADR 0030](../.procoder/adr/0030-a-secret-is-redeemed-on-its-tenants-subject-and-issued-under-policy.md)).
At dispatch the scheduler decides the step, and then each of its declared
secrets as a decision of its own, with `input.subject` `secret:<name>` and every
other key exactly as it was for the step. A rule that never reads
`input.secret_name` therefore answers a secret's decision the way it answered
the step's; the key is present, and empty, for every other decision, so a rule
reading it does not fail closed on them. A refused secret is recorded as the
step's `STEP_POLICY_DENIED`, naming the secret, and the run fails:

```yaml
- id: signing-key-for-releases-only
  expression: 'input.secret_name != "release-signing-key" || input.tier == "release"'
  reason: only the release tier may read the signing key
```

## Rules worth having

Untrusted data may not reach anything effectful. Taint is a policy decision
rather than a Go one precisely so an operator can read, audit and change it
([ADR 0015](../.procoder/adr/0015-agent-loops-are-bounded-nodes-and-untrusted-data-is-tainted.md)):

```yaml
- id: untrusted-data-stays-pure
  expression: '!input.tainted || input.effect_class == "PURE"'
  reason: a step acting on untrusted data may have no external effect
```

Untrusted work does not run on a privileged engine:

```yaml
- id: untrusted-off-privileged-engines
  expression: '!input.tainted || !("PRIVILEGED" in input.engine_capabilities)'
  reason: untrusted data may not run on an engine that holds host privilege
```

An unspecified effect class is treated as effectful, everywhere. It is not a
gap to be lenient about:

```yaml
- id: effect-class-must-be-declared
  expression: input.effect_class != "UNSPECIFIED"
  reason: every step declares what it does to the world
```

Only signed artifacts, from upstreams this deployment named:

```yaml
- id: signed-from-known-upstreams
  expression: input.signed && input.upstream in ["registry.internal", "ghcr.io/acme"]
  reason: plugins run here only if signed and from a registered upstream
```

## Testing a policy

`dhole policy test` evaluates one hypothetical step against a policy file,
locally, talking to no server. It is `internal/policy`'s own CEL engine, not a
re-implementation, so what it says is what the control plane would decide.

The exit code **is** the answer, which is what lets it gate in CI:

```
dhole policy test --policy policy.yaml --signed
dhole policy test --policy policy.yaml --signed \
  --capability PRIVILEGED --taint-source git-webhook --expect-allow=false
```

`--expect-allow=false` makes the refusal the passing outcome — that is how you
assert a policy still says no to the thing you wrote it for. A policy test suite
worth having is mostly `--expect-allow=false` cases.

The flags map one-to-one onto the variables above: `--tier`, `--tenant`,
`--subject`, `--plugin-ref`, `--effect-class`, `--capability` (repeatable),
`--engine-capability` (repeatable), `--signed`, `--upstream`, `--secret-name`,
and `--taint-source` (repeatable). Naming any taint source is what makes the step
tainted: there is no run in which data is untrusted by nobody, so a `--tainted`
flag separate from its sources could describe a state the system cannot produce.

## Cost, and why it is bounded

One rule's evaluation is capped. CEL has no recursion and no unbounded loop, but
it does have comprehensions, and nesting a few of those over modest lists
multiplies into work no dispatcher should wait for. Past the limit, evaluation
is an error — and like every other error here, it denies. Real policies cost
tens against a limit of a million; if you hit it, the rule is doing something
that does not belong in a dispatch path.
