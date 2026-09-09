# Policy authoring

Policy is the one place a trust-tier decision is made and the one place it is
recorded ([ADR 0012](../.procoder/adr/0012-policy-is-a-first-class-subsystem-keyed-on-trust-tier.md)).
The scheduler, the plugin registry, the dispatcher and the secret resolver all
ask the same question here, so "why was this allowed" has one answer from one
log rather than four subtly divergent notions of trust.

Rules are written in CEL ([ADR 0019](../.procoder/adr/0019-policy-is-expressed-in-cel.md)):
data rather than code, so a tenant can supply its own without a rebuild, and not
Turing-complete, so evaluation always terminates.

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

Enums are the short name — `PRIVILEGED`, not `CAPABILITY_PRIVILEGED`; `PURE`,
not `EFFECT_CLASS_PURE` — because that is what a policy author writes and the
proto prefix adds nothing.

**The key set is a versioned public contract.** Keys are added, never renamed and
never removed, because tenant-authored policies depend on them.

`input.engine_capabilities` is not the step's own, and the distinction matters:
a privileged **engine** is a host-level foothold whatever the step asked for. A
rule that keeps untrusted work off privileged engines reads
`input.engine_capabilities`, not `input.capabilities`.

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
`--engine-capability` (repeatable), `--signed`, `--upstream`, and
`--taint-source` (repeatable). Naming any taint source is what makes the step
tainted: there is no run in which data is untrusted by nobody, so a `--tainted`
flag separate from its sources could describe a state the system cannot produce.

## Cost, and why it is bounded

One rule's evaluation is capped. CEL has no recursion and no unbounded loop, but
it does have comprehensions, and nesting a few of those over modest lists
multiplies into work no dispatcher should wait for. Past the limit, evaluation
is an error — and like every other error here, it denies. Real policies cost
tens against a limit of a million; if you hit it, the rule is doing something
that does not belong in a dispatch path.
