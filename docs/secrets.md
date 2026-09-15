# Step secrets

A step that pushes an image to a private registry, calls an authenticated API or
signs an artifact needs a credential. In Dhole a step **names** the secret it
needs, an operator **configures** that name for a tenant, and the value reaches
the step's process at exec time and nowhere else
([ADR 0027](../.procoder/adr/0027-a-step-declares-the-secrets-it-needs-and-the-plane-issues.md)).

## Declaring a secret in a pipeline

```yaml
steps:
  - id: push
    plugin_ref: 'command:{"args":["/bin/sh","-c","printf %s \"$REGISTRY_PASSWORD\" | docker login -u robot --password-stdin harbor.example"]}'
    effect_class: EFFECT_CLASS_IDEMPOTENT
    capabilities: [CAPABILITY_SECRETS]
    secrets:
      - name: harbor-robot # the secret, as the operator configured it
        env: REGISTRY_PASSWORD # the variable the step's process sees it in
```

- `name` is the secret's name for this tenant. It is not a value, and not the
  name of an environment variable on the control plane.
- `env` is the variable the step receives the value in. It must be a valid
  variable name, and two secrets in one step may not bind the same one.
- A step declaring any secret **must** declare `CAPABILITY_SECRETS`. That
  capability is what routes the step to an engine able to redeem a secret, and
  it is what policy sees in `input.capabilities`. A step that declares secrets
  without it is refused.

The definition carries the names only, so it is safe to mirror to git and to
archive with every run.

## Adding a secret to an existing step

A secret binding and a capability are each one editing operation
([ADR 0028](../.procoder/adr/0028-a-steps-declarations-are-edited-one-element-at-a-time.md)),
so declaring a secret on a step that already exists does not mean replacing the
step. From the CLI, two edits — each prints the operation that undoes it:

```sh
dhole pipeline apply ci --base "$REV" \
  --operation '{"setStepSecret":{"stepId":"image","env":"NEXUS_PASSWORD","name":"nexus-push"}}'
dhole pipeline apply ci --base "$NEXT_REV" \
  --operation '{"setStepCapability":{"stepId":"image","capability":"CAPABILITY_SECRETS"}}'
```

`setStepSecret` with an env the step already binds rebinds it in place, and
`"remove":true` unbinds it. `setStepCapability` with `"remove":true` withdraws
the capability. In the editor, the inspector shows both lists for the selected
step and makes the same operations.

Neither operation refuses a step that binds secrets without
`CAPABILITY_SECRETS`, so the two edits above may come in either order. `dhole
pipeline validate` reports such a step as an error, in the words the scheduler
refuses it in.

## Providing a secret as an operator

Step secrets are configured per tenant, explicitly, even while a deployment
serves one tenant.

With the single binary, name the tenant, the secret, and the environment
variable the plane reads the value from:

```sh
HARBOR_ROBOT_PASSWORD=... dhole serve --secret default/harbor-robot=HARBOR_ROBOT_PASSWORD
```

`--secret` is repeatable. The plane refuses to start on a malformed spec, an
invalid tenant, a name given twice, or a variable that is unset or empty — so a
missing credential is found at start-up, not by a run days later.

With the Helm chart, each entry names a Kubernetes Secret and the key holding
the value; the value is never in the values file:

```yaml
controlPlane:
  secrets:
    - name: harbor-robot
      tenant: default
      existingSecret: harbor-robot
      key: password
```

The chart refuses to render an entry with no `tenant`, `existingSecret` or
`key`.

Step secrets are **separate** from `--model-secret` and
`controlPlane.modelSecrets`, which hold the credentials the plane uses for its
own `builtin:llm` calls
([ADR 0024](../.procoder/adr/0024-the-plane-redeems-its-own-secrets.md)). A model
key is not readable by pipeline steps unless it is also configured here.

## What happens at run time

1. When the step is ready, the scheduler checks that every declared secret is
   configured for the step's tenant and that the step declares
   `CAPABILITY_SECRETS` — **before** it claims a lease, takes a concurrency slot
   or writes anything to the outbox.
2. It mints one short-lived, single-use handle per secret through the plane's
   broker, fresh for this attempt, and puts the handles — never the values — in
   `JobDispatch.secrets`. A handle expires ten minutes after it is issued
   (`scheduler.DefaultSecretTTL`): long enough for the dispatch to be picked up
   and its sandbox acquired, not the length of the step.
3. The engine redeems each handle over the bus immediately before running the
   command, on its tenant's subject `secret.redeem.<tenant>`, and binds the
   value into the command's environment only. It is not placed in the
   executor's sandbox spec, which a backend such as Kubernetes stores
   ([wire contract](wire-contract.md), "Secrets"). The plane refuses a handle
   presented on another tenant's subject, and an engine's tier credential may
   not ask on one.
4. When the attempt ends — succeeded, failed, cancelled or lost — the plane
   revokes whichever of its handles were not redeemed. Cancelling a run revokes
   every handle of the run, including those of a dispatch still waiting in a
   queue ([ADR 0030](../.procoder/adr/0030-a-secret-is-redeemed-on-its-tenants-subject-and-issued-under-policy.md)).

A retry gets new handles. A dispatch that waits longer than the handle's expiry
has its redemption refused, fails, and is retried under its effect class.
Handles live in the memory of the plane that issued them; on a deployment of
several planes, one that did not issue a handle cannot revoke it, and the expiry
is what bounds it.

## Steps the plane runs itself

A `builtin:` step — `builtin:llm`, `builtin:wait`, `builtin:loop` and the rest —
runs on the control plane and is never dispatched to an engine, so it has
nowhere to receive a step secret. A `builtin:` step that declares `secrets:` is
refused with `STEP_SECRET_UNAVAILABLE` saying so, and the run fails. A builtin
that needs a credential names it in its own configuration, as `builtin:llm`
does with `api_key_secret`.

## When a secret is not there

A step declaring a secret the plane does not hold for its tenant is **never
dispatched**. The run's log records `STEP_SECRET_UNAVAILABLE` naming the secret
— once, however many scheduler passes reach the step together — and the run
fails. It is not retried: it is a deployment fault, and the fix is to
configure the secret and run the pipeline again. A step never runs with the
variable missing or empty.

## What never happens to the value

Dhole never writes the value to the run's event log, to a dispatch at rest in
the outbox, to the content-addressed store, to a cache entry or its key, or to
an error message. `TestAStepReceivesTheSecretItDeclaresAndTheValueIsRecordedNowhere`
runs a step through the single binary and then searches every run event, every
stored outbox row, the run database and every file under the plane's state
directory for the value.

What Dhole cannot prevent is a step printing its own environment. A value the
step writes to stdout is in its log, and a value it writes to an output port is
in the CAS; treat step code holding a secret the way you would treat any
process holding one.

## Policy

A step declaring secrets must declare `CAPABILITY_SECRETS`, which a tier policy
can refuse with a rule on `input.capabilities`. Each declared secret is also
decided on its own, with the step's facts plus `input.secret_name`
([policy](policy.md)); a refusal is the step's `STEP_POLICY_DENIED`, naming the
secret, and every decision — allows included — is a row in `policy_audit`.

`dhole serve` always evaluates a dispatch policy
([ADR 0032](../.procoder/adr/0032-the-plane-always-evaluates-a-dispatch-policy-over-a-floor.md)).
Unconfigured, it is the built-in default, which permits every secret; replace it
with `dhole serve --policy FILE` or `controlPlane.policy` to restrict one:

```yaml
- id: registry-robot-for-trusted-pushes-only
  expression: 'input.secret_name != "harbor-robot" || (input.tier == "trusted" && !input.tainted)'
  reason: the registry robot is for trusted pushes, never for a step reading webhook data
```

`input.tainted` is set at dispatch from the step's inputs, so a rule can keep a
secret away from a step that reads data an untrusted trigger admitted.
