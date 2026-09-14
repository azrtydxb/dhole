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
   command, and binds the value into the command's environment only. It is not
   placed in the executor's sandbox spec, which a backend such as Kubernetes
   stores ([wire contract](wire-contract.md), "Secrets").

A retry gets new handles. A dispatch that waits longer than the handle's expiry
has its redemption refused, fails, and is retried under its effect class.

## When a secret is not there

A step declaring a secret the plane does not hold for its tenant is **never
dispatched**. The run's log records `STEP_SECRET_UNAVAILABLE` naming the secret,
and the run fails. It is not retried: it is a deployment fault, and the fix is to
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

Today the only control over which steps may read secrets is the `SECRETS`
capability, which a tier policy can refuse with a rule on `input.capabilities`.
There is no rule keyed on an individual secret's name. The dispatch path does
not set `input.tainted`, and `dhole serve` does not yet wire the dispatch-time
policy check at all; both are recorded as open in the plan.
