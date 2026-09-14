# 0027 — A step declares the secrets it needs and the plane issues them per attempt

Status: accepted
Date: 2026-09-14

## Context

A real CI/CD pipeline on Dhole has to push an image to a private registry, and the registry
refuses anonymous push. No Dhole step could be given a credential. Nearly all of the machinery
existed: `JobDispatch.secrets` carries `SecretRef`s, an engine redeems them over the bus and
binds each value at exec time only (conformance case `secret-redemption`), and the plane
serves a broker (ADR 0024). What was missing was the producer half — `Step` had no field to
ask for a secret, `scheduler.buildDispatch` never set `JobDispatch.secrets`, and the only
plane-held source was the one ADR 0024 introduced for `builtin:llm`'s own API key.

Three choices had to be made, and each had a tempting wrong answer.

Where a step asks. Putting a secret's value, or the name of an environment variable holding
it, into `Step.config` or `command:` env would put a credential into the definition — which
is content-hashed, mirrored to git and archived with every run. A step must name the secret,
never carry it.

When the handle is minted. Minting at definition save, or once per run, gives a bearer
credential a lifetime measured in runs; a redelivered or retried dispatch would carry a
handle an earlier attempt already spent or could still spend.

Where the value comes from. The obvious reuse is the plane's existing source: the secrets
an operator configured for `builtin:llm`. That would widen, on upgrade, who can read a model
provider's key from "the plane, per call" to "any pipeline author in the tenant, into any
process", without the operator having changed anything.

## Decision

A `Step` declares `repeated StepSecret secrets`, each binding a secret NAME to the
environment variable the step sees it as. The declaration is names only; it is part of the
definition and carries no value.

The scheduler issues, for each declaration, one `SecretRef` through the plane's broker at
dispatch time: scoped to the step's tenant, minted for exactly one (run, step, attempt), with
an expiry that covers the dispatch's pickup and sandbox acquisition rather than the whole
run. Every attempt gets fresh handles. A declared secret the plane does not hold for that
tenant refuses the step BEFORE any lease, slot or outbox row exists, records a
`STEP_SECRET_UNAVAILABLE` event naming the secret, and fails the run — never a dispatch
without it, never an empty variable.

A step declaring a secret must also declare `CAPABILITY_SECRETS`. That is not a new rule but
the existing one made unavoidable: the capability is what routes a dispatch to an engine
that can redeem (docs/wire-contract.md), and it is what `input.capabilities` shows policy.
A step that received a secret without declaring the capability would be invisible to both.

Step secrets come from a source SEPARATE from the plane's own (ADR 0024's). An operator
names them explicitly, per tenant — `dhole serve --secret TENANT/NAME=ENVVAR`, and
`controlPlane.secrets` in the chart, reading each value from a Kubernetes Secret. The
model-secret path is unchanged, and a model credential is not available to steps unless an
operator names it a second time as a step secret.

## Consequences

A pipeline can push to a private registry, and the credential exists in exactly three
places: the operator's Secret, the plane's in-memory source and broker, and the environment
of the one process that redeemed it. It is not in the definition, the run log, the outbox
row, the CAS, the cache key, or the executor's `Spec`.

Two sources is one more thing an operator configures, and naming the same credential for a
model call and a step means naming it twice. That is the price of not widening an existing
deployment's exposure on upgrade.

The broker binds a handle to its tenant but cannot bind it to its run, step or attempt at
redemption: the wire contract's request carries the handle and nothing else, so scoping to
the attempt is by construction (one fresh single-use handle per attempt) rather than checked
on the way back. Revoking an attempt's unspent handles when the attempt ends, and a
redemption subject that is itself tenant-scoped, are both left open.

A missing secret is a configuration fault and is not retried: the run fails and a person
fixes the deployment and runs it again. A step whose dispatch sits in a queue longer than the
handle's expiry has its redemption refused, fails, and is retried under its effect class with
fresh handles.

The values are still held in a plane process for its lifetime, which is the property an
external secret manager behind the `secrets.Source` interface would replace. That remains
the separate decision ADR 0024 left open.
