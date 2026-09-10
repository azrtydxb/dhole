# The `llm` step type

A model call that asks for a JSON object, holds the answer to a declared schema,
and records what the call was and what it cost. Implemented in
`internal/steps/llm`.

## The effect class is computed, not declared

This is the one place in the system where a step's effect class is derived
rather than taken on trust.

A model call is `pure` — the class that promises the same inputs produce the same
output — only when nothing about it can move: temperature 0 **and** a
digest-pinned model. Everything else is `idempotent`: retried, never cached.

An alias like `latest` silently repoints, and a cache key folded over an alias
serves yesterday's model's answer as today's with nothing in the run to say so.
A pipeline author writing `model: claude-latest` has not thought about
repointing and should not have to.

## Which model answered is read from the response

`Fingerprint` takes the **response**, never the configuration, and deliberately
has no way to see what was asked for. The name a pipeline asks for is very often
an alias, so "which model produced this output" can only be answered by the
output.

A response that does not name its model yields the empty string and
`ErrModelUnattributable`. There is no fallback, because every candidate fallback
is the alias — and answering "the model was whatever we asked for" is exactly
the false claim this exists to prevent. A call nobody can attribute cannot be
cached and should not be recorded as if it could.

## A validated object, or nothing

A model that answers with unparseable JSON is retried. A step that runs out of
attempts fails with the provider's own error. It never returns the part that
parsed: a half-formed object flowing downstream fails somewhere else, later,
with no connection to the model that caused it — and the failure it replaced was
visible.

## The token ceiling halts the run

A model that hits its budget stops mid-sentence, and the JSON it produced up to
that point often parses. A truncated answer that looks complete is the worst
outcome available here, so a ceiling breach fails the step and closes the run
rather than trimming and carrying on.

## Where the credential comes from

A model configuration **names** a secret and never carries one. A step's config
carries `api_key_secret`, and the control plane resolves that name at CALL time
through its own broker — the same single-use handle, the same issuer-enforced
expiry and the same refusal an engine's `SecretRef` gets (ADR 0024,
`docs/wire-contract.md` under "Secrets"). There is exactly one way a credential
reaches a running thing in Dhole, and the plane is not an exception to it.

Three consequences:

- **This package never sees a key.** It is handed a constructed model, which is
  the primary reason no key can leak from it; the redaction in `record.go` is
  the second net, not the first.
- **Redeemed per call, never cached.** A provider's key rotates without
  restarting the plane, and a run that takes an hour holds no value for an hour.
- **An unresolvable credential fails the step by NAME.** Never an empty key
  handed to a provider, which comes back as an authentication failure naming no
  secret at all.

A deployment supplies the values: `dhole serve --model-secret NAME=ENVVAR`, or
the chart's `controlPlane.modelSecrets`, each reading from a Kubernetes Secret.
A plane given none fails a step that names one, with that reason.

## Recording prompts

Recorded calls carry the prompt, the response, token counts, latency and the
model fingerprint. A recorder with **no retention window is refused**
(`ErrRetentionRequired`): this table holds whatever the pipeline fed the model,
and "kept for ever because nobody configured a window" is the default that turns
an operational record into a breach. A deployment that genuinely wants a decade
says a decade.

## Refusals

| Error                    | Means                                                  |
| ------------------------ | ------------------------------------------------------ |
| `ErrNoObject`            | no parseable JSON across every attempt                 |
| `ErrObjectInvalid`       | JSON that does not satisfy the declared output schema  |
| `ErrTokenCeiling`        | over budget; the answer is discarded and the run halts |
| `ErrProviderCall`        | the provider refused the call outright                 |
| `ErrModelUnattributable` | the response does not name the model that produced it  |
| `ErrRetentionRequired`   | a recorder was configured with no retention window     |
