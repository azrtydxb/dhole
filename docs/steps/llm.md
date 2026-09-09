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
