# 0002 — Effect classes govern caching and retry

Status: accepted
Date: 2026-09-09

## Context

Dhole is a general workflow engine, not a CI tool, so it has to host two populations of
step with incompatible assumptions. The CI population is pure: same inputs, same outputs,
cache it, rerun it freely. The automation population is side-effecting: "send the Slack
message", "charge the card", "file the complaint" must not re-execute on retry. These
cannot both be the ambient default, and picking either one silently breaks the other.
The bus adds urgency: JetStream delivery is at-least-once, so duplicate execution is a
routine event rather than a rare failure.

## Decision

Every step declares an effect class, and both cache policy and retry policy derive from
it:

- `pure` — cacheable, content-addressed, retried freely and automatically.
- `idempotent` — never cached; retried automatically, with a required idempotency key.
- `at-most-once` — never cached; never retried automatically. Requires a keyed or
  human-approved replay, and an exclusive lease claimed before execution.

Rejected: inferring purity from the step type or plugin, which is guesswork the plugin
author is better placed to make explicit. Also rejected: a single `retry: true/false`
flag, which conflates "is it safe to run twice" with "should we try again".

## Consequences

Easier: the cache never has to reason about side effects, because it only ever applies to
`pure`. Retry, replay, and duplicate-delivery handling all read one field. Policy can
refuse a whole class of step for a trust tier without inspecting behaviour.

Harder: it is one more mandatory declaration on every step, and a wrong declaration is
dangerous rather than merely inconvenient — an effectful step mislabelled `pure` will be
cached and skipped, or retried into a double charge. Plugin manifests must declare the
class they operate under so the value can be defaulted and validated rather than typed by
hand each time.
