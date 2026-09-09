# 0007 — Triggers are pluggable and bind to typed pipeline inputs

Status: accepted
Date: 2026-09-09

## Context

Dhole is explicitly not a CI tool, so a git commit is one way a pipeline starts, not the
privileged way. Runs need to be startable by schedule, inbound webhook, direct API call,
queue message, object-store or file change, email, MQTT, another pipeline's completion, a
human, or an agent. GitLab CI and GitHub Actions are structurally stuck because the
commit is baked into the model rather than being one event source among many.

## Decision

Triggers are a pluggable subsystem shaped exactly like executors: an interface plus
implementations, each normalising its event into pipeline inputs. The pipeline declares
typed inputs; triggers bind to those inputs. A pipeline therefore does not know or care
what started it.

Rejected: a fixed trigger enum with git privileged, which is the thing we are reacting
against. Rejected: triggers as pipeline steps, which cannot express a pipeline that has
not started yet.

## Consequences

Easier: the same pipeline is reachable from cron, curl, an agent, and a webhook with no
change to its definition. New event sources are additive. Testing a pipeline means
supplying inputs directly, with no trigger involved.

Harder: input typing and validation become load-bearing, since a malformed webhook
payload is now a type error at the boundary rather than a runtime surprise mid-run. Each
trigger needs its own authentication, deduplication and replay-protection story. Data
arriving from untrusted triggers must be tainted at the boundary (see 0015).
