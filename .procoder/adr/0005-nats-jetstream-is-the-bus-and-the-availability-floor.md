# 0005 — NATS JetStream is the bus and the availability floor

Status: accepted
Date: 2026-09-09

## Context

0004 makes the bus the only contract between control plane and engines, so its
capabilities directly constrain the architecture. We need synchronous request/reply
(capability queries, validate, plan, cancel, interactive shell attach), durable
asynchronous work dispatch with redelivery, live log streaming, an ephemeral instance
registry, and a way for engines behind NAT to participate without inbound firewall rules.
We also promised a single-binary homelab deployment, which rules out a bus that needs its
own cluster to exist at all.

## Decision

NATS with JetStream. Request/Reply covers the synchronous surface natively. JetStream
work-queue streams with pull consumers cover dispatch, giving durable delivery, explicit
ack, redelivery on engine death, and backpressure — which is also the mechanism for
fair-share scheduling. Leaf nodes and WebSocket transport let engines connect outbound
only. NATS accounts provide hard isolation and map onto trust tiers, so subject-level
authorization is enforced by the bus rather than by our code. `nats-server` embeds as a Go
library, preserving the single-binary mode.

The bus carries transport, not truth: durable run state lives in our own store (0003),
bridged by an outbox so a state transition and its published message cannot disagree.
Logs take two paths — core NATS fire-and-forget for live tailing, and a direct engine
write to object storage for the authoritative copy, with only the reference on the bus.

Rejected: Kafka (no request/reply, partitioned-log semantics are the wrong shape for job
dispatch, heavy ops). RabbitMQ (workable, less capability per unit of operational
surface). Redis Streams (insufficient durability guarantees for `at-most-once` work).

## Consequences

Easier: one dependency covers sync, async, KV, and object storage. Multi-tenant isolation
and trust tiering get enforced at the transport layer. Remote and edge engines need no
inbound network exposure.

Harder: NATS becomes the availability floor — clustering it matters more than
control-plane HA, and engines need local buffering for when the bus itself is
unreachable. Subject layout becomes part of the public contract engine authors code
against, so it must be designed deliberately and versioned. Log retention needs care to
avoid JetStream storage growth.
