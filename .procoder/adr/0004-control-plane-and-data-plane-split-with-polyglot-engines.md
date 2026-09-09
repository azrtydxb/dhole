# 0004 — Control plane and data plane split with polyglot engines

Status: accepted
Date: 2026-09-09

## Context

Engines must run in places the control plane cannot reach or trust: a homelab, a customer
network behind CGNAT, a macOS build host, a GPU box, a RouterOS device. Requiring them to
be Go plugins loaded into the server, or to accept inbound connections, rules out most of
those. We also want a control-plane restart to be a non-event for work already in flight.

## Decision

The Go control plane owns the DAG, scheduling, policy, the API and the GUI. Engines are
separate processes, written in any language, that pull jobs from the bus and publish
status and logs back to it. The schema is the only contract between them; the control
plane never calls an engine directly.

Job messages are self-contained — step spec, input references, secret references plus a
credential to redeem them, output destinations — so an engine needs nothing from the
control plane once dispatched. Engines connect outbound only. The control plane consumes
status via a durable consumer with a persisted ack position, so an outage is replayed
rather than lost.

Because engines can be written in anything, the wire schema is the real public API: it is
versioned explicitly, the control plane supports N-1, and a conformance suite is published
for engine authors.

Rejected: in-process Go executors as the extension mechanism (the `plugin` package is
unusable in practice and it excludes other languages). Rejected: control plane pushing
work to engines over inbound connections, which fails behind NAT.

## Consequences

Easier: a control-plane restart does not disturb running work; engines keep executing and
keep publishing, and the control plane catches up on replay. New engine types — RouterOS,
GPU, macOS — need zero control-plane changes. Third parties can write engines.

Harder: the schema becomes a public contract that cannot be changed casually, which means
version negotiation and a conformance suite are v1 work rather than later work. Orphan
detection now needs heartbeat leases with TTL, and the resulting network-partition window
means duplicate dispatch is possible — handled by fencing tokens and, for `at-most-once`
steps, an exclusive lease claimed before any side effect. Scheduling does not advance
while the control plane is down: in-flight work is unaffected, pending work queues.
