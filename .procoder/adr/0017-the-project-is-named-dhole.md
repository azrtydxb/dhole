# 0017 — The project is named Dhole

Status: accepted
Date: 2026-09-09

## Context

The name appears in the binary, the schema namespace, the repository and the plugin
identity format, so it needed settling before those existed. The criteria were: short
enough to type constantly, unclaimed in the CI and infrastructure namespace, and a
metaphor matching the architecture rather than decorating it. An animal name was a
requirement.

## Decision

Dhole — the pack-hunting wild dog. Binary `dhole`, schema namespace `dhole.v1`.

Conflict checks ruled out most candidates. Octopus had the best metaphor of any considered
(each arm carries its own neural cluster and keeps acting when disconnected from the
central brain, which is exactly the control plane / data plane split in 0004) but Octopus
Deploy owns that name in CI/CD. Heron is Apache Heron, Kestrel is the ASP.NET server, and
Ant, Badger, Otter, Rook and Capybara are all taken in adjacent tooling. Magpie was ruled
out late: Apache Magpie and Open Raven's Magpie are both dev-tooling projects. Pika was
chosen and then reversed — seven prominent projects carry it, including the Python AMQP
client, which sits in the same messaging-infrastructure neighbourhood as this system.
Shrike and Nuthatch were viable, with better caching metaphors, but more crowded.

Dhole's existing uses are a small containerised remote-desktop tool and a PHP cryptography
library. Nothing in CI, workflow or orchestration.

## Consequences

Easier: the namespace is clear, so package names, the org, the domain and search results
are ownable rather than contested. Five letters, unambiguous to type and say.

Harder: the metaphor is about coordination generally and says nothing about caching or
pipelines, so it carries less explanatory weight than Shrike or Pika would have. The word
is unfamiliar to most English speakers and will be mispronounced and misspelled, which is
a mild ongoing discoverability cost.
