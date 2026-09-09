# 0018 — Dhole is licensed Apache-2.0

Status: accepted
Date: 2026-09-09

## Context

0014 commits to three audiences at once: a single-operator homelab, open-source
self-hosters, and an eventual commercial hosted offering. The repository was created
public with no license, which by default means all rights reserved — the opposite of the
stated intent, and a state that silently blocks anyone from using or contributing.

The license choice is unusually consequential here because 0011 depends on a third-party
plugin ecosystem and 0004 invites third-party engines written in any language. Whatever
deters those authors undermines two other decisions.

## Decision

Apache-2.0, with copyright held by azrtydxb.

It is the default expectation for infrastructure and CI tooling, carries an explicit
patent grant that matters more for infrastructure than most projects assume, and imposes
the least possible friction on the plugin and engine authors that 0011 and 0004 depend on.

Rejected: AGPL-3.0, which would have preserved protection against a vendor hosting Dhole
commercially — and whose usual cost does not apply here, since engines and plugins are
separate processes across a protocol boundary and therefore not derivative works. It was
rejected in favour of adoption. Rejected: BSL 1.1, the strongest commercial protection but
not OSI open source, which carries a real community cost that has to be stated plainly.
Rejected: MIT, the same commercial exposure as Apache-2.0 with less legal precision and no
patent grant.

## Consequences

Easier: contributors, plugin authors and engine authors face no licensing question.
Corporate adoption and vendoring are unobstructed. The patent grant reduces risk for
downstream users.

Harder: nothing prevents a cloud vendor from operating Dhole as a hosted service in
competition with our own, and no later license change can reach code already released
under these terms. Commercial differentiation therefore has to come from something other
than the license — proprietary control-plane features, or the hosted operation itself.
