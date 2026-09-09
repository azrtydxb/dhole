# The `http` trigger

`kind: http` — fires on an inbound POST. `internal/trigger/http`.

The plainest reading of
[ADR 0007](../../.procoder/adr/0007-triggers-are-pluggable-and-bind-to-typed-pipeline-inputs.md):
a body arrives, its fields become the pipeline's **declared** inputs, and the
pipeline never learns that a person with `curl` — rather than a cron boundary or
another pipeline — put them on the table.

## Configuration

| Field          | Meaning                                                                                  |
| -------------- | ---------------------------------------------------------------------------------------- |
| `ID`           | identity within the tenant; appears in errors and in the taint mark                      |
| `TenantID`     | the tenant every fire lands in                                                           |
| `Binding`      | maps each pipeline input to a **dotted path** in the body: `ref`, `repository.full_name` |
| `Pipeline`     | the definition binding and payload are checked against; required                         |
| `MaxBodyBytes` | defaults to 1 MiB                                                                        |
| `Untrusted`    | marks every value this trigger produces as tainted                                       |

## Both rules are about an endpoint anyone can route to

**The body is bounded before it is read.** `ReadAll` on a request body is a
memory-exhaustion vector that takes one request to exploit, and no validation
afterwards helps, because the damage is done during the read. The default
ceiling is generous for an event payload and cheap to hold; the point of the
number is that there **is** one.

**The payload is checked against the pipeline's own input schema**, and a body
that fails is refused at the boundary with the reason. That is what typed ports
buy: a malformed payload is a 400 somebody can read, not a run that dies in its
first step with the values already half-applied.

## Responses

`202 Accepted`, not 200: the run has been **started**, not finished, and a caller
that waits for a result is misreading the contract. Failures answer with one
JSON object carrying the reason — "400 Bad Request" alone tells whoever is
holding a webhook configuration nothing about which field they got wrong.

## Untrusted mode

Set `Untrusted` when the endpoint is reachable by anyone whose data you do not
control. Every value it produces then carries a taint mark naming this trigger,
and what may be done with it is a policy decision — see
[policy authoring](../policy.md). If the source is a forge webhook, use
[the `git` trigger](git.md) instead: it verifies signatures and taints
unconditionally.
