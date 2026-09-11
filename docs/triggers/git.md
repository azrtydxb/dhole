# The `git` trigger

`kind: git` — fires on a forge's webhook: GitHub, Gitea or Forgejo.
`internal/trigger/git`.

A git push is **one** event source here and never the privileged one
([ADR 0007](../../.procoder/adr/0007-triggers-are-pluggable-and-bind-to-typed-pipeline-inputs.md)).
The pipeline it starts declares inputs like any other and cannot tell a push
from a cron boundary. Dhole is not a CI tool; CI is one profile of it.

## Configuration

| Field          | Meaning                                                         |
| -------------- | --------------------------------------------------------------- |
| `ID`           | identity within the tenant                                      |
| `TenantID`     | the tenant every fire lands in                                  |
| `Secret`       | the shared secret configured on the forge. **Required**         |
| `Binding`      | maps pipeline inputs to the event fields below                  |
| `Pipeline`     | the definition binding and payload are checked against          |
| `MaxBodyBytes` | defaults to 5 MiB — forge payloads carry every commit in a push |

`Secret` is required because a webhook endpoint that cannot verify anything is
an unauthenticated pipeline trigger.

## Event fields a binding may read

`ref`, `branch`, `commit_sha`, `repository`, `clone_url`, `pusher`, `event`,
`flavour`, `trigger_id`, `kind`.

`flavour` is `github`, `gitea` or `forgejo` — a pipeline that runs for three
forges may legitimately need to know which one it was. `DefaultEvents` is
`["push"]`: firing on every comment on every issue is a pipeline running a
hundred times a day for no reason.

## The three rules, each a way this stops being a trigger

**The signature is verified before the payload is parsed**, in constant time, and
a **missing** signature is refused exactly as hard as a wrong one. An endpoint
that verifies a signature when it finds one and fires when it does not has no
signature check at all: the attacker omits the header.

**The body is bounded before it is read**, like every public endpoint's.

**Everything the payload produces is tainted**
([ADR 0015](../../.procoder/adr/0015-agent-loops-are-bounded-nodes-and-untrusted-data-is-tainted.md)).
A webhook body is attacker-controlled by definition — anyone who can open a pull
request can choose a branch name — and this is the boundary the taint model is
defined at. Nothing downstream can add a mark that was not applied here, and
nothing downstream can remove one.

What a tainted step may then do is a policy decision, not a hard-coded rule. See
[policy authoring](../policy.md) for the rules worth writing.

## The inputs it binds

What happens to them after the event arrives — the run log they land in, the
taint mark they keep, and the two refusals that stop a run starting on a value
the pipeline cannot use — is the same for every kind:
[what a trigger's bound inputs become](bound-inputs.md).
