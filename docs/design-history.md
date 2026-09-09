# Design history

How Dhole was designed, in the order it happened, including what was rejected and what was
reversed. This exists so that someone joining later does not re-litigate settled ground, and
can tell the difference between a decision that was argued and one that was merely assumed.

The ADRs under `.procoder/adr/` are the authoritative record of *what* was decided. This
document is the record of *how* — the reasoning chain, the alternatives, and the mistakes.

## Origin

The project began as a question about tooling, not a plan to build anything: *is there an
open-source tool that does pipelines like GitLab CI, without the overhead?*

The answer was Woodpecker CI — the community fork of Drone, Apache-2.0, a Go server plus
agents, `.woodpecker.yml` in the repo, GitLab CI's mental model minus the forge. Others
considered and set aside: Drone (relicensed and effectively frozen), Concourse (elegant but
heavy), Tekton and Argo Workflows (Kubernetes-dialect YAML, more overhead not less),
Forgejo/Gitea Actions (viable if a forge is wanted alongside), Dagger (pipelines-as-code
with no server), Laminar (minimal to the point of being cron with a web UI).

The follow-up question is what produced this project: *if we built our own, what would we
improve?*

## The critique that shaped everything

Nearly every weakness of the Woodpecker/Drone/GitLab CI family traces to one decision: a
pipeline is an ordered list of steps sharing a mutable workspace volume. That single choice
makes caching impossible to do correctly (a step's inputs are "the whole filesystem, mutated
by unspecified predecessors"), makes parallelism unsafe, makes runs non-reproducible, and
makes local execution impossible — which is why debugging CI means pushing commits.

The fix — steps declare typed inputs and outputs, and the DAG is derived from them — is what
BuildKit, Bazel and Dagger all arrived at, and what no mainstream *CI server* had adopted.
That gap is the reason to build rather than adopt. It became ADR 0001, and everything else
either follows from it or had to be made compatible with it.

Secondary improvements identified at the same time, all of which survived into the spec:
local execution identical to remote, composable configuration with dynamic pipelines,
non-ambient secrets, isolation that means something, real observability, step-level debug
attach, typed plugins, scheduling with fair-share, and provenance by default.

## How the scope grew, in four steps

**Step one — engines.** The owner wanted execution backends beyond containers: VMs,
Kubernetes, bare processes, shells. The key insight in response was that this is *two*
orthogonal axes, not one — *where* a step runs (engine) and *how long* the sandbox lives
(lease scope) — and that "containers are slow" is usually a lease and image-pull problem
rather than an engine problem. Both became ADR 0006. A consequence surfaced immediately:
a `pool` lease reuses state that cannot be hashed, so cacheability had to become a computed,
displayed property rather than an assumption.

**Step two — the editor.** An n8n-style drag-and-drop canvas generating the underlying
definition, with the same operations available over an API so an agent could work alongside
the user. The design risk here is well documented by the tools that died of it — Blue Ocean,
Xcode storyboards — where the GUI and the text file fight over ownership. Two decisions
defused it: the GUI has no privileged endpoints (ADR 0013), and wires are typed artifact
edges between ports rather than n8n-style data-flow arrows, so the picture and the execution
model are the same object.

**Step three — everything-pipeline.** The owner then made explicit that this must not be a
CI tool: triggers are API calls, schedules, queues and events, and LLM/agent workloads are
first-class. This forced the single hardest conflict in the design — CI assumes purity and
free retries, automation assumes side effects that must never be replayed — resolved by
declaring an effect class per step and deriving both cache and retry policy from it
(ADR 0002). It also forced durable, event-sourced runs (ADR 0003), since automation
workflows wait days rather than minutes.

**Step four — the deployment shape.** Go core, a message bus, React front end. The owner
then corrected an assumption in the proposed architecture, and the correction improved it:
engines are not libraries loaded into the server but separate processes in any language,
reading jobs from the bus and publishing status back, so a control-plane restart is a
non-event. That is ADR 0004.

## Two positions that were reversed

Recorded because reversals are the most useful part of a history.

**The reattach problem dissolved.** Before the control-plane/data-plane split was stated,
the design assumed the control plane owned execution and therefore needed a `reattach`
call on the executor interface to recover an in-flight sandbox after a restart. Once engines
own execution and publish to a durable stream, there is nothing to reattach to — recovery is
replay from a durable consumer position. The concern was real under the earlier assumption
and simply stopped existing under the better one.

**Pika was chosen and then withdrawn.** The name was selected, then a conflict check found
seven prominent projects using it — including the Python AMQP/RabbitMQ client, which sits in
the same messaging-infrastructure neighbourhood as this system. The lesson worth carrying:
the check should have run *before* the choice, not after. Dhole was picked on the same
criteria with the check done first (ADR 0017).

## What the interview settled that the ADRs did not

The spec interview closed the gaps the architecture records left open. The consequential
answers:

- **Platform scale** — tens of thousands of runs per day, thousands of concurrent steps,
  hundreds of engines. This demoted SQLite to a development target, made clustered NATS and
  Postgres primary, and pulled scheduler fairness into v1 rather than leaving it deferred.
- **All three profiles in parallel** — CI, automation and agent acceptance pipelines
  developed together rather than in sequence, which makes the wire schema and API contract
  the critical path, since three consumers depend on them at once.
- **Full canvas in v1**, three executors (containerd, process, Kubernetes), four triggers,
  Linux amd64 and arm64 on both sides. The VM executor and the macOS/Windows/RouterOS
  targets it unlocks were moved explicitly out of v1 scope — designed for, not built.
- **Effect classes inherit from the plugin manifest** rather than being restated on every
  step, so ordinary pipelines carry no policy boilerplate and the safe value is the default.
- **`github.com/azrtydxb/go-ai-sdk`** — the owner's own Apache-2.0 Go port of the Vercel AI
  SDK, 39 providers, zero root dependencies. Finding it materially shrank the agent work: its
  `agent` package already provides `maxSteps` bounded iteration, tool-call approval, `AsTool`
  for exposing granted steps as agent tools, typed suspension, and `GenerateObject` for
  schema-validated output. Dhole adds model fingerprinting, token ceilings and call
  recording on top rather than building an agent runtime.

## Named alternatives and why they lost

| Decision | Chosen | Rejected, and why |
|---|---|---|
| Execution model | Typed content-addressed DAG | Shared workspace plus a cache plugin — the cache is then advisory and wrong at the edges |
| Bus | NATS + JetStream | Kafka (no request/reply, wrong shape for dispatch), RabbitMQ (less capability per unit of ops), Redis Streams (insufficient durability) |
| Run state | Event-sourced in the store | JetStream as the log — conflates delivery retention with domain history |
| Definition store | Server DB primary, one-way git mirror | Git-primary (forces CST-preserving editing, fails with no repo), bidirectional sync (where these tools lose data) |
| Cache reclamation | Refcount from retained runs | TTL+LRU (can evict a blob a visible run needs), never evict (unusable at scale) |
| Policy language | CEL | Rego/OPA (heavier, learning curve), a home-grown DSL (accretes into a language), compiled Go (no per-tenant policy) |
| Licence | Apache-2.0 | AGPL-3.0 (would have preserved SaaS protection, and the bus boundary means it would not have deterred plugin authors — rejected in favour of adoption), BSL (not OSI open source), MIT (no patent grant) |
| Plugin index | Federated, locally mirrored | A hosted community index — would make the project the trust anchor and abuse desk for third-party code running with production credentials |
| Name | Dhole | Octopus (best metaphor of all — each arm has its own neural cluster and keeps acting when disconnected — but Octopus Deploy owns it in CI/CD), Pika (crowded), Magpie (Apache Magpie and Open Raven Magpie are both dev tooling), Heron, Kestrel, Ant, Badger, Otter, Rook, Capybara — all taken |

## Still genuinely open

Not deferred by oversight — these have no answer yet and will need one:

- Where the executor lease sits during a control-plane restart is settled by ADR 0004, but
  the orphan-detection window it creates (control plane declares a live engine dead during a
  partition and redispatches) is handled by fencing tokens whose behaviour under clock skew
  has not been specified.
- The set of variables and functions CEL policies may reference becomes a versioned public
  contract the moment tenants author their own policies. It has not been enumerated.
- Retention defaults are tiered by data class, but the actual default windows are unset.
