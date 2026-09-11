# Dhole documentation

| Page                                         | For                                                      |
| -------------------------------------------- | -------------------------------------------------------- |
| [Quickstart](quickstart.md)                  | a pipeline running in fifteen minutes                    |
| [The engine wire contract](wire-contract.md) | the protocol every engine is written against             |
| [Writing an engine](writing-an-engine.md)    | contract, engine, conformance suite — in that order      |
| [Writing a plugin](writing-a-plugin.md)      | manifests, capabilities, effect classes, signatures      |
| [Policy authoring](policy.md)                | CEL rules, the variables they read, and how to test them |
| [Deployment topologies](deployment.md)       | laptop, homelab, cluster, and the Helm chart             |
| [Upgrades and version skew](upgrades.md)     | what "N and N-1" means for a fleet mid-upgrade           |
| [Design history](design-history.md)          | how each decision was reached, including the reversals   |

## Reference

One page per pluggable thing. `TestEveryStepTypeAndTriggerIsDocumented` reads
the source tree and requires a page for every step type, trigger kind and
executor kind it finds, so a backend added next month fails the gate until it is
documented here.

- **Step types** — [agent](steps/agent.md), [approval](steps/approval.md),
  [llm](steps/llm.md), [loop](steps/loop.md)
- **Triggers** — [schedule](triggers/schedule.md), [http](triggers/http.md),
  [git](triggers/git.md), [completion](triggers/completion.md), and
  [what their bound inputs become](triggers/bound-inputs.md)
- **Executors** — [process](executors/process.md), [kubernetes](executors/kubernetes.md)

## The decisions behind all of it

Every significant decision is an immutable record in
[`.procoder/adr/`](../.procoder/adr/), with the constraint that forced it and
the price it pays. The pages here say what the system does; the records say why,
and what was rejected.
