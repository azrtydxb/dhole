# The `completion` trigger

`kind: completion` — fires when another pipeline's run finishes.
`internal/trigger/completion`.

It is what makes a chain of pipelines expressible without one giant graph: build
finishes, publish starts, and neither definition mentions the other's steps. To
the downstream pipeline this is one more way its inputs arrived
([ADR 0007](../../.procoder/adr/0007-triggers-are-pluggable-and-bind-to-typed-pipeline-inputs.md)).

## Configuration

| Field                | Meaning                                                            |
| -------------------- | ------------------------------------------------------------------ |
| `ID`                 | identity within the tenant                                         |
| `TenantID`           | the tenant every fire lands in                                     |
| `UpstreamPipelineID` | the pipeline whose completion is watched                           |
| `Binding`            | names the pipeline this trigger **starts**, and maps its inputs    |
| `Pipeline`           | the downstream definition, checked against the binding             |
| `Source`             | the channel of upstream events `Start` consumes                    |
| `Registry`           | every completion trigger in one control plane, for cycle detection |

## Event fields a binding may read

`upstream_run_id`, `upstream_pipeline_id`, `outcome`, `completed_at`,
`trigger_id`, `kind`.

An `Event` carries the run event log's own vocabulary — its type is a
`runstore.EventType` — so whoever feeds this trigger passes on what was
**recorded** rather than their own reading of it.

## A failed run is not a completed run

The trigger fires on `RUN_COMPLETED` and on nothing else. Default-deny, so an
event type added later cannot quietly start meaning "success" — because a
downstream pipeline fired off a failed upstream is how a broken build gets
published.

## A completion trigger cannot fire itself

Not directly, and not around a loop. The graph is acyclic by decision and
iteration is a bounded loop node
([ADR 0015](../../.procoder/adr/0015-agent-loops-are-bounded-nodes-and-untrusted-data-is-tainted.md));
a pipeline that starts itself on completion is not one run looping, it is an
unbounded number of runs, with no iteration budget anywhere.

`New` refuses the direct case on its own. It refuses the indirect one — a to b
to c to a — when the triggers share a `Registry`, which is how a control plane
holding all of a tenant's triggers wires them up. Configure the registry: without
it, only the one-hop cycle is caught.
