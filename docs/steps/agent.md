# The `agent` step type

`plugin_ref: builtin:agent`

A model given a bounded loop and a **closed set** of things it is allowed to do.
It is a builtin rather than a plugin the registry resolves, because the action
space has to be enforced somewhere no tenant-supplied code can reach.

Implemented in `internal/steps/agent`. The rules come from
[ADR 0015](../../.procoder/adr/0015-agent-loops-are-bounded-nodes-and-untrusted-data-is-tainted.md).

## The action space is a closed set

An agent may invoke only the steps explicitly granted to it. That is a closed
set built from named grants, not a filter over everything that exists, because
the two fail in opposite directions: a filter that is wrong grants too much, and
a closed set that is wrong grants too little. Only one of those is an incident.

A grant naming a step the catalogue does not define is refused **at
configuration**. The alternative — an unknown step with an unspecified effect
class — hands the agent an action whose danger nothing can assess.

The check happens at **invocation**, not while the tool list is built. The tool
list is what the agent was offered; what has to be refused is what it asked for,
and a model that invents a plausible tool name it was never given is a Tuesday.
Every path to running a step goes through `ActionSpace.Check`.

## Taint is consulted before anything effectful runs

An agent reading a webhook body is acting on attacker-controlled data. Once that
is true, "the model decided to" and "an attacker decided to" are the same
sentence.

A **pure** action may still read untrusted data — parsing and reshaping a webhook
body is what should happen to it. Everything else may not, an unspecified effect
class included. The refusal is a `policy.Decision`, so a taint refusal reads and
audits like every other refusal rather than being a second vocabulary for "no".

## An at-most-once action is routed to the approval gate

It is not executed and then reported. The gate is the same one a person's
approval queue reads, so an agent asking to deploy appears exactly where a human
asking to deploy appears — see [the `approval` step](approval.md). A separate
agent-approval path would be a second queue nobody watches.

An `at-most-once` grant with no gate configured is refused at construction:
discovering it at the moment of a deploy is discovering it too late.

## A gated action parks the step, and an approval resumes it

The step does not fail. It **parks**: the model's loop suspends with the gated
batch unexecuted, `STEP_AWAITING_APPROVAL` goes in the run log against the
agent's own step, and the run waits for a person exactly as it would for a
human-authored gate. Nothing is running and nothing has been decided.

Where the loop stopped is written to the log as one event, `AGENT_PARKED`,
carrying the whole conversation up to and including the unanswered call, the
ids of the calls the person is being asked about, and how much of `MaxSteps`
has been spent. A run's position lives in the log and nowhere else
([ADR 0003](../../.procoder/adr/0003-runs-are-durable-and-event-sourced.md)):
the plane that parked the step may be gone by the time a person wakes up.

When a person **approves**, the gate writes `STEP_RESUMED` rather than a
verdict — the step is released, not finished — and the scheduler dispatches it
again. The step reads its own park record back and re-enters the model's loop
**at the call it stopped on**.

Three properties make that safe:

- **Nothing is replayed.** The conversation comes back verbatim, with every
  earlier tool call already answered in it, so the model is never asked to
  decide again what it has already decided and no earlier action is taken
  twice. An agent acts through Dhole's public API
  ([ADR 0025](../../.procoder/adr/0025-an-agent-step-acts-through-the-contract.md)),
  so "twice" would mean a second run started or a second operation applied.
- **The ceiling is not reset.** A resume gets `MaxSteps` minus what the parked
  loop already spent. A loop that parked on its last step is refused with
  `ErrCeilingSpent`, because otherwise "ask for something gated" would be the
  way around the bound.
- **The decision covers one batch and no more.** It is applied to the calls it
  was made about, and the window closes at the resumed loop's first model call.
  Anything the model asks for after that is a new question.

When a person **denies**, the step ends. The refusal is not handed back to the
model as a tool result it can react to: an agent told "no" and left running is
free to reach the same effect another way, and the person who refused would
have no say in what happened next. The run fails naming the approver.

There is **one gate per step per run**. An agent that parks a second time in
the same run is refused by the standing decision, and the step fails saying so
— a readable limit rather than a silent one.

## The loop is bounded

`MaxSteps` must be positive, and a non-positive value is refused at
configuration. A model that always calls a tool never stops on its own, and an
SDK's default is a number nobody chose for this pipeline.

## Refusals

| Error                   | Means                                                         |
| ----------------------- | ------------------------------------------------------------- |
| `ErrOutsideActionSpace` | the agent asked for a step it was not granted                 |
| `ErrApprovalRequired`   | an at-most-once action was requested; the gate has been asked |
| `ErrTainted`            | an effectful step was asked for on untrusted data             |
| `ErrUnbounded`          | configured without a positive step ceiling                    |
| `ErrNoGate`             | an at-most-once grant with nothing to route approvals to      |
| `ErrDenied`             | a person refused the gate; the step is over                   |
| `ErrCeilingSpent`       | the loop parked with none of its step ceiling left            |
| `ErrUnrecorded`         | nothing would record where the loop parked, so it cannot      |
| `ErrTranscript`         | the parked conversation could not be written down or read back |

Each names both sides of the refusal. "Denied" alone sends whoever reads it off
to reconstruct the grant list by hand from the pipeline definition.
