# 0031 — A dispatch that cannot start gives back what it holds, and says why

Status: accepted
Date: 2026-09-15

## Context

ADR 0029 made an engine ask the plane, on `job.accept.<run>.<step>`, before it starts a
dispatch carrying `confirm_acceptance`, and made an `at-most-once` step that gets no answer
keep asking, holding its delivery, until one arrives. It also served that subject on every
plane with a plain subscription. Four consequences were left open when it closed.

An `at-most-once` dispatch that nobody answers holds an engine SLOT for as long as nobody
answers: a plane restarting, a plane partitioned from this engine, or a rolling upgrade in
which the only plane that set the flag is the one being replaced. A one-slot engine then runs
nothing at all — including `pure` work queued behind it, which ADR 0004 promises keeps
running while the plane is down. The engine's room to fetch is gone as well, so the queue
cannot even hand that pure work to an idle neighbour through this engine.

Every plane answers every acceptance request. Each answer is a lease `Renew`: with three
planes, three writes to the lease bucket for one question, and three replies of which the
engine reads one.

A dispatch that sits in the queue unfetched while engines able to run it are registered says
nothing in the run log. `surfaceStranded` reports a waiting dispatch only when NO engine
matches it. An engine whose consumer never fetches — a crashed pump, a queue bound under the
wrong name, an engine that cannot reach a plane to confirm — and an engine that is simply
full look identical from the run: a step in flight that never starts.

And a mixed fleet of planes during an upgrade was never stated to be safe, only believed to be.

## Decision

**An `at-most-once` dispatch that gets no answer is given back to the queue.** The engine
asks exactly as ADR 0029 says — holding a slot, before it holds the job — and on no answer
(no responder, a timeout, `ACCEPTANCE_UNSPECIFIED`) it stops renewing the delivery,
negatively acknowledges it with a delay of about a second (`-NAK {"delay": …}`), and frees
both the slot and its room to fetch. The queue redelivers it after the delay to whichever
engine has room, and that engine asks again. Nothing starts it but `ACCEPTANCE_CURRENT`, so
ADR 0002's guarantee is untouched; the question is still only ever asked holding a slot, so a
CURRENT answer — which accepts the lease — is always followed at once by the step starting,
and the heartbeat window never runs for a dispatch that is waiting for a slot.

Rejected: releasing the slot and keeping the delivery while waiting. The slot is not the only
thing held: the dispatch also counts against the engine's room, and a waiting dispatch that
does not is an engine fetching without bound — every `at-most-once` dispatch in the tier
drained into whichever engine asked first, renewed, where no neighbour that can reach a
plane will ever see it. Bounding that is a second budget beside the slots, and it still
leaves the work in one engine's memory rather than in the queue that exists to hold it.
Rejected: asking before taking a slot, for ADR 0029's reason. Rejected: bounding the wait and
then starting anyway, which is the double effect this whole chain exists to prevent.

**One plane answers each acceptance request.** The plane serves `job.accept.>` in the queue
group `dhole-plane-accept`, so the bus delivers each request to one member. An older plane
still subscribed plainly beside a newer one also receives it and also answers; the engine
reads the first reply. Both answers are the same lease compare against the same bucket, so
either is a correct linearisation — exactly as two plain subscribers were.

**A dispatch waiting past the heartbeat window says why.** On every sweep, an unaccepted
offer older than the lease TTL (floored at ten seconds) whose step `Match`es a registered
engine records `STEP_WAITING` with a cause:

- `capacity` — every matching engine's last heartbeat lists as many jobs as it has slots.
  The step is waiting for a slot, which is what a queue is for; the event says so once.
- `unconsumed` — at least one matching engine has a free slot and none has accepted the
  dispatch within the window: nothing is consuming its queue, or the engines that fetch it
  cannot reach a plane to confirm it. The reason names the dispatch subject.

The event is recorded once per cause (a change of cause records again) and cleared by the
next `STEP_DISPATCHED`, exactly like `STEP_UNSCHEDULABLE`, which still covers the case where
nothing matches. It records and changes nothing: the dispatch stays queued as the same
attempt.

**A plane-hosted step's body stops when its lease is gone.** A `builtin:` step whose lease
renewal is refused as fenced, or has not succeeded for a whole TTL, has its body's context
cancelled; the step writes nothing, and the model call or agent tool call in progress is
abandoned. The llm step asks the model no further attempt, and the agent step takes no further
action, once that context has ended.

## Consequences

A plane that is down costs `at-most-once` work its start, and nothing else: pure and
idempotent work keeps the slots, and a neighbour engine that can reach a plane can take the
`at-most-once` dispatch the moment it is redelivered. A rolling upgrade in which new planes
set the flag and old ones do not is safe by construction — only CURRENT starts an
`at-most-once` step, only a plane that answers sets the flag, and a dispatch whose plane is
gone waits in the queue until one that answers is up.

The cost is churn while nobody answers: each waiting `at-most-once` dispatch is fetched,
asked about and given back about once a second per dispatch, and its delivery count grows
with every cycle. JetStream counts a dispatch in its NAK delay against the consumer's
`max_ack_pending`, which is therefore still a bound engine authors must set to at least their
slot count. One more failure is possible than before: a plane that renewed the lease and whose
reply never reached the engine leaves an accepted lease nobody lists in a heartbeat, which
is lost a TTL later if the dispatch is not refetched in time — recorded as a lost attempt,
never run twice, and for an `at-most-once` step a replay a person decides.

`STEP_WAITING` is a new stored event type, additive. The run view and `dhole run logs` show it
as they show any event they do not special-case. Its `capacity` / `unconsumed` split is read
from heartbeats, which list only jobs an engine has confirmed: a dispatch sitting in a slot
for the length of one acceptance round trip is not visible, so the window is a TTL rather
than a moment.

A `builtin:` step abandoned mid-call can leave a model call that completed on the provider's
side with nobody to record it; the step's effect class decides whether it runs again, as it
does for an engine step whose engine died.
