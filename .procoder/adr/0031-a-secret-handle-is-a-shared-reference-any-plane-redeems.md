# 0031 — A secret handle is a shared reference that any plane redeems and revokes

Status: accepted
Date: 2026-09-15

Supersedes: [0030 — A secret is redeemed on its tenant's subject and issued under policy](0030-a-secret-is-redeemed-on-its-tenants-subject-and-issued-under-policy.md)
— in part: where a handle lives, which plane may redeem or revoke it, and when a run's
handles are revoked. The tenant-scoped subject, the legacy subject's window, the refusal of a
`builtin:` step, the unique refusal and the policy decision per secret stand as decided there.

## Context

The control plane scales out: `controlPlane.replicas` planes share one store and one bus, and
consumers are partitioned by run id. ADR 0030 left handles in the memory of the plane that
issued them, which was written before that was true of any deployment and fails four ways in
one that has more than one replica.

A redemption is a request on `secret.redeem.<tenant>`, and every plane subscribes. Each one
answered — the responder was a plain subscription, not a queue group — and the engine took
the first reply. A plane that had not issued the handle refuses it, so whether a step
received its credential depended on which replica's refusal was faster than the issuer's
value. And the refusing replica deleted nothing, so the handle was not even spent.

Revocation had the same shape. An attempt's end is processed by the plane that owns the run's
partition, which is not necessarily the plane that dispatched it after a rebalance, and a
cancel arrives at whichever replica the API call landed on. Both revoked nothing anywhere
else, and the handle's ten-minute expiry was the only bound.

A run the scheduler fails on its own — attempts exhausted, a step refused by policy or for a
secret — left its queued siblings' handles live, because only an attempt's end and a cancel
revoked anything. Neither does a run failed outside the scheduler: an approval denied, a
halting `builtin:llm` step.

And a tenant in its own NATS account (ADR 0014) redeems inside that account, where the plane's
one connection is not: nothing answers there at all.

The tempting fix is to put the broker's map into a shared store as it is. That map holds
VALUES, and a shared, replicated, durable copy of every live credential is exactly the
secret at rest ADR 0027 exists to avoid.

## Decision

**A handle is recorded in the JetStream KV bucket `dhole-secret-handles`, shared by every
plane on the bus, as a REFERENCE and never a value.** The record is the tenant, the run, the
step and the attempt it was issued for, which of the plane's sources it resolves from
(`step` — the operator's step secrets, ADR 0027 — or `plane`, the model credentials of
ADR 0024), the secret's name in that source, and its expiry. The key is
`<tenant>.<run digest>.<handle digest>`, where each digest is SHA-256: the bucket holds no
bearer credential either, so reading it is not a way to redeem.

**The value is resolved at redemption, on whichever plane answers, from that plane's own
source.** Every replica is configured with the same sources — the chart gives them the same
`controlPlane.secrets` — so the answer does not depend on the replica. A secret removed from
the source between issue and redemption is refused, which is what removing it means.

**Single use holds across planes by compare-and-set.** Spending a handle deletes its key at
the revision the spender read; of two planes presenting it concurrently, the store lets one
through and the other is refused. A handle is still spent by being presented, on the wrong
tenant's subject as on the right one.

**Any plane revokes.** Revoking an attempt, a run or a named handle deletes the matching
keys, so it no longer matters which replica issued them.

**The issuer's expiry is still enforced** by the record at redemption. The bucket keeps a
record for at most thirty minutes, and a handle asked for a longer expiry is refused at
issue rather than silently vanishing early; step handles live ten minutes and the plane's
own thirty seconds.

**Exactly one plane answers a redemption**: the responder on `secret.redeem.*` and on the
legacy subject joins the queue group `dhole-secret-redeem`.

**A run's handles are revoked when the run ends, by whatever ended it.** The scheduler
revokes them when it writes `RUN_FAILED` or `RUN_COMPLETED`, and the API when it writes
`RUN_CANCELLED`. A run closed by any other writer — an approval denied, a halting
`builtin:llm` step, a writer added later — is caught by the plane's orphan sweep, which
revokes every handle whose run is no longer in its tenant's open-run index. The sweep reads
the handles BEFORE the index, so a run created during the sweep is never mistaken for one
that closed.

**A plane given a tenant's account credential serves that tenant's redemption inside the
account** (`dhole serve --secret-account TENANT=ENVVAR`). The responder there answers
`secret.redeem.<tenant>` and the legacy subject for that tenant only, whatever token a
request carries, and redeems from the same shared bucket, in the same queue group. The
handles themselves stay in the bucket on the plane's own connection.

Nothing on the wire changes: a handle is still an opaque string, on the same subjects, with
the same refusal. Engines at protocol version N-1 are unaffected.

## Consequences

`controlPlane.replicas` greater than one redeems, spends and revokes correctly, whichever
replica an engine's request or an API call reaches.

A plane restart no longer invalidates the handles it issued: they live until they are spent,
revoked or expire. That was never the property relied on — the expiry was — and a rolling
upgrade no longer fails every step whose dispatch was issued by the replica being replaced.

A redemption costs a key listing and a compare-and-set on the bus instead of a map lookup,
and revoking a run costs a filtered listing. Neither is on a hot path: a handle is redeemed
once per secret per attempt.

The credential now exists in the operator's Secret, the source of every plane, and the one
process that redeemed it. The broker holds none, in memory or in the bucket.

A run closed outside the scheduler keeps its handles until the next orphan sweep rather than
revoking them in the same moment. The expiry remains the backstop beneath the sweep.

A plane serving a tenant's account needs that account's credential, which is the identity
ADR 0014 already gives the plane for the account. Dispatching into a tenant's own account is
not decided here: this record makes redemption reachable there, not the rest of the path.
