-- The per-tenant sequence allocator.
--
-- run_events' primary key is (tenant_id, run_id, step_id, attempt, sequence),
-- and every writer used to choose its own sequence as `MAX(sequence) + 1` read
-- outside the transaction that used it. Two writers that read before either
-- wrote therefore chose the SAME number, which had two consequences and the
-- second is the dangerous one:
--
--   * the per-tenant log had no total order, though the outbox's contract and
--     the run view both say it does; and
--   * two DIFFERENT events that landed on one (run, step, attempt, sequence)
--     did not conflict loudly — the append is ON CONFLICT DO NOTHING, for
--     idempotence — so one of them was simply never written, and nothing
--     anywhere said so.
--
-- One row per tenant, holding the next position to hand out. The allocation is
-- a single upsert with RETURNING, so it is atomic on both dialects: on
-- Postgres the row lock serialises concurrent planes, on SQLite the
-- database-wide write lock does. It runs inside the SAME transaction as the
-- event it numbers, so a rolled-back append cannot burn a number that then
-- looks like a lost event.
--
-- `next` is seeded from the tenant's existing high-water mark the first time
-- it is asked for, so a database written by an older build carries on from
-- where it was rather than colliding with its own history.
--
-- Dialect-neutral: BIGINT has INTEGER affinity under SQLite.
CREATE TABLE IF NOT EXISTS run_sequences (
    tenant_id TEXT   NOT NULL PRIMARY KEY,
    next      BIGINT NOT NULL
);
