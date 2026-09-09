-- What a tenant is allowed to consume, and what it actually consumed.
--
-- The tenant register itself is migration 0009 (Task 22); this adds the two
-- tables that turn a tenant id into an account somebody can be held to. They
-- are separate from `tenants` on purpose: a limit is edited by an operator and
-- a usage record is append-only history, and putting a mutable column beside
-- an immutable ledger is how a bill becomes unreproducible.
--
-- Dialect-neutral: BIGINT has INTEGER affinity under SQLite, so both runners
-- apply this one file and there is nothing to keep in step between two copies.

-- One quota row per tenant. Every limit is present and NOT NULL rather than
-- nullable-means-unlimited: "no row, so no limit" is the shape in which a
-- forgotten provisioning step becomes an unbounded bill, and the provisioner
-- writes the defaults precisely so that a tenant is never in that state.
--
-- The rows are written with ON CONFLICT DO NOTHING by provisioning and
-- overwritten only by SetQuota, so re-provisioning an existing tenant cannot
-- reset limits an operator lowered by hand.
CREATE TABLE IF NOT EXISTS quotas (
    tenant_id            TEXT   NOT NULL PRIMARY KEY,
    -- How many of this tenant's steps may be in flight across the fleet at
    -- once. It is the tenant-wide companion to the scheduler's per-pipeline
    -- budget: the budget stops one pipeline taking the fleet, this stops one
    -- tenant doing it.
    max_concurrent_steps INTEGER NOT NULL,
    -- Runs ADMITTED in one UTC day. Counted from usage_records rather than
    -- from a counter, so the number on the invoice and the number the limit
    -- was applied to are the same number.
    max_runs_per_day     INTEGER NOT NULL,
    -- Bytes of content-addressed storage the tenant may hold.
    max_cas_bytes        BIGINT NOT NULL,
    updated_at           TEXT   NOT NULL
);

-- The usage ledger: one row per billable — or deliberately unbillable — unit
-- of work, append-only.
--
-- The primary key IS the idempotence key, and that is the whole design. This
-- system delivers at least once by construction: the outbox redelivers, and a
-- step lost with its engine is re-dispatched under a new fence. A meter that
-- counted deliveries would bill a customer twice for one step, so a usage
-- record is identified by the work it describes — (tenant, kind, run, step,
-- attempt) — and never by the message that carried it. Recording the same
-- work twice is an ON CONFLICT DO NOTHING, which is a no-op rather than a
-- second charge.
--
-- Rows that are NOT billable are still written. An attempt the platform lost
-- costs the customer nothing, and an invoice that simply omitted it could not
-- answer "the run took nine seconds, why am I charged for two". `billable`
-- says which is which; nothing is deleted to make a total come out right.
--
-- quantity is an INTEGER in the kind's own base unit — milliseconds for
-- step-seconds, bytes for storage, a count of one for a run — never a float.
-- Money-adjacent arithmetic that rounds differently on two machines is not
-- defensible to somebody reading an invoice.
--
-- occurred_at is RFC3339 with nanoseconds in UTC, for a human reading the
-- table. Every COMPARISON uses occurred_at_unix_nano, because RFC3339Nano
-- trims trailing zeros and its lexicographic order is therefore not its
-- chronological order at sub-second resolution — the same trap 0008 documents
-- for llm_calls.retain_until.
CREATE TABLE IF NOT EXISTS usage_records (
    tenant_id             TEXT    NOT NULL,
    kind                  TEXT    NOT NULL,
    run_id                TEXT    NOT NULL,
    step_id               TEXT    NOT NULL,
    attempt               INTEGER NOT NULL,
    quantity              BIGINT  NOT NULL,
    billable              INTEGER NOT NULL,
    occurred_at           TEXT    NOT NULL,
    occurred_at_unix_nano BIGINT  NOT NULL,
    PRIMARY KEY (tenant_id, kind, run_id, step_id, attempt)
);

-- The daily run count and the storage total are both "this tenant, this kind,
-- since this instant", so that is the index they get. The primary key already
-- covers reading one run's records back for an invoice line.
CREATE INDEX IF NOT EXISTS usage_records_by_kind_time
    ON usage_records (tenant_id, kind, occurred_at_unix_nano);
