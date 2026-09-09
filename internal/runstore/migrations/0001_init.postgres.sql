-- The run event log, Postgres dialect. It mirrors 0001_init.sql row for row;
-- the two exist separately because the types differ, not because the schema
-- does, and a divergence between them is a bug the shared store contract is
-- there to catch.
--
-- Naming is the dialect switch: the SQLite runner applies every migration
-- EXCEPT `*.postgres.sql`, and the Postgres runner applies only those.
CREATE TABLE IF NOT EXISTS run_events (
    tenant_id TEXT     NOT NULL,
    run_id    TEXT     NOT NULL,
    step_id   TEXT     NOT NULL,
    -- SQLite's INTEGER is a 64-bit signed value; BIGINT is its Postgres
    -- equivalent. Sequences are per-tenant and monotonic and will outlive an
    -- INTEGER on a busy tenant, so neither column is narrowed here.
    attempt   BIGINT   NOT NULL,
    sequence  BIGINT   NOT NULL,
    type      TEXT     NOT NULL,
    payload   BYTEA,
    -- SQLite stores RFC3339 text; Postgres has a real instant type. TIMESTAMPTZ
    -- rather than TIMESTAMP: an event has one instant, and a control plane in
    -- another zone must replay it as the same moment.
    at        TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, run_id, step_id, attempt, sequence)
);

-- The scheduler resumes a tenant from its highest sequence, and every read is
-- tenant-scoped, so the tenant column leads every index.
CREATE INDEX IF NOT EXISTS run_events_tenant_sequence
    ON run_events (tenant_id, sequence);
