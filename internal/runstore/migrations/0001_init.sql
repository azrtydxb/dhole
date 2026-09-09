-- The run event log. A run's position is this table, not a goroutine's call
-- stack: a control plane restart replays these rows instead of losing the run,
-- and a step waiting three days costs nothing but a row.
CREATE TABLE IF NOT EXISTS run_events (
    tenant_id TEXT    NOT NULL,
    run_id    TEXT    NOT NULL,
    step_id   TEXT    NOT NULL,
    attempt   INTEGER NOT NULL,
    sequence  INTEGER NOT NULL,
    type      TEXT    NOT NULL,
    payload   BLOB,
    at        TEXT    NOT NULL,
    PRIMARY KEY (tenant_id, run_id, step_id, attempt, sequence)
);

-- The scheduler resumes a tenant from its highest sequence, and every read is
-- tenant-scoped, so the tenant column leads every index.
CREATE INDEX IF NOT EXISTS run_events_tenant_sequence
    ON run_events (tenant_id, sequence);
