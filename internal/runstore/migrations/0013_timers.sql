-- Durable waits. A run that sleeps until Tuesday, or waits three days for a
-- person, is one row here and nothing else: no goroutine, no in-memory timer,
-- nothing that a control-plane restart can lose (ADR 0003). The process that
-- scheduled a wait is routinely not the process that fires it.
--
-- One row per (tenant, run, step): scheduling the same wait twice — a
-- redelivered command, a replayed event — is the same wait, not two.
CREATE TABLE IF NOT EXISTS run_timers (
    tenant_id TEXT NOT NULL,
    run_id    TEXT NOT NULL,
    step_id   TEXT NOT NULL,
    -- RFC3339 with nanoseconds in UTC, exactly as run_events stores its
    -- timestamps. Text in BOTH dialects on purpose: the poll compares due_at
    -- to a bound value, and that format sorts lexicographically in the order
    -- it sorts chronologically, so one dialect-neutral file serves both and
    -- there is no second copy of this schema to drift.
    due_at    TEXT NOT NULL,
    -- NULL means outstanding. It is set inside the same transaction that
    -- claims the row and appends the events resuming the run, so a timer
    -- cannot be handed to two control planes and cannot fire twice.
    fired_at  TEXT,
    PRIMARY KEY (tenant_id, run_id, step_id)
);

-- The poll's only query is "what is outstanding and due", and it runs every
-- second forever. The index keeps that off the history of every wait the
-- deployment has ever served.
CREATE INDEX IF NOT EXISTS run_timers_outstanding
    ON run_timers (due_at) WHERE fired_at IS NULL;
