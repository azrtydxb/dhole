-- The open-run index: every run that has been created and has not yet
-- completed or failed.
--
-- ADR 0003's claim is that a control-plane restart is a REPLAY rather than a
-- loss. Replay needs a run id, and until this table existed the only place the
-- set of unfinished runs lived was the memory of the process that submitted
-- them — so a plane that restarted could rediscover a run that was in flight
-- (its engine's status arrives on a durable subject and drives the run from
-- there) and could NOT rediscover one that was merely waiting: unschedulable,
-- backing off, or sleeping. Those runs were lost in exactly the way the ADR
-- says is impossible.
--
-- A table rather than a query over run_events. The query is expressible —
-- distinct run ids with no terminal event — but its cost grows with the whole
-- history of the tenant forever, while the answer is proportional to the work
-- actually outstanding. This table is that answer and nothing else: one row
-- per open run, inserted and deleted on the append path, inside the same
-- transaction as the event that justifies it, so the index and the log cannot
-- disagree about whether a run is finished.
--
-- Dialect-neutral: every column is TEXT, so both runners apply this one file.
-- created_at is RFC3339 with nanoseconds in UTC, which sorts lexicographically
-- in the same order it sorts chronologically.
CREATE TABLE IF NOT EXISTS open_runs (
    tenant_id  TEXT NOT NULL,
    run_id     TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id)
);

-- The only query is "which runs does this tenant still owe work on, oldest
-- first", so the tenant leads and the age follows.
CREATE INDEX IF NOT EXISTS open_runs_by_age
    ON open_runs (tenant_id, created_at);
