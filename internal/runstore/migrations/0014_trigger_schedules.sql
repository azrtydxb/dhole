-- Schedule triggers. One row per (tenant, schedule): the NEXT time it is due,
-- and whether a fire it started is still in flight.
--
-- It is a due time, never a queue of intervals. Coming back after three hours
-- of downtime fires the missed window ONCE, because the row says "next due
-- at", and firing advances it past everything already elapsed. The catch-up
-- loop this refuses is how a minutely schedule runs 180 times at once.
--
-- Separate from run_timers on purpose. A run timer resumes a step of a run
-- that already exists — it is claimed and fired by appending that step's
-- STEP_TIMER_FIRED and STEP_SUCCEEDED — and a schedule has no run and no step
-- until it fires. Sharing the table would mean writing run events for runs
-- that do not exist, and the wait poll, which claims and resumes EVERY due
-- row, would consume schedules as if they were waits.
--
-- The claim discipline is the same as run_timers' and the outbox's, and
-- deliberately so: the due row is taken under FOR UPDATE SKIP LOCKED on
-- Postgres and under SQLite's immediate write lock, in the same transaction
-- that advances due_at. Two control planes poll this table every second, and
-- an occurrence handed to both starts the pipeline twice.
CREATE TABLE IF NOT EXISTS trigger_schedules (
    tenant_id  TEXT NOT NULL,
    -- The schedule's own id, unique within the tenant. The same id in two
    -- tenants is two schedules that never see each other's row.
    trigger_id TEXT NOT NULL,
    -- RFC3339 with nanoseconds in UTC, exactly as run_events and run_timers
    -- store their timestamps: text in BOTH dialects, because that format
    -- sorts lexicographically in the order it sorts chronologically, so one
    -- dialect-neutral file serves both and there is no second copy to drift.
    due_at     TEXT NOT NULL,
    -- Non-NULL while a fire started here has not finished. It is what makes
    -- the concurrency budget durable rather than a counter in one process: a
    -- second control plane sees the same in-flight run and skips too. It is
    -- treated as stale after the configured lease, so a plane that dies
    -- mid-fire does not stop the schedule forever.
    active_at  TEXT,
    -- The last occurrence that was passed over, and why. A skip that is not
    -- recorded is indistinguishable from a schedule that quietly stopped.
    skipped_at     TEXT,
    skipped_reason TEXT,
    PRIMARY KEY (tenant_id, trigger_id)
);

-- The poll's only query is "what is due", and it runs every second forever.
CREATE INDEX IF NOT EXISTS trigger_schedules_due ON trigger_schedules (due_at);
