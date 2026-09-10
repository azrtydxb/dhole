-- At most one STEP_AWAITING_TIMER per step.
--
-- This is migration 0020's check-then-act again, on the event that arms a
-- durable gate. Both halves of the family are here:
--
--   * arming used to be a DIFFERENT transaction from the readiness decision it
--     belonged to. Whoever started a run scheduled the wait; the scheduler
--     decided what was ready. A sequence is allocated inside a transaction and
--     its VISIBILITY is not, so the gate event could hold a lower sequence
--     than the STEP_DISPATCHED of the very step it gated: the log read "gated,
--     then dispatched anyway" and the wait had been skipped entirely. That
--     half is closed in code — a gate is now a step type, armed by the same
--     act that found it ready, and gatedness is read from the pinned
--     definition rather than from the log.
--
--   * two advances of one run can still both find one gate ready. The 250ms
--     open-run tick and a status arriving from an engine replay concurrently,
--     both conclude the step is ready, and both arm. The arming's own
--     "is there a timer row already" check is taken inside its transaction,
--     which serialises it on SQLite and does NOT on Postgres at read
--     committed: both see no row, both append, and only the second INSERT is
--     absorbed by run_timers' primary key. The log then says the step began
--     waiting twice — two due times for one wait, in the one place a person
--     looks to find out what a stopped run is stopped on.
--
-- Uniqueness is per (tenant, run, step), like 0021's, because a fan-out may
-- legitimately hold a gate on each of several branches: that is several waits,
-- not one repeated.
--
-- Once per step for the LIFE of the run is what the code already promises, not
-- a new rule. A timer fires exactly once — the row is marked, not merely
-- handled — and firing appends the step's own STEP_SUCCEEDED, after which the
-- step is never ready again. If re-arming a gate ever becomes a thing a replay
-- may do, that is a change to the rule and gets its own migration.
--
-- The DELETE first, because a database written before this index exists may
-- already hold a duplicate, and CREATE UNIQUE INDEX over one would fail on
-- every open thereafter. The earliest survives: it is the wait the timer row
-- actually holds, and the one every reader has already been served.
--
-- The migration runner has no version table and re-applies every file on every
-- open, so the DELETE runs at each start-up. It is cheap after the first: its
-- predicate is the index's own, so both dialects satisfy it from the index
-- rather than by scanning the whole log.
--
-- Dialect-neutral: both SQLite and Postgres take a partial unique index, and
-- the correlated subquery is ordinary SQL in both.
DELETE FROM run_events
WHERE type = 'STEP_AWAITING_TIMER'
  AND sequence > (
    SELECT MIN(prior.sequence) FROM run_events AS prior
    WHERE prior.tenant_id = run_events.tenant_id
      AND prior.run_id = run_events.run_id
      AND prior.step_id = run_events.step_id
      AND prior.type = 'STEP_AWAITING_TIMER'
  );

CREATE UNIQUE INDEX IF NOT EXISTS run_events_one_gate_armed
    ON run_events (tenant_id, run_id, step_id)
    WHERE type = 'STEP_AWAITING_TIMER';
