-- At most one STEP_SECRET_UNAVAILABLE per step.
--
-- Migration 0021's rule, for the refusal ADR 0027 added. The scheduler decides
-- to refuse a step's secrets from a replay taken OUTSIDE the transaction it
-- appends in, with the step's readiness as the check. The 250ms open-run tick
-- and a status arriving from an engine both advance the same run, both can
-- find the step ready before either appends, and both record the refusal —
-- under two sequences, so run_events' primary key does not collide. The run
-- fails once (migration 0020); the log said the secret was refused twice.
--
-- Uniqueness is per (tenant, run, step), because two steps of one run refused
-- for their own secrets are two facts an operator has to act on. Once per step
-- for the life of the run is what the code already promises: a refusal is
-- always followed by RUN_FAILED, after which Advance returns early.
--
-- The DELETE first, for a database the race already reached: CREATE UNIQUE
-- INDEX over a duplicate would fail on every open. The earliest survives. The
-- runner re-applies every file on every open, and the DELETE's predicate is
-- the index's own, so after the first start it is answered from the index.
--
-- Dialect-neutral, like 0021: a partial unique index and a correlated
-- subquery are ordinary SQL in both SQLite and Postgres.
DELETE FROM run_events
WHERE type = 'STEP_SECRET_UNAVAILABLE'
  AND sequence > (
    SELECT MIN(prior.sequence) FROM run_events AS prior
    WHERE prior.tenant_id = run_events.tenant_id
      AND prior.run_id = run_events.run_id
      AND prior.step_id = run_events.step_id
      AND prior.type = 'STEP_SECRET_UNAVAILABLE'
  );

CREATE UNIQUE INDEX IF NOT EXISTS run_events_one_secret_unavailable
    ON run_events (tenant_id, run_id, step_id)
    WHERE type = 'STEP_SECRET_UNAVAILABLE';
