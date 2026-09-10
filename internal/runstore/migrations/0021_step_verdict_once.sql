-- At most one STEP_AWAITING_REPLAY and one STEP_POLICY_DENIED per step.
--
-- These are the same check-then-act that migration 0020 closed for a run's
-- terminal event, one level down. The scheduler decides both from a replay
-- taken OUTSIDE the transaction it later appends in:
--
--   * recordAwaitingReplay skips the write when the replay already showed a
--     STEP_AWAITING_REPLAY for the step; and
--   * deny writes STEP_POLICY_DENIED for a step the replay showed as ready,
--     which is a check-then-act with the readiness as the check.
--
-- The two things that ask a run to advance — the 250ms open-run tick and a
-- status arriving from an engine — do so concurrently in one process, so both
-- can replay before either appends. A step blocked on its effect class is
-- re-examined on EVERY pass for as long as the run stays open, which is until
-- a person acts, so the tick meets the engine's status on it repeatedly; a
-- denied step is examined by both triggers in the one pass where it is ready.
-- The allocator gives each append its own sequence, so run_events' primary key
-- does not collide and neither write complains: the log carries the step's
-- one-time verdict twice.
--
-- Both events exist to be READ by a person looking at a stuck or refused run,
-- and both are worded as the single fact that explains it — "nothing will move
-- until you act", "this rule refused this artifact". Said twice, with two
-- timestamps and two sequences, they read as two separate refusals of the same
-- step, which is a thing that never happened.
--
-- Uniqueness is per (tenant, run, step) rather than per run, because a run may
-- legitimately hold one of each for DIFFERENT steps: a fan-out where two
-- branches both stop is two facts, not one repeated.
--
-- Once per step for the LIFE of the run is what the code already promised, not
-- a new rule: the awaiting guard is a set that is only ever added to, and a
-- denial is always followed by RUN_FAILED, after which Advance returns early
-- on every later pass. If authorising a replay ever needs to re-arm the
-- awaiting event, that is a change to the rule and gets its own migration.
--
-- STEP_UNSCHEDULABLE is deliberately NOT here. Its rule is "not the same
-- reason twice running" — a changed reason is news and is recorded again — and
-- an index can only express "never twice". Keying one on the payload would
-- silently drop the second half of a reason that went A, B, A, leaving the log
-- saying the step is stuck on B when it is stuck on A. A duplicate row is
-- noise; a log that names the wrong reason is a wrong answer.
--
-- The DELETEs first, because a database written before this index exists may
-- already hold a duplicate, and CREATE UNIQUE INDEX over one would fail on
-- every open thereafter. The earliest survives: it is the one every reader has
-- already been served.
--
-- The migration runner has no version table and re-applies every file on every
-- open, so the DELETEs run at each start-up. They are cheap after the first:
-- their predicate is the index's own, so both dialects satisfy them from the
-- index rather than by scanning the whole log.
--
-- Dialect-neutral: both SQLite and Postgres take a partial unique index, and
-- the correlated subquery is ordinary SQL in both.
DELETE FROM run_events
WHERE type = 'STEP_AWAITING_REPLAY'
  AND sequence > (
    SELECT MIN(prior.sequence) FROM run_events AS prior
    WHERE prior.tenant_id = run_events.tenant_id
      AND prior.run_id = run_events.run_id
      AND prior.step_id = run_events.step_id
      AND prior.type = 'STEP_AWAITING_REPLAY'
  );

DELETE FROM run_events
WHERE type = 'STEP_POLICY_DENIED'
  AND sequence > (
    SELECT MIN(prior.sequence) FROM run_events AS prior
    WHERE prior.tenant_id = run_events.tenant_id
      AND prior.run_id = run_events.run_id
      AND prior.step_id = run_events.step_id
      AND prior.type = 'STEP_POLICY_DENIED'
  );

CREATE UNIQUE INDEX IF NOT EXISTS run_events_one_awaiting_replay
    ON run_events (tenant_id, run_id, step_id)
    WHERE type = 'STEP_AWAITING_REPLAY';

CREATE UNIQUE INDEX IF NOT EXISTS run_events_one_policy_denied
    ON run_events (tenant_id, run_id, step_id)
    WHERE type = 'STEP_POLICY_DENIED';
