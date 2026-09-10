-- At most one terminal event per run.
--
-- A run was closed by check-then-act: the scheduler replayed the log, saw no
-- terminal event, and appended one. The two things that ask a run to advance
-- — the open-run tick and a status arriving from an engine — do so
-- concurrently in one process, so both could replay before either appended,
-- both conclude "nothing ready, nothing in flight", and both append. The
-- allocator gives each its own sequence, so run_events' primary key does not
-- collide and nothing complains: a live run's log ended with RUN_COMPLETED
-- twice. That log is corrupt for every reader downstream — the run view and
-- the SSE stream both treat the first terminal event as the end and close the
-- client's connection on it.
--
-- The fix is this index rather than a lock or a re-read, because it makes the
-- decision and the write the same act. The second insert violates the index,
-- the append's ON CONFLICT DO NOTHING turns that into the no-op it already
-- treats a redelivered event as, and the loser learns nothing it needs to
-- know: the run is over either way.
--
-- The DELETE first, because a database written before this index exists may
-- already hold a duplicate, and CREATE UNIQUE INDEX on it would fail on every
-- open thereafter. The earliest terminal event survives — it is the one every
-- reader has already acted on.
--
-- The migration runner has no version table and re-applies every file on every
-- open, so the DELETE runs at each start-up. It is cheap after the first: its
-- predicate is the index's own predicate, so both dialects satisfy it from the
-- index rather than by scanning the whole log.
--
-- Dialect-neutral: both SQLite and Postgres take a partial unique index, and
-- the correlated subquery is ordinary SQL in both.
DELETE FROM run_events
WHERE type IN ('RUN_COMPLETED', 'RUN_FAILED', 'RUN_CANCELLED')
  AND sequence > (
    SELECT MIN(prior.sequence) FROM run_events AS prior
    WHERE prior.tenant_id = run_events.tenant_id
      AND prior.run_id = run_events.run_id
      AND prior.type IN ('RUN_COMPLETED', 'RUN_FAILED', 'RUN_CANCELLED')
  );

CREATE UNIQUE INDEX IF NOT EXISTS run_events_one_terminal
    ON run_events (tenant_id, run_id)
    WHERE type IN ('RUN_COMPLETED', 'RUN_FAILED', 'RUN_CANCELLED');
