-- The editing head: which revision of a pipeline the next edit must be based
-- on.
--
-- Revisions are content-addressed and carry no parent (internal/defstore), so
-- "is this base the latest?" cannot be answered from the revisions table: two
-- edits made against the same base produce two sibling revisions and nothing
-- says which of them the pipeline is now at. Until this table existed the
-- answer lived in a map inside one control-plane process, which made
-- base_revision a real optimistic-concurrency check within that process and
-- NO check at all between two of them — both would find no head they knew of,
-- both would accept, and the loser's edit would disappear with nothing
-- recording that it had been made. Run partitioning makes several planes the
-- normal deployment, so that window is now open in production rather than in
-- theory.
--
-- One row per pipeline, moved by compare-and-set: the writer names the head it
-- read, and a move from a head that is no longer current updates nothing. That
-- is what makes the check hold ACROSS planes rather than within one, and it is
-- why this is an UPDATE with the old value in the predicate rather than an
-- upsert.
--
-- Dialect-neutral: every column is TEXT, so both runners apply this one file.
-- updated_at is RFC3339 with nanoseconds in UTC, which sorts
-- lexicographically in the same order it sorts chronologically.
CREATE TABLE IF NOT EXISTS pipeline_heads (
    tenant_id   TEXT NOT NULL,
    pipeline_id TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (tenant_id, pipeline_id)
);
