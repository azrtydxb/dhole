-- The Postgres form of this number. It exists only because SQLite's BLOB is
-- Postgres's BYTEA; everything else is identical to the plain file, which
-- this one replaces for Postgres. Keep the two in step.
-- Pipeline definitions and their revisions. The server database is canonical
-- (ADR 0008): git is a one-way mirror, so versioning, approval and history are
-- modelled here rather than inherited from a forge.
--
-- Applied by the same migration runner as the run event log, in filename
-- order, with no version table — every statement is idempotent so reopening a
-- database re-applies them harmlessly, and a gap in the numbering leaves this
-- file applicable on its own.

-- A pipeline is the stable identity an editor and a run refer to. It holds no
-- definition of its own: the definition lives in revisions, so nothing about a
-- pipeline can change under a run that is already executing one of them.
CREATE TABLE IF NOT EXISTS pipelines (
    tenant_id  TEXT NOT NULL,
    id         TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

-- A revision is one immutable definition. id is derived from content_hash, so
-- saving identical content twice is the same revision rather than a second
-- one; definition is the canonical protobuf encoding the hash was taken over,
-- and is never rewritten — a run pins a revision id and must read back exactly
-- what it started with, however long it runs and whoever edits meanwhile.
--
-- state is draft -> reviewed -> active, at most one active row per pipeline.
-- Approval moves; the definition does not. lockfile is a JSON object of
-- resolved plugin references; created_at is RFC3339 with nanoseconds in UTC,
-- which sorts lexicographically in the same order it sorts chronologically.
CREATE TABLE IF NOT EXISTS revisions (
    tenant_id    TEXT NOT NULL,
    id           TEXT NOT NULL,
    pipeline_id  TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    state        TEXT NOT NULL,
    lockfile     TEXT NOT NULL,
    definition   BYTEA NOT NULL,
    author       TEXT NOT NULL,
    approver     TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (tenant_id, id),
    FOREIGN KEY (tenant_id, pipeline_id) REFERENCES pipelines (tenant_id, id)
);

-- Resolving a pipeline's active revision is the hot read; listing a
-- pipeline's history is the other access path. Both are tenant-scoped,
-- because there is no unscoped query in this system.
CREATE INDEX IF NOT EXISTS revisions_tenant_pipeline
    ON revisions (tenant_id, pipeline_id, state);
