-- The Postgres form of this number. It exists only because SQLite's BLOB is
-- Postgres's BYTEA; everything else is identical to the plain file, which
-- this one replaces for Postgres. Keep the two in step.
-- The content-addressed cache. One row is a promise: a pure step whose inputs
-- hash to `key`, run under the environment and plugin lockfile folded into
-- that same key, produced exactly these outputs — so the next run skips the
-- work and reuses them (ADR 0009).
--
-- The key is a pure function of content, so two tenants building the same
-- source compute the same key. Only tenant_id keeps their results apart, which
-- is why it leads the primary key and why there is no lookup without it.
CREATE TABLE IF NOT EXISTS cache_entries (
    tenant_id  TEXT NOT NULL,
    key        TEXT NOT NULL,
    -- The recorded OutputRef list, length-framed protobuf. Outputs are
    -- references, never bytes: the bytes live in the CAS.
    outputs    BYTEA NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, key)
);
