-- The durable catalog: which step, trigger and engine types exist, what they
-- declare, and what they are allowed to do (ADR 0010).
--
-- This is deliberately NOT the runtime engine registry. That one lives in NATS
-- KV with a heartbeat TTL and is meant to evaporate: an instance that stops
-- heartbeating must age out, because a stale claim that a dead process is alive
-- is worse than no claim at all. This table is the opposite — a control-plane
-- restart must not forget what a step type is, or every pipeline already saved
-- would lose the meaning of its steps.
--
-- Every column is TEXT so the DDL is identical in both dialects; the file is
-- duplicated as 0006_catalog.postgres.sql only because the migration runner
-- tells the dialects apart by filename and applies each set exclusively.
CREATE TABLE IF NOT EXISTS catalog_entries (
    tenant_id     TEXT NOT NULL,
    namespace     TEXT NOT NULL,
    name          TEXT NOT NULL,
    version       TEXT NOT NULL,
    -- "algo:hex" — what dispatch actually routes on. Tags are human-facing
    -- aliases resolved at save time; the digest is the identity.
    digest        TEXT NOT NULL,
    -- step | trigger | engine.
    kind          TEXT NOT NULL,
    -- The enum NAME, not its number: stored values are a persistence contract
    -- and a name survives a renumbering that a wire tag would not.
    effect_class  TEXT NOT NULL,
    -- JSON arrays of enum names and of engine type names.
    capabilities  TEXT NOT NULL,
    engine_types  TEXT NOT NULL,
    -- The declared JSON Schema documents, validated before they were stored.
    input_schema  TEXT NOT NULL,
    output_schema TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    -- A version is immutable: the primary key is what refuses a republish that
    -- would change code already pinned by a lockfile.
    PRIMARY KEY (tenant_id, namespace, name, version)
);

-- Listing a tenant's catalog is the other access path, and there is no lookup
-- that is not tenant-scoped.
CREATE INDEX IF NOT EXISTS catalog_entries_tenant ON catalog_entries (tenant_id, namespace, name);
