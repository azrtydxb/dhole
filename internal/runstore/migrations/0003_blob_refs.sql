-- Which runs still need which blobs. The CAS is content-addressed, so the same
-- bytes are one object no matter how many runs produced or consumed them:
-- reclamation is therefore refcounting, not ownership. A blob is collectable
-- only when every row pointing at it belongs to a run that has aged out of the
-- retention window (ADR 0009).
--
-- The primary key is (tenant_id, digest, run_id): a run referencing the same
-- blob twice records it once, so an at-least-once redelivery cannot inflate the
-- count and strand a blob forever. tenant_id leads it because two tenants
-- building the same source hold the same digest, and only the scope keeps one
-- tenant's collection away from the other's bytes.
CREATE TABLE IF NOT EXISTS blob_refs (
    tenant_id TEXT NOT NULL,
    -- The digest in its "<algo>:<hex>" text form, the same shape the cache
    -- stores its keys in.
    digest    TEXT NOT NULL,
    run_id    TEXT NOT NULL,
    PRIMARY KEY (tenant_id, digest, run_id)
);

-- The collector sweeps a tenant's references and asks which runs hold them, so
-- the tenant leads and the run follows.
CREATE INDEX IF NOT EXISTS blob_refs_tenant_run
    ON blob_refs (tenant_id, run_id);
