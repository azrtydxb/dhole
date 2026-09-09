-- Federated upstreams and what this tenant has mirrored from them.
--
-- An upstream consulted at dispatch time is two risks at once: an availability
-- one — a step cannot start because someone else's registry is down — and a
-- supply-chain one, since the bytes fetched at that moment are whatever the
-- upstream serves right then. Mirroring moves both to sync time, where a human
-- is watching and a refusal is read. These two tables are what makes that
-- possible: the first records who we federate with and on whose word, the
-- second records what we already hold locally so dispatch never has to ask.
CREATE TABLE IF NOT EXISTS plugin_upstreams (
    tenant_id  TEXT NOT NULL,
    -- The namespace this upstream's plugins are addressed under. It is part of
    -- the key because two federations both publish `docker-build`: without the
    -- namespace the second registration silently shadows the first and every
    -- pipeline naming `docker-build` starts running somebody else's code.
    namespace  TEXT NOT NULL,
    -- "oci://<registry>[/<repository prefix>]".
    url        TEXT NOT NULL,
    -- JSON array of "<issuer> <identity>" entries. Trust is PER UPSTREAM: a
    -- signer vouched for inside one federation is not thereby vouched for
    -- inside another, so this list belongs on the row and never in a pool
    -- shared between upstreams.
    allowed_identities TEXT NOT NULL,
    -- always | on_demand. Both mirror before dispatch; they differ only in
    -- whether save-time resolution may pull an artifact sync has not seen.
    mirror_policy TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, namespace)
);

-- One row per artifact this tenant holds locally, and the coordinate it is
-- addressed by. The digest is the UPSTREAM manifest digest, unchanged by the
-- copy: the mirror is a byte-identical copy, so the signature made over those
-- bytes upstream still verifies here, and a re-run pins the same artifact it
-- pinned before.
CREATE TABLE IF NOT EXISTS plugin_mirrors (
    tenant_id  TEXT NOT NULL,
    namespace  TEXT NOT NULL,
    name       TEXT NOT NULL,
    tag        TEXT NOT NULL,
    -- "<algo>:<hex>".
    digest     TEXT NOT NULL,
    media_type TEXT NOT NULL,
    -- The local reference dispatch fetches by. Stored rather than re-derived,
    -- so a later change to the mirror's naming scheme cannot orphan what is
    -- already mirrored.
    local_ref  TEXT NOT NULL,
    -- The upstream reference it was copied from, kept for the audit trail:
    -- "where did these bytes come from" must have an answer that does not
    -- depend on the upstream still being registered.
    source_ref TEXT NOT NULL,
    mirrored_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, namespace, name, tag)
);

CREATE INDEX IF NOT EXISTS plugin_mirrors_by_namespace
    ON plugin_mirrors (tenant_id, namespace);
