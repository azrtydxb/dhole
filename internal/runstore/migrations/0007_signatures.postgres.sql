-- Detached signature records: who vouched for exactly these bytes.
--
-- The row is keyed on the DIGEST, not on the reference, and that is the whole
-- design. A signature is a statement about bytes. Keying it on the digest is
-- what makes verification identical for an `oci://` and a `cas://` artifact —
-- one code path, no scheme switch above this table — and what stops a moved
-- tag from carrying a signature over to different content. Task 32 guarantees
-- a reference is pinned to a digest exactly once; this table is what makes
-- that digest mean "and someone we trust vouched for it".
--
-- Nothing here is authorisation. The row says who signed; the tenant's allowed
-- signer list, applied at verification time, says whose word is accepted. A
-- record from an unknown signer is stored and simply never satisfies a check,
-- which is what keeps the audit trail honest about signatures we saw and did
-- not accept.
--
-- Trust is re-checked, never remembered: there is deliberately no "verified"
-- column. A cached verdict would keep dispatching an artifact whose signature
-- was withdrawn after a key compromise, which is the one moment the check
-- exists for.
CREATE TABLE IF NOT EXISTS artifact_signatures (
    -- Two tenants mirroring the same upstream plugin hold the SAME digest, so
    -- the digest alone cannot be the key: only the scope keeps one tenant's
    -- decision to trust a signer from silently becoming another's.
    tenant_id  TEXT NOT NULL,
    -- "<algo>:<hex>", the same text form the catalog and the cache store.
    digest     TEXT NOT NULL,
    -- The signer, exactly as the certificate asserted it. Compared for exact
    -- equality at verification; never a prefix or substring, or
    -- "evil-release-bot@corp.example" would pass a check for
    -- "release-bot@corp.example".
    identity   TEXT NOT NULL,
    -- The OIDC issuer that asserted the identity. It is part of the key
    -- because the same identity string from a different issuer is a DIFFERENT
    -- principal: anyone who can make any issuer assert an address would
    -- otherwise be able to sign as the release bot.
    issuer     TEXT NOT NULL,
    -- The evidence itself: the cosign bundle, or the operator's attestation
    -- document for a manual record. Bytes, not text — a bundle is base64 and
    -- DER, and a TEXT column would invite an encoding round-trip on the one
    -- value whose exact bytes a signature check depends on. SQLite BLOB is
    -- Postgres BYTEA, which is why this migration has a .postgres.sql sibling.
    payload    BYTEA NOT NULL,
    -- cosign | manual. A source this build does not understand denies rather
    -- than admits: "I cannot check this" is not "checked".
    source     TEXT NOT NULL,
    created_at TEXT NOT NULL,
    -- One record per (tenant, digest, signer): re-recording the same
    -- attestation is idempotent, so a retried publish cannot fan out rows,
    -- while two different signers of the same artifact both keep their record.
    PRIMARY KEY (tenant_id, digest, identity, issuer)
);

-- Verification always asks the same question — what does this tenant hold for
-- this digest — and there is no lookup that is not tenant-scoped.
CREATE INDEX IF NOT EXISTS artifact_signatures_tenant_digest
    ON artifact_signatures (tenant_id, digest);
