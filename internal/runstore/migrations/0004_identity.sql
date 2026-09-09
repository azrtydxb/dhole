-- Built-in identity: the principals the control plane knows about and the
-- service tokens they hold. Applied by the same migration runner as the run
-- event log, in filename order, and independent of 0002 and 0003 — a gap in
-- the numbering leaves this file applicable on its own.

-- A principal is a human or a service. credential_hash is a PHC-encoded
-- argon2id string carrying its own parameters and salt, never a password:
-- a stolen database must not be a list of passwords. Services have no
-- password credential and store an empty hash; they authenticate by token.
CREATE TABLE IF NOT EXISTS principals (
    tenant_id       TEXT NOT NULL,
    subject         TEXT NOT NULL,
    kind            TEXT NOT NULL,
    credential_hash TEXT NOT NULL,
    PRIMARY KEY (tenant_id, subject)
);

-- Service tokens. token_hash is a SHA-256 of the 32 random bytes handed to the
-- caller and is the only representation stored: a tokens table that can be
-- read back is a credential database in plaintext, so the issued token exists
-- exactly once, in the caller's hands. scopes is a JSON array; expires_at is
-- RFC3339 with nanoseconds in UTC, which sorts lexicographically in the same
-- order it sorts chronologically.
CREATE TABLE IF NOT EXISTS tokens (
    tenant_id  TEXT NOT NULL,
    subject    TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    scopes     TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, token_hash)
);

-- Every lookup is tenant-scoped; listing or revoking a service's tokens is
-- the other access path.
CREATE INDEX IF NOT EXISTS tokens_tenant_subject ON tokens (tenant_id, subject);
