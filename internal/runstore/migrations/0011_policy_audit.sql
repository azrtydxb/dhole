-- The policy audit trail. One row is one decision made at the single
-- evaluation point ADR 0012 establishes, kept so that "why was this allowed"
-- and "why was this refused" have one answer from one log rather than four
-- components' guesses.
--
-- Allows are recorded as faithfully as denials: the permitted run is the one
-- nobody remembers approving.
CREATE TABLE IF NOT EXISTS policy_audit (
    tenant_id  TEXT NOT NULL,
    -- Random per row. Rows are read back ordered by (at, id), so two decisions
    -- recorded in the same nanosecond still have a stable order.
    id         TEXT NOT NULL,
    at         TEXT NOT NULL,
    -- The trust tier the decision was keyed on, and what it was about.
    tier       TEXT NOT NULL,
    subject    TEXT NOT NULL,
    -- The rule that decided, and what it says about the decision. A denial by
    -- absence — a tier with no policy — names no rule and carries an empty one.
    rule       TEXT NOT NULL,
    reason     TEXT NOT NULL,
    plugin_ref TEXT NOT NULL,
    -- 0 or 1. An INTEGER rather than a boolean so this one dialect-neutral
    -- file serves both SQLite and Postgres.
    allow      INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, id)
);

CREATE INDEX IF NOT EXISTS policy_audit_by_time
    ON policy_audit (tenant_id, at);
