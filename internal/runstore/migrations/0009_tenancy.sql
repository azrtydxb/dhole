-- The tenant register. Tenancy is in the data model from the first commit
-- (ADR 0014), so the tenant ids that scope every other table have a table of
-- their own rather than being an implied string that appears in ten primary
-- keys and nowhere else.
--
-- Dialect-neutral: every column is TEXT, so both runners apply this one file
-- and there is nothing to keep in step between two copies.

-- id is the scope every other table's tenant_id refers to. It is restricted to
-- [a-z0-9] plus non-leading '-' and '_' by internal/tenant.Validate, because
-- the same string becomes a NATS subject token and a storage path segment: a
-- '.' would split a subject, and a '*' or '>' would turn one tenant's scope
-- into a wildcard over all of them.
--
-- nats_account is the account the tenant's bus traffic is isolated in. It is
-- stored rather than only derived so that an operator reading the database can
-- tell which account a tenant's traffic is in without knowing the naming rule,
-- and so a future rename has somewhere to record itself.
--
-- created_at is RFC3339 with nanoseconds in UTC, which sorts lexicographically
-- in the same order it sorts chronologically.
CREATE TABLE IF NOT EXISTS tenants (
    id           TEXT NOT NULL PRIMARY KEY,
    display_name TEXT NOT NULL,
    nats_account TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

-- One account belongs to exactly one tenant. Two tenants sharing an account
-- would put both tenants' traffic in one subject namespace, which is precisely
-- the isolation this design buys by mapping tenants onto accounts.
CREATE UNIQUE INDEX IF NOT EXISTS tenants_nats_account
    ON tenants (nats_account);
