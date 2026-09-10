-- Every subject holding a service token is a principal of its tenant.
--
-- A credential's identity and an approver's identity lived in two tables. A
-- token minted by the supported path — `dhole token issue`, which calls
-- identity.Local.IssueToken and writes only `tokens` — authenticated every
-- API call, and was then refused by approval.Decide with "is not a principal
-- of tenant %q", because that check reads `principals`. So the holder of a
-- perfectly good credential could start a run through the contract and could
-- not release the gate it stopped at, and the refusal named an identity
-- problem the operator had no way to act on: they had issued the token.
--
-- IssueToken now establishes the principal at the mint, which fixes every
-- token issued from here on. This file is the other half: a deployment that
-- already has tokens in flight must not have to reissue them, and a token
-- whose holder is refused by half the system is exactly the case that is
-- hardest to diagnose from the outside.
--
-- kind 'service' for a backfilled row, because a token is a machine
-- credential; a human principal already has a `principals` row written by
-- CreateUser, and NOT EXISTS leaves it alone — this must never touch an
-- existing row, whose credential_hash is a password nobody can recover.
--
-- The migration runner has no version table and re-applies every file on
-- every open, so this INSERT runs at each start-up. It is idempotent by its
-- own predicate, and cheap after the first: `principals` is keyed on
-- (tenant_id, subject) and `tokens` is indexed on it, so both dialects answer
-- the anti-join from an index.
--
-- Dialect-neutral: INSERT ... SELECT with a correlated NOT EXISTS is ordinary
-- SQL in both SQLite and Postgres, and DISTINCT is what keeps a subject with
-- several live tokens from being inserted twice within one statement.
INSERT INTO principals (tenant_id, subject, kind, credential_hash)
SELECT DISTINCT t.tenant_id, t.subject, 'service', ''
FROM tokens AS t
WHERE NOT EXISTS (
    SELECT 1 FROM principals AS p
    WHERE p.tenant_id = t.tenant_id AND p.subject = t.subject
);
