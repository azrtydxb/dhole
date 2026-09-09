-- The outbox claim is scoped to the control plane that owes the message; see
-- 0015_outbox_deployment.sql for why.
--
-- Postgres is the shared, long-lived deployment, so this number adds the
-- column in place instead of relying on 0002's CREATE TABLE having run before
-- the column existed. The default is empty rather than a name: a row enqueued
-- by an older build belongs to no deployment anyone can identify, and leaving
-- it unclaimable is better than handing it to whichever plane drains next.
ALTER TABLE outbox ADD COLUMN IF NOT EXISTS deployment_id TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS outbox_unsent_by_deployment
    ON outbox (deployment_id, id) WHERE sent_at IS NULL;
