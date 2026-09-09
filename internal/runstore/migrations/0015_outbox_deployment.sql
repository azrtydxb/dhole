-- The outbox claim is scoped to the control plane that owes the message.
--
-- `SELECT ... WHERE sent_at IS NULL` names nothing, so two control planes
-- sharing a database steal each other's messages — observed for real: a
-- server's drainer claimed another component's rows and published them onto
-- its own bus, to engines that had never heard of the run. The scope is the
-- DEPLOYMENT and not the tenant: one tenant may run two planes, and every
-- drainer of ONE plane must keep sharing rows, which is what makes running
-- two of them for availability work.
--
-- SQLite has no idempotent ADD COLUMN and this schema is re-applied in full on
-- every open, so the column itself lives in 0002's CREATE TABLE and the
-- Postgres form of this number carries the in-place ALTER for databases that
-- already exist. A SQLite database created before this migration therefore
-- fails to open here, loudly, naming the missing column: the development and
-- homelab store is recreated rather than migrated in place.
CREATE INDEX IF NOT EXISTS outbox_unsent_by_deployment
    ON outbox (deployment_id, id) WHERE sent_at IS NULL;
