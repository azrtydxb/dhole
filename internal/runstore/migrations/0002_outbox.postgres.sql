-- The outbox, Postgres dialect. It mirrors 0002_outbox.sql row for row; the
-- two exist separately because the types differ, not because the schema does.
-- See that file for why this table exists at all.
--
-- Naming is the dialect switch: the SQLite runner applies every migration
-- EXCEPT `*.postgres.sql`, and the Postgres runner applies only those.
CREATE TABLE IF NOT EXISTS outbox (
    -- GENERATED ALWAYS rather than a sequence default: nothing outside the
    -- database ever chooses an id, so the ordering cannot be perturbed by a
    -- caller supplying one.
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id  TEXT        NOT NULL,
    subject    TEXT        NOT NULL,
    payload    BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    -- NULL means still owed. Set only after the bus has accepted the message.
    sent_at    TIMESTAMPTZ
);

-- The drainer claims with `WHERE sent_at IS NULL ... FOR UPDATE SKIP LOCKED`,
-- so the partial index is what keeps that claim off the sent history.
CREATE INDEX IF NOT EXISTS outbox_unsent
    ON outbox (id) WHERE sent_at IS NULL;
