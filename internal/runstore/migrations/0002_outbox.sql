-- The outbox. A row here is an INTENT TO PUBLISH, written in the same
-- transaction as the run event that justifies it (ADR 0005). The bus is
-- transport, not truth: if the event were committed and the message published
-- as two separate acts, a crash between them would lose the step with no trace
-- and no retry could recover what was never recorded. One transaction makes
-- that impossible; publishing then becomes a side effect that can only be
-- LATE, never lost.
--
-- The price is at-least-once: a crash between publishing and marking a row
-- sent republishes it. The subject and payload therefore have to carry enough
-- for the consumer to deduplicate — for a dispatch that is the fence token.
CREATE TABLE IF NOT EXISTS outbox (
    -- Monotonic per insert, and the order a single drainer publishes in.
    -- SQLite's INTEGER PRIMARY KEY is the rowid, so this is the natural
    -- counter rather than an extra one.
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id  TEXT NOT NULL,
    -- The control plane that owes this message. A row is addressed to ONE
    -- deployment's bus, so the drainer's claim names it: two planes sharing a
    -- database are not a misconfiguration, and an unscoped claim makes each of
    -- them publish the other's dispatches to engines that never heard of the
    -- run. Added to this table's definition rather than by an ALTER because
    -- SQLite cannot add a column idempotently and every migration here is
    -- re-applied on every open; 0015 carries the in-place upgrade for
    -- databases that already exist.
    deployment_id TEXT NOT NULL DEFAULT '',
    subject    TEXT NOT NULL,
    -- The marshalled protobuf, opaque here. The store never interprets it.
    payload    BLOB NOT NULL,
    created_at TEXT NOT NULL,
    -- NULL means still owed. It is set only AFTER the bus has accepted the
    -- message, never before: a row marked sent by a publish that then failed
    -- is a step lost silently, which is the exact failure this table exists
    -- to prevent.
    sent_at    TEXT
);

-- The drainer's only query is "what is still owed, oldest first". A partial
-- index keeps that scan proportional to the backlog rather than to the
-- history, which on a busy control plane is the difference between a constant
-- and a table scan that grows forever.
CREATE INDEX IF NOT EXISTS outbox_unsent
    ON outbox (id) WHERE sent_at IS NULL;
