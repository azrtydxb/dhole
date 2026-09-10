-- Stored triggers: the event sources an operator created through the contract.
--
-- Before this table a trigger could only be DECLARED, on server.Config, read
-- once from a YAML file at start-up by `--triggers`. Creating one therefore
-- required a shell on the control plane's host and a restart, which is a
-- capability the GUI and an agent can never have — exactly what ADR 0013
-- refuses. The declared path still works and still wins: a row whose id a
-- running plane also declares is ignored, because the file is what the next
-- restart will read.
--
-- One row per (tenant, trigger). The id is the schedule's key in
-- trigger_schedules too, so two planes running one stored schedule run ONE
-- schedule between them, exactly as two planes declaring it do.
--
-- Every column is TEXT so the DDL is identical in both dialects, the same
-- rule 0006_catalog.sql follows; `untrusted` is '1' or '0' rather than a
-- boolean for that reason.
CREATE TABLE IF NOT EXISTS triggers (
    tenant_id     TEXT NOT NULL,
    trigger_id    TEXT NOT NULL,
    -- schedule | http | git.
    kind          TEXT NOT NULL,
    -- The pipeline this trigger drives. Its ACTIVE revision is resolved when
    -- the trigger fires, never pinned here: a trigger that pinned whatever was
    -- current when it was created would keep running last week's definition.
    pipeline_id   TEXT NOT NULL,
    -- A JSON object: the pipeline's input name to the field of this trigger's
    -- own event that fills it.
    input_mapping TEXT NOT NULL,
    -- A five- or six-field cron expression. Schedules only; empty otherwise.
    expression    TEXT NOT NULL,
    -- The shared secret the forge signs with, for git triggers. It is at rest
    -- here in the same sense the `--triggers` file has always held it in
    -- plaintext on disk: this table is no worse, and it is named rather than
    -- hidden. It is never returned by the contract.
    secret        TEXT NOT NULL,
    -- '1' when every value this trigger produces is tainted (ADR 0015).
    untrusted     TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    created_by    TEXT NOT NULL,
    PRIMARY KEY (tenant_id, trigger_id)
);

-- Every read is "what does this tenant have", which the primary key already
-- serves; there is no unscoped listing to index for.
