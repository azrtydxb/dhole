-- Every call a run made to a language model: what was asked, what came back,
-- which model actually answered, what it cost and how long it took.
--
-- This table is a PRIVACY SURFACE, not a metrics table. `prompt` holds
-- whatever the pipeline fed the model — a customer's complaint, a candidate's
-- CV, a patient's note — and `response` holds what the model made of it. Two
-- consequences are built into the shape below rather than left to the reader.
--
-- It is tenant-scoped like everything else: tenant_id leads the primary key,
-- and the store refuses a query without one. Two tenants running the same
-- pipeline produce the same run and step ids, and only the tenant tells their
-- prompts apart.
--
-- It carries its OWN retention, separate from the run event log's. The event
-- log is the audit trail and is kept longest; prompt content is the one class
-- of record where "kept because nobody wrote the sweep" is a breach rather
-- than a cost. retain_until is written by the recorder from its configured
-- window and is read by its purge — a retention column nothing deletes from
-- is a comment pretending to be a control.
--
-- retain_until is Unix SECONDS rather than the RFC3339 text the other tables
-- use for timestamps, because it is compared in SQL. RFC3339Nano trims
-- trailing zeros, so its lexicographic order is not its chronological order at
-- sub-second resolution, and a purge is exactly the statement that must not be
-- subtly wrong about which side of a boundary a row falls on.
CREATE TABLE IF NOT EXISTS llm_calls (
    tenant_id         TEXT    NOT NULL,
    run_id            TEXT    NOT NULL,
    step_id           TEXT    NOT NULL,
    -- The attempt this call was: a step that retried a malformed answer made
    -- several calls, each of which cost money and each of which is recorded.
    attempt           INTEGER NOT NULL,
    -- The digest of the model that ANSWERED, resolved from the response —
    -- never the alias that was asked for. See fingerprint.go.
    model_fingerprint TEXT    NOT NULL,
    prompt            TEXT    NOT NULL,
    response          TEXT    NOT NULL,
    prompt_tokens     INTEGER NOT NULL,
    completion_tokens INTEGER NOT NULL,
    latency_ms        INTEGER NOT NULL,
    recorded_at       TEXT    NOT NULL,
    retain_until      INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, run_id, step_id, attempt)
);

-- The purge sweeps by expiry across every tenant, so that is the index it
-- needs; the primary key already covers the per-run read.
CREATE INDEX IF NOT EXISTS llm_calls_retain_until ON llm_calls (retain_until);
