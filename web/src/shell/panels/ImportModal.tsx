/**
 * Bringing a pipeline in from YAML.
 *
 * Import lands a DRAFT revision, never a live one, and the note under the
 * editor says so. A YAML file that names plugins this tenant has not mirrored,
 * or wires a report into a blob, is a perfectly valid file and an invalid
 * pipeline; importing it straight to the current revision would replace
 * something that ran with something that cannot, and the first anyone would
 * know is the next trigger firing.
 *
 * The three source buttons are all offered even though only pasting is wired,
 * because the choice a user makes here is "where is my YAML", and hiding two
 * of the three answers makes the panel look like it only understands
 * clipboards.
 *
 * There is no import RPC. The textarea's contents are a fixture and the title
 * row carries a permanent sample chip; `onImport` hands the caller whatever is
 * in the box, so the day the RPC exists nothing in here changes.
 */
import { useState } from "react";

import { Modal, ModalButton } from "./Modal.js";

/** ImportSource is where the YAML is coming from. Only `paste` is wired; the
 * other two are named so the panel does not pretend they are impossible. */
export type ImportSource = "paste" | "file" | "git";

const sources: readonly { id: ImportSource; label: string }[] = [
  { id: "paste", label: "paste YAML" },
  { id: "file", label: "choose local file…" },
  { id: "git", label: "from git repo…" },
];

const sampleYaml = `name: nightly-etl
on:
  schedule: {cron: "0 4 * * *"}
steps:
  extract:
    plugin: dhole/pg-dump@2.1.0
    in: {tick: $trigger.tick}
  transform:
    plugin: dhole/run-container@1.2.0
    effect: pure
    in: {data: extract.dump}
  load:
    plugin: dhole/s3-put@1.0.3
    effect: at-most-once
    in: {archive: transform.out}`;

export function ImportModal({
  onClose,
  onImport,
  onPickSource,
  initialYaml = sampleYaml,
  sample = true,
}: {
  readonly onClose: () => void;
  /** Hands over the YAML to validate and import as a draft revision. */
  readonly onImport: (yaml: string) => void;
  /** Called when the user picks a source other than pasting — a file dialog
   * and a repo browser both live outside this component. */
  readonly onPickSource: (source: ImportSource) => void;
  readonly initialYaml?: string;
  readonly sample?: boolean;
}) {
  const [yaml, setYaml] = useState(initialYaml);
  const [source, setSource] = useState<ImportSource>("paste");

  return (
    <Modal
      title="import pipeline from YAML"
      width={520}
      sample={sample}
      onClose={onClose}
      footer={
        <>
          <ModalButton kind="secondary" onClick={onClose}>
            cancel
          </ModalButton>
          <ModalButton
            kind="primary"
            onClick={() => {
              onImport(yaml);
            }}
          >
            validate &amp; import
          </ModalButton>
        </>
      }
    >
      <div
        style={{
          padding: 16,
          display: "flex",
          flexDirection: "column",
          gap: 10,
          fontSize: 10,
        }}
      >
        <div style={{ display: "flex", gap: 8 }}>
          {sources.map((entry) => {
            const active = entry.id === source;
            return (
              <button
                key={entry.id}
                type="button"
                aria-pressed={active}
                onClick={() => {
                  setSource(entry.id);
                  if (entry.id !== "paste") onPickSource(entry.id);
                }}
                style={{
                  flex: 1,
                  background: active ? "var(--accent-soft)" : "none",
                  border: `1px solid ${active ? "var(--accent)" : "var(--line)"}`,
                  borderRadius: 5,
                  color: active ? "var(--accent)" : "var(--ink2)",
                  fontSize: 10,
                  padding: "7px 0",
                  cursor: "pointer",
                }}
              >
                {entry.label}
              </button>
            );
          })}
        </div>
        <textarea
          rows={9}
          spellCheck={false}
          aria-label="pipeline YAML"
          value={yaml}
          onChange={(event) => {
            setYaml(event.currentTarget.value);
          }}
          className="dh-selectable"
          style={{
            width: "100%",
            boxSizing: "border-box",
            background: "var(--panel2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            color: "var(--ink)",
            fontSize: 9,
            lineHeight: 1.6,
            padding: 9,
            outline: "none",
            resize: "none",
          }}
        />
        <div style={{ color: "var(--ink3)" }}>
          imported YAML becomes a draft revision in the database — validated
          first, with diagnostics if any port types don&apos;t line up.
        </div>
      </div>
    </Modal>
  );
}
