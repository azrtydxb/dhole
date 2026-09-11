/**
 * Connecting a git mirror, and saying out loud which direction it runs.
 *
 * The paragraph in the middle is the whole point of the panel. A box asking
 * for a remote, a branch and a path looks exactly like "sync my pipelines with
 * git", and a user who reads it that way will edit a YAML file in the repo and
 * wait for the change to appear here. It never will: the mirror is ONE WAY
 * (ADR 0008), the database stays authoritative, and an edit made in git is
 * reported as drift rather than imported. Saying so on the form is cheaper
 * than the support conversation.
 *
 * There is no git RPC on the control plane yet, so the fields below are a
 * fixture and the title row carries a permanent sample chip. `onConnect` is a
 * real callback so the day the RPC lands only the caller changes.
 */
import { useState } from "react";

import { Modal, ModalButton } from "./Modal.js";

/** GitMirror is the three fields a one-way mirror needs. */
export type GitMirror = {
  readonly remote: string;
  readonly branch: string;
  readonly path: string;
};

function Field({
  label,
  value,
  onChange,
}: {
  readonly label: string;
  readonly value: string;
  readonly onChange: (next: string) => void;
}) {
  return (
    <div style={{ flex: 1, minWidth: 0 }}>
      <div style={{ color: "var(--ink2)", marginBottom: 4 }}>{label}</div>
      <input
        aria-label={label}
        value={value}
        onChange={(event) => {
          onChange(event.currentTarget.value);
        }}
        style={{
          width: "100%",
          boxSizing: "border-box",
          background: "var(--panel2)",
          border: "1px solid var(--line)",
          borderRadius: 5,
          color: "var(--ink)",
          fontSize: 10,
          padding: "7px 9px",
          outline: "none",
        }}
      />
    </div>
  );
}

export function GitModal({
  onClose,
  onConnect,
  initial = {
    remote: "git@git.acme.dev:platform/pipelines.git",
    branch: "main",
    path: "pipelines/",
  },
  sample = true,
}: {
  readonly onClose: () => void;
  readonly onConnect: (mirror: GitMirror) => void;
  readonly initial?: GitMirror;
  readonly sample?: boolean;
}) {
  const [remote, setRemote] = useState(initial.remote);
  const [branch, setBranch] = useState(initial.branch);
  const [path, setPath] = useState(initial.path);

  return (
    <Modal
      title="connect git mirror"
      width={440}
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
              onConnect({ remote, branch, path });
            }}
          >
            connect mirror
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
        <Field label="remote" value={remote} onChange={setRemote} />
        <div style={{ display: "flex", gap: 10 }}>
          <Field label="branch" value={branch} onChange={setBranch} />
          <Field label="path" value={path} onChange={setPath} />
        </div>
        <div
          style={{
            color: "var(--ink3)",
            lineHeight: 1.6,
            border: "1px solid var(--line2)",
            borderRadius: 5,
            padding: "8px 10px",
          }}
        >
          one-way mirror (ADR 0008): the database stays authoritative. Every
          saved revision pushes its YAML here; edits made directly in git are
          reported as drift, never imported.
        </div>
      </div>
    </Modal>
  );
}
