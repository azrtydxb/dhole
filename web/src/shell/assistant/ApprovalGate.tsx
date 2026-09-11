/**
 * A person deciding whether an at-most-once effect happens.
 *
 * This is not a confirmation dialog. The run is already paused at a gate, the
 * step behind it is dispatched ONCE OR NEVER, and the decision made here is
 * recorded in the run's event log against the principal the credential
 * authenticated. Approving releases the run; denying fails it, and the step is
 * never dispatched at all.
 *
 * THREE THINGS FOLLOW FROM THAT, and each one prevents a specific accident.
 *
 * The step is stated in full — pipeline, revision, run, step, effect class,
 * engine — at the largest type in the dialog. An approval dialog that says
 * "approve this step?" is how somebody approves the wrong deploy: they were
 * told a run was waiting, not which one.
 *
 * Denying is exactly as reachable as approving: same size, same row, no extra
 * click, no confirmation on top. A gate where refusing is the awkward path
 * teaches people to approve, which makes the gate decorative.
 *
 * And neither button works until a reason is typed. The reason is the whole
 * value of the record six months later — "denied" with no sentence beside it
 * is indistinguishable from a misclick, and the run that stopped for no stated
 * reason is the worst possible artefact of a control plane that keeps an audit
 * trail. Escape closes without deciding, because closing is not denying.
 */
import { useState } from "react";

import { useDialogChrome } from "./dialog.js";

/** GateRun is the run that is waiting. */
export type GateRun = {
  readonly id: string;
  /** The short label a person recognises — "#129" — when there is one. */
  readonly label?: string;
  readonly pipelineName?: string;
  readonly revisionId?: string;
};

/** GateStep is the step whose gate this is. `effect` is here because the
 * effect class is the reason the gate exists at all: policy requires an
 * approval before an at-most-once step under a non-trusted tier. */
export type GateStep = {
  readonly id: string;
  readonly name: string;
  readonly effect?: string;
  readonly engine?: string;
  readonly approvers?: string;
  readonly timeout?: string;
  /** How long it has been waiting, already phrased by the caller. */
  readonly waitingFor?: string;
  /** What the step will actually do, in the caller's words — the registry it
   * pushes to, the environment it deploys. */
  readonly summary?: string;
};

/** Row is one fact about the gate: label on the left, value on the right, so
 * the facts line up and a missing one is visibly missing. */
function Row({
  label,
  value,
}: {
  readonly label: string;
  readonly value: string;
}) {
  return (
    <div style={{ display: "flex", gap: 10, fontSize: 10 }}>
      <span style={{ color: "var(--ink3)", flex: "none", width: 78 }}>
        {label}
      </span>
      <span className="dh-selectable" style={{ color: "var(--ink2)" }}>
        {value}
      </span>
    </div>
  );
}

export function ApprovalGate({
  run,
  step,
  busy = false,
  onDecide,
  onClose,
}: {
  readonly run: GateRun;
  readonly step: GateStep;
  readonly busy?: boolean;
  readonly onDecide: (approved: boolean, reason: string) => void;
  readonly onClose: () => void;
}) {
  const ref = useDialogChrome<HTMLDivElement>(onClose);
  const [reason, setReason] = useState("");

  const stated = reason.trim();
  const undecidable = stated === "" || busy;
  const decide = (approved: boolean) => {
    if (undecidable) return;
    onDecide(approved, stated);
  };

  return (
    <div
      onClick={onClose}
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 80,
        background: "rgba(0,0,0,0.45)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <div
        ref={ref}
        tabIndex={-1}
        role="dialog"
        aria-modal="true"
        aria-label={`approval gate · ${step.name} · run ${run.label ?? run.id}`}
        // The backdrop closes; a click inside it must not travel up to that.
        onClick={(event) => {
          event.stopPropagation();
        }}
        style={{
          width: 440,
          background: "var(--panel)",
          border: "1px solid var(--err)",
          borderRadius: 8,
          boxShadow: "0 24px 60px rgba(0,0,0,0.5)",
          outline: "none",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            padding: "13px 16px",
            borderBottom: "1px solid var(--line2)",
          }}
        >
          <span
            aria-hidden
            style={{
              width: 8,
              height: 8,
              flex: "none",
              borderRadius: "50%",
              background: "var(--err)",
              animation: "dh-pulse 1.2s infinite",
            }}
          />
          <span style={{ fontSize: 12, fontWeight: 700 }}>
            approval gate · run {run.label ?? run.id}
          </span>
          <button
            type="button"
            onClick={onClose}
            aria-label="close without deciding"
            style={{
              marginLeft: "auto",
              background: "none",
              border: "none",
              color: "var(--ink3)",
              fontSize: 12,
              cursor: "pointer",
            }}
          >
            ✕
          </button>
        </div>

        <div
          style={{
            padding: 16,
            display: "flex",
            flexDirection: "column",
            gap: 10,
            fontSize: 10,
          }}
        >
          <div
            data-testid="gate-subject"
            style={{
              background: "var(--panel2)",
              border: "1px solid var(--err)",
              borderRadius: 6,
              padding: "11px 12px",
              display: "flex",
              flexDirection: "column",
              gap: 7,
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span style={{ fontSize: 13, fontWeight: 700 }}>{step.name}</span>
              {step.effect !== undefined && (
                <span
                  style={{
                    fontSize: 9,
                    color: "var(--err)",
                    border: "1px solid var(--err)",
                    borderRadius: 3,
                    padding: "1px 7px",
                  }}
                >
                  {step.effect}
                </span>
              )}
            </div>
            {step.summary !== undefined && (
              <div
                className="dh-selectable"
                style={{ color: "var(--ink2)", lineHeight: 1.6 }}
              >
                {step.summary}
              </div>
            )}
            {run.pipelineName !== undefined && (
              <Row
                label="pipeline"
                value={
                  run.revisionId === undefined
                    ? run.pipelineName
                    : `${run.pipelineName} · rev ${run.revisionId}`
                }
              />
            )}
            <Row label="run" value={run.id} />
            <Row label="step" value={step.id} />
            {step.engine !== undefined && (
              <Row label="engine" value={step.engine} />
            )}
            {step.approvers !== undefined && (
              <Row label="approvers" value={step.approvers} />
            )}
            {(step.waitingFor !== undefined || step.timeout !== undefined) && (
              <Row
                label="waiting"
                value={[
                  step.waitingFor,
                  step.timeout === undefined
                    ? undefined
                    : `timeout ${step.timeout}`,
                ]
                  .filter((part) => part !== undefined)
                  .join(" · ")}
              />
            )}
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
            this step is at-most-once: approving dispatches it exactly once and
            releases the rest of the run; denying halts the run and it is never
            dispatched. Closing this dialog decides nothing — the run keeps
            waiting.
          </div>

          <div>
            <label
              htmlFor="gate-reason"
              style={{
                color: "var(--ink2)",
                display: "block",
                marginBottom: 4,
              }}
            >
              reason{" "}
              <span style={{ color: "var(--ink3)" }}>
                · recorded in the run&apos;s event log
              </span>
            </label>
            <input
              id="gate-reason"
              data-autofocus
              value={reason}
              disabled={busy}
              placeholder="why this is approved, or why it is not"
              onChange={(event) => setReason(event.currentTarget.value)}
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
        </div>

        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            padding: "12px 16px",
            borderTop: "1px solid var(--line2)",
          }}
        >
          <span
            data-testid="gate-hint"
            style={{
              flex: 1,
              fontSize: 9,
              color: undecidable ? "var(--warn)" : "var(--ink3)",
            }}
          >
            {stated === ""
              ? "a decision needs a reason before it can be recorded"
              : "recorded against you, on this run, permanently"}
          </span>
          <button
            type="button"
            disabled={undecidable}
            onClick={() => decide(true)}
            style={{
              width: 104,
              background: "var(--ok)",
              border: "1px solid var(--ok)",
              borderRadius: 5,
              // On-accent text, as Toolbar's primary button does: no palette
              // token is white in either theme.
              color: "#fff",
              fontSize: 10,
              fontWeight: 700,
              padding: "7px 0",
              cursor: undecidable ? "default" : "pointer",
              opacity: undecidable ? 0.5 : 1,
            }}
          >
            APPROVE
          </button>
          <button
            type="button"
            disabled={undecidable}
            onClick={() => decide(false)}
            style={{
              width: 104,
              background: "none",
              border: "1px solid var(--err)",
              borderRadius: 5,
              color: "var(--err)",
              fontSize: 10,
              fontWeight: 700,
              padding: "7px 0",
              cursor: undecidable ? "default" : "pointer",
              opacity: undecidable ? 0.5 : 1,
            }}
          >
            DENY
          </button>
        </div>
      </div>
    </div>
  );
}
