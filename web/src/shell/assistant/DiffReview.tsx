/**
 * What an agent wants to do to this pipeline, BEFORE it has done it.
 *
 * Every operation the control plane applies returns a diff and its exact
 * inverse (ADR 0020), which makes "apply it and undo if wrong" technically
 * sound and organisationally wrong: a definition that was applied and reverted
 * reads differently in an audit log from one that was never applied, and the
 * revision numbers in between are real. So an agent's edit stops here, as a
 * list of changes a person reads, and reaches ApplyOperation only on APPLY.
 *
 * The changes are the SERVER'S sentences. This component groups them by kind
 * and colours the verdict; it does not describe an operation in its own words,
 * because a description composed in the browser could differ from the thing
 * the button sends. Nothing here is sample data: the operations, their diffs
 * and their inverses are all in the contract.
 */
import { useDialogChrome } from "./dialog.js";

/** ReviewChangeKind mirrors dhole.v1.ChangeKind — added, removed, changed —
 * without binding this component to the wire type, the way Diagnostics does
 * not bind to the Diagnostic message. The caller maps one to the other, so a
 * field the contract gains is not a field this panel has an opinion about. */
export type ReviewChangeKind = "added" | "removed" | "changed";

/** ReviewChange is one element the operation touched. `detail` is the second
 * line: the digest, the port types, the resolved plugin — the part that says
 * whether the change is the one you asked for. */
export type ReviewChange = {
  readonly kind: ReviewChangeKind;
  /** The operation's name, when the caller has it: add_step, connect… */
  readonly op?: string;
  readonly summary: string;
  readonly detail?: string;
};

const kindToken: Record<ReviewChangeKind, string> = {
  added: "var(--ok)",
  removed: "var(--err)",
  changed: "var(--warn)",
};

const kindGlyph: Record<ReviewChangeKind, string> = {
  added: "+",
  removed: "−",
  changed: "~",
};

export function DiffReview({
  changes,
  title = "agent edit · claude",
  prompt,
  revisionId,
  note = "each op returned a diff + inverse · applying creates a new revision and mirrors YAML to git",
  busy = false,
  onApprove,
  onReject,
  onClose,
}: {
  readonly changes: readonly ReviewChange[];
  readonly title?: string;
  /** What was asked for, quoted back — the sentence the operations answer. */
  readonly prompt?: string;
  /** The revision the operations were computed against. */
  readonly revisionId?: string;
  readonly note?: string;
  readonly busy?: boolean;
  readonly onApprove: (changes: readonly ReviewChange[]) => void;
  readonly onReject: (changes: readonly ReviewChange[]) => void;
  readonly onClose: () => void;
}) {
  const ref = useDialogChrome<HTMLDivElement>(onClose);
  const count = changes.length;

  return (
    <div
      ref={ref}
      tabIndex={-1}
      role="dialog"
      aria-label="review this agent edit"
      style={{
        position: "absolute",
        inset: 0,
        zIndex: 5,
        background: "var(--panel)",
        borderLeft: "2px solid var(--accent)",
        display: "flex",
        flexDirection: "column",
        outline: "none",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "11px 14px",
          borderBottom: "1px solid var(--line2)",
        }}
      >
        <span
          aria-hidden
          style={{
            width: 7,
            height: 7,
            flex: "none",
            borderRadius: "50%",
            background: "var(--accent)",
          }}
        />
        <span style={{ fontSize: 11, fontWeight: 700 }}>{title}</span>
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
          flex: 1,
          minHeight: 0,
          overflowY: "auto",
          padding: 14,
          fontSize: 9,
          lineHeight: 1.7,
        }}
      >
        <div style={{ color: "var(--ink2)", marginBottom: 12 }}>
          {prompt !== undefined && `"${prompt}" — `}
          {count} operation{count === 1 ? "" : "s"}
          {revisionId !== undefined && ` against rev ${revisionId}`}:
        </div>

        {count === 0 && (
          <div data-testid="diff-review-empty" style={{ color: "var(--ink3)" }}>
            nothing to apply — the agent proposed no change to this revision.
          </div>
        )}

        {changes.map((change, index) => (
          <div
            // Two changes can carry the same sentence (the same property set on
            // two steps), so the position in the diff is part of the key.
            key={`${index}-${change.summary}`}
            data-testid="diff-review-change"
            className="dh-selectable"
            style={{
              border: "1px solid var(--line2)",
              borderRadius: 5,
              padding: "8px 10px",
              marginBottom: index === count - 1 ? 12 : 6,
            }}
          >
            <span style={{ color: kindToken[change.kind] }}>
              {kindGlyph[change.kind]} {change.op ?? change.kind}
            </span>{" "}
            <span style={{ color: "var(--ink)" }}>{change.summary}</span>
            {change.detail !== undefined && (
              <>
                <br />
                <span style={{ color: "var(--ink3)" }}>{change.detail}</span>
              </>
            )}
          </div>
        ))}

        <div style={{ color: "var(--ink3)" }}>{note}</div>
      </div>

      <div
        style={{
          display: "flex",
          gap: 8,
          padding: "12px 14px",
          borderTop: "1px solid var(--line2)",
        }}
      >
        <button
          type="button"
          data-autofocus
          disabled={busy || count === 0}
          onClick={() => onApprove(changes)}
          style={{
            flex: 1,
            background: "var(--ok)",
            border: "none",
            borderRadius: 5,
            // On-accent text, as Toolbar's primary button does: no palette
            // token is white in either theme.
            color: "#fff",
            fontSize: 10,
            fontWeight: 700,
            padding: "8px 0",
            cursor: busy || count === 0 ? "default" : "pointer",
            opacity: busy || count === 0 ? 0.5 : 1,
          }}
        >
          APPLY
        </button>
        <button
          type="button"
          disabled={busy}
          onClick={() => onReject(changes)}
          style={{
            flex: 1,
            background: "none",
            border: "1px solid var(--line)",
            borderRadius: 5,
            color: "var(--ink2)",
            fontSize: 10,
            padding: "8px 0",
            cursor: busy ? "default" : "pointer",
          }}
        >
          REJECT
        </button>
      </div>
    </div>
  );
}
