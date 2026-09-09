/**
 * The review a change passes through before it is applied, and the record of
 * what the control plane actually did once it was.
 *
 * TWO THINGS ARE DELIBERATE HERE.
 *
 * A save is not a save. It opens this view, and the operation reaches
 * ApplyOperation only when the person confirms it. An editor that applies on
 * click and offers an undo afterwards is a different promise: undo is exact
 * (ADR 0020), but a pipeline definition is reviewed and approved by people,
 * and "it was applied and then reverted" is a different sentence in an audit
 * log from "it was never applied".
 *
 * POLICY DECISION POINTS ARE THE SERVER'S, NOT THIS FILE'S. A warning shown
 * here is a diagnostic the control plane returned from Validate — for the
 * effect-class case, the exact sentence internal/catalog composed in
 * ResolveStep (Entry.OverrideWarnings) when a step widened its plugin's
 * declaration, handed out by internal/api/validate.go. This component
 * highlights every warning the plane attached to a step this change touches
 * and computes none of them: a browser that decided for itself which override
 * was dangerous would be a second implementation of ADR 0002's rule, free to
 * drift from the one that governs the run.
 */
import type { Diagnostic, Diff } from "../gen/dhole/v1/api_pb.js";

/** DiffViewProps is one diff and the plane's opinion of the result.
 *
 * `diff` is a Diff message: after ApplyOperation it is the one the server
 * returned; before it, the changes this editor is proposing, which the
 * heading says out loud so the two are never confused.
 */
export interface DiffViewProps {
  readonly revisionId: string;
  readonly diff: Diff | undefined;
  /** Diagnostics the control plane returned for the resulting definition. */
  readonly diagnostics: readonly Diagnostic[];
  /** True once ApplyOperation has run: the diff shown is then the plane's. */
  readonly applied: boolean;
  readonly onConfirm?: (() => void) | undefined;
  readonly onCancel?: (() => void) | undefined;
  readonly busy?: boolean | undefined;
}

/** DiffView shows what a change does, and asks. */
export function DiffView({
  revisionId,
  diff,
  diagnostics,
  applied,
  onConfirm,
  onCancel,
  busy = false,
}: DiffViewProps) {
  const changes = diff?.changes ?? [];
  const decisions = diagnostics.filter((d) => d.severity === "warning");
  const errors = diagnostics.filter((d) => d.severity !== "warning");

  return (
    <section
      data-testid={applied ? "diff-applied" : "diff-view"}
      role={applied ? "status" : "dialog"}
      aria-label={applied ? "applied change" : "review this change"}
      style={{ border: "1px solid #cbd5e0", padding: 12, marginTop: 12 }}
    >
      <h3>{applied ? "applied" : "review this change"}</h3>
      <p>
        {applied ? "now at " : "based on "}
        <code data-testid="diff-revision">{revisionId}</code>
      </p>

      {changes.length === 0 ? (
        <p data-testid="diff-empty">nothing to apply.</p>
      ) : (
        <ul>
          {changes.map((change, index) => (
            <li
              // The summary is the server's own sentence and is not unique on
              // its own — two steps can be set to the same value — so the
              // position in the diff is part of the key.
              key={`${index}-${change.summary}`}
              data-testid="diff-change"
            >
              {change.summary}
            </li>
          ))}
        </ul>
      )}

      {decisions.length > 0 && (
        <ul aria-label="policy decision points">
          {decisions.map((decision) => (
            <li
              key={`${decision.stepId}-${decision.message}`}
              data-testid="policy-decision"
              style={{ color: "#975a16" }}
            >
              <strong>policy decision:</strong> {decision.message}
            </li>
          ))}
        </ul>
      )}

      {errors.length > 0 && (
        <ul aria-label="problems with the result">
          {errors.map((problem) => (
            <li
              key={`${problem.stepId}-${problem.message}`}
              data-testid="diff-error"
              style={{ color: "#c53030" }}
            >
              {problem.message}
            </li>
          ))}
        </ul>
      )}

      {!applied && (
        <p>
          <button
            data-testid="diff-confirm"
            type="button"
            disabled={busy}
            onClick={onConfirm}
          >
            apply
          </button>{" "}
          <button
            data-testid="diff-cancel"
            type="button"
            disabled={busy}
            onClick={onCancel}
          >
            cancel
          </button>
        </p>
      )}
    </section>
  );
}
