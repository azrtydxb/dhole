/**
 * What is wrong with this pipeline, above the status bar, always.
 *
 * The strip is collapsible but never hidden: a pipeline with an error that the
 * user has to go looking for is a pipeline they will try to run. Collapsed it
 * still shows the counts, so the cost of ignoring it stays visible.
 *
 * Each row names its SOURCE — validate, registry, policy — because "required
 * input not connected" and "engine offline" are fixed in completely different
 * places, and a user who cannot tell which system is complaining has to guess.
 */

/** Severity orders the strip. Errors first: a warning above an error means
 * scrolling past the thing that will actually fail. */
export type Severity = "error" | "warning" | "info";

export type Diagnostic = {
  readonly id: string;
  readonly severity: Severity;
  readonly text: string;
  /** Which subsystem said so. */
  readonly source: string;
  /** The step this is about, when it is about one — clicking selects it. */
  readonly stepId?: string;
};

const severityToken: Record<Severity, string> = {
  error: "var(--err)",
  warning: "var(--warn)",
  info: "var(--ink2)",
};

const severityIcon: Record<Severity, string> = {
  error: "●",
  warning: "▲",
  info: "·",
};

export function Diagnostics({
  diagnostics,
  open,
  onToggle,
  onSelectStep,
}: {
  readonly diagnostics: readonly Diagnostic[];
  readonly open: boolean;
  readonly onToggle: () => void;
  readonly onSelectStep: (stepId: string) => void;
}) {
  const errors = diagnostics.filter((d) => d.severity === "error").length;
  const warnings = diagnostics.filter((d) => d.severity === "warning").length;

  return (
    <div
      style={{
        borderTop: "1px solid var(--line2)",
        background: "var(--panel2)",
        maxHeight: open ? 132 : 30,
        overflow: "hidden",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <button
        type="button"
        onClick={onToggle}
        aria-expanded={open}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 14,
          padding: "0 14px",
          height: 30,
          flex: "none",
          background: "transparent",
          border: "none",
          color: "var(--ink2)",
          fontSize: 10,
          cursor: "pointer",
          textAlign: "left",
        }}
      >
        <span style={{ color: "var(--err)" }}>
          ● {errors} error{errors === 1 ? "" : "s"}
        </span>
        <span style={{ color: "var(--warn)" }}>
          ▲ {warnings} warning{warnings === 1 ? "" : "s"}
        </span>
        <span style={{ flex: 1 }} />
        <span aria-hidden>{open ? "▾" : "▴"}</span>
      </button>

      {open && (
        <div style={{ overflowY: "auto" }}>
          {diagnostics.length === 0 && (
            <div
              style={{
                padding: "6px 14px",
                fontSize: 10,
                color: "var(--ink3)",
              }}
            >
              nothing to report — validate has not found a problem with this
              revision
            </div>
          )}
          {diagnostics.map((diagnostic) => (
            <div
              key={diagnostic.id}
              onClick={() => {
                if (diagnostic.stepId !== undefined)
                  onSelectStep(diagnostic.stepId);
              }}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                padding: "5px 14px",
                fontSize: 10,
                color: "var(--ink2)",
                cursor: diagnostic.stepId === undefined ? "default" : "pointer",
              }}
            >
              <span style={{ color: severityToken[diagnostic.severity] }}>
                {severityIcon[diagnostic.severity]}
              </span>
              <span className="dh-selectable">{diagnostic.text}</span>
              <span style={{ flex: 1 }} />
              <span style={{ color: "var(--ink3)" }}>{diagnostic.source}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
