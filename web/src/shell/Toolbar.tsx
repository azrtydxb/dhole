/**
 * The four things you do to a pipeline, in the order you do them.
 *
 * validate → plan → run → save is not a stylistic ordering. Validate and plan
 * are free and answer "would this work"; run and save change something. Put
 * `run` first and the cheapest question becomes the one nobody asks.
 *
 * `plan` is labelled "dry-run" because that is what it is: the plan RPC walks
 * the DAG and reports what WOULD execute, what would come from cache and what
 * cannot be placed, without dispatching anything.
 */

/** ToolbarAction is a command the shell knows how to run. `busy` draws the
 * in-flight state on the one button that is working rather than disabling the
 * whole bar, so a slow validate does not block a save. */
export type ToolbarAction = {
  readonly id: string;
  readonly label: string;
  readonly primary?: boolean;
  readonly busy?: boolean;
  readonly disabled?: boolean;
};

export function Toolbar({
  actions,
  onAction,
  status,
  agentEditsPending,
  onOpenAgentEdits,
  onOpenAssistant,
}: {
  readonly actions: readonly ToolbarAction[];
  readonly onAction: (id: string) => void;
  readonly status: string;
  readonly agentEditsPending: number;
  readonly onOpenAgentEdits: () => void;
  readonly onOpenAssistant: () => void;
}) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 10,
        padding: "0 14px",
        background: "var(--panel)",
        borderBottom: "1px solid var(--line2)",
      }}
    >
      {actions.map((action) => {
        const isPrimary = action.primary === true;
        return (
          <button
            key={action.id}
            type="button"
            disabled={action.disabled === true || action.busy === true}
            onClick={() => onAction(action.id)}
            style={{
              background: isPrimary ? "var(--accent)" : "var(--panel2)",
              border: `1px solid ${isPrimary ? "var(--accent)" : "var(--line)"}`,
              color: isPrimary ? "#fff" : "var(--ink2)",
              fontSize: 11,
              padding: "6px 14px",
              borderRadius: 5,
              cursor:
                action.disabled === true || action.busy === true
                  ? "default"
                  : "pointer",
              opacity: action.disabled === true ? 0.5 : 1,
              animation:
                action.busy === true ? "dh-pulse 1s infinite" : undefined,
            }}
          >
            {isPrimary ? `▶ ${action.label}` : action.label}
          </button>
        );
      })}

      <span style={{ fontSize: 10, color: "var(--ink3)" }}>{status}</span>

      <div style={{ flex: 1 }} />

      {/* An agent edits this pipeline through the same operation API a person
          does (ADR 0013), so its work arrives as a diff waiting for review
          rather than as a change that already happened. This counter is how
          the user learns there is one. */}
      {agentEditsPending > 0 && (
        <button
          type="button"
          onClick={onOpenAgentEdits}
          style={{
            display: "flex",
            alignItems: "center",
            gap: 7,
            background: "var(--accent-soft)",
            border: "1px solid var(--accent)",
            color: "var(--accent)",
            fontSize: 10,
            padding: "5px 12px",
            borderRadius: 5,
            cursor: "pointer",
          }}
        >
          <span
            aria-hidden
            style={{
              width: 6,
              height: 6,
              borderRadius: "50%",
              background: "var(--accent)",
              animation: "dh-pulse 1.4s infinite",
            }}
          />
          {agentEditsPending} agent edit
          {agentEditsPending === 1 ? "" : "s"} pending
        </button>
      )}

      <button
        type="button"
        onClick={onOpenAssistant}
        style={{
          background: "var(--panel2)",
          border: "1px solid var(--line)",
          color: "var(--ink2)",
          fontSize: 10,
          padding: "5px 12px",
          borderRadius: 5,
          cursor: "pointer",
        }}
      >
        ✳ assistant
      </button>
    </div>
  );
}
