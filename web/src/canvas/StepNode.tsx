/**
 * One step, drawn with ONE HANDLE PER DECLARED PORT.
 *
 * That is the whole design. A node in a shared-workspace tool has one input
 * and one output because everything else travels through the filesystem;
 * here a step is (environment, command, declared inputs) -> declared outputs
 * (ADR 0001), so the ports ARE the interface and the node has to show them
 * individually or the user cannot say which output feeds which input.
 *
 * The ports come from the step. Nothing here knows a plugin, a step type or a
 * shape: a step declaring no ports draws no handles and is perfectly valid -
 * a step nobody has wired up yet, which is what every step is for a moment
 * after it is added.
 *
 * The visual language is the editor's, from the Dhole Editor design: a status
 * rail down the left edge, ports as type-shaped glyphs with the label reading
 * `name:type`, and a footer carrying the two facts that decide what the
 * scheduler may do with this step — its effect class and where it will run.
 * Every colour is a token so the node follows the theme without knowing there
 * is one.
 */
import { Handle, Position, type Node, type NodeProps } from "@xyflow/react";

import { PortGlyph, asPortTypeName } from "../design/PortGlyph.js";
import { EffectClass } from "../gen/dhole/v1/common_pb.js";
import type { Port, Step } from "../gen/dhole/v1/pipeline_pb.js";

/** StepStatus is what a run says about this step right now. `none` is the
 * editing state: no run is attached and the node is just a definition. */
export type StepStatus =
  "none" | "queued" | "running" | "succeeded" | "failed" | "cached" | "blocked";

/** StepNodeData is what the canvas hands each node: the step itself, plus
 * whatever a run has said about it. */
export type StepNodeData = {
  readonly step: Step;
  readonly status?: StepStatus;
  /** What a DRY RUN says would happen to this step. Drawn as a chip above the
   * node, in the design's wording, and never at the same time as a run status:
   * a prediction and a result must not be confusable. */
  readonly badge?: { readonly text: string; readonly token: string };
  /** Set when the step is untrusted or downstream of something untrusted.
   * Drawn as a chip rather than a colour, because "this data came from
   * outside" is the one thing a user must not miss on a glance (ADR 0015). */
  readonly taint?: "untrusted" | "tainted";
};

/** StepNodeType is this node's type as React Flow sees it. */
export type StepNodeType = Node<StepNodeData, "step">;

/** The geometry of a node, in the order it stacks: 1px border, 6px padding,
 * a 16px title row, then one 15px row per declared port, then the footer.
 *
 * These are used to declare each node's size to the minimap. The HANDLES do
 * NOT use them — they are positioned on the glyph itself, because arithmetic
 * that has to agree with a flex layout is arithmetic that will one day be
 * twelve pixels out, which is a wire that visibly misses the dot it claims to
 * connect to. */
export const nodeWidth = 215;
export const titleRow = 16;
export const portSpacing = 15;
export const nodeChrome = 2 + 12 + titleRow + 22;

const statusRail: Record<StepStatus, string> = {
  none: "var(--ink3)",
  queued: "var(--ink3)",
  running: "var(--accent)",
  succeeded: "var(--ok)",
  failed: "var(--err)",
  cached: "var(--ok)",
  blocked: "var(--err)",
};

const statusLabel: Record<StepStatus, string> = {
  none: "",
  queued: "QUEUED",
  running: "RUN",
  succeeded: "✓",
  failed: "FAILED",
  cached: "HIT",
  blocked: "GATE",
};

/** portTypeName is the port's declared type in the word the glyphs are keyed
 * on. Its only job is to make a mistyped wire obvious BEFORE it is attempted. */
function portTypeName(port: Port): string {
  switch (port.type?.kind.case) {
    case "blob":
      return "blob";
    case "structured":
      return port.type.kind.value.schemaId;
    default:
      return "unknown";
  }
}

/** effectLabel spells the wire enum the way the editor talks about it.
 *
 * It switches on the generated enum rather than its numbers: a step whose
 * effect class the editor mislabels is a step whose retry and cache behaviour
 * the user has been told wrongly, and a bare `case 2` is one renumbering away
 * from saying that with a straight face. */
function effectLabel(step: Step): string {
  switch (step.effectClass) {
    case EffectClass.PURE:
      return "pure";
    case EffectClass.IDEMPOTENT:
      return "idempotent";
    case EffectClass.AT_MOST_ONCE:
      return "at-most-once";
    default:
      return "unspecified";
  }
}

function PortRow({
  step,
  port,
  direction,
}: {
  readonly step: Step;
  readonly port: Port;
  readonly direction: "in" | "out";
}) {
  const type = portTypeName(port);
  const glyph = asPortTypeName(type);
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 6,
        height: portSpacing,
        fontSize: 9,
        color: "var(--ink2)",
        flexDirection: direction === "in" ? "row" : "row-reverse",
      }}
    >
      {/* The handle sits ON the glyph, not at a computed offset from the top
          of the node. React Flow takes an edge's endpoint from the handle's
          real position in the DOM, so anchoring it to the glyph is what makes
          a wire terminate exactly on the dot it belongs to — at any port
          count, in any theme, whatever the row height turns out to be. */}
      <span
        style={{
          position: "relative",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          width: 11,
          height: portSpacing,
          flex: "none",
        }}
      >
        <Handle
          type={direction === "in" ? "target" : "source"}
          position={direction === "in" ? Position.Left : Position.Right}
          id={port.name}
          data-testid={`port-${direction}-${step.id}-${port.name}`}
          title={`${port.name}: ${type}`}
          style={{
            position: "absolute",
            left: "50%",
            top: "50%",
            transform: "translate(-50%, -50%)",
            width: 13,
            height: 13,
            minWidth: 0,
            minHeight: 0,
            background: "transparent",
            border: "none",
            // Invisible: a 13px circle drawn over a 9px shape would hide the
            // type the shape exists to state.
          }}
        />
        <PortGlyph type={glyph} title={type} />
      </span>
      <span style={{ pointerEvents: "none" }}>
        {port.name}:{type}
      </span>
    </div>
  );
}

/** StepNode renders one step and its ports. */
export function StepNode({ data, selected }: NodeProps<StepNodeType>) {
  const { step } = data;
  const status = data.status ?? "none";
  const rail = statusRail[status];
  const label = statusLabel[status];

  return (
    <div
      data-testid={`step-node-${step.id}`}
      style={{
        position: "relative",
        width: nodeWidth,
        display: "flex",
        background: "var(--panel)",
        border: `1px solid ${selected === true ? "var(--accent)" : "var(--line)"}`,
        borderRadius: 6,
        boxShadow:
          selected === true
            ? "0 0 0 3px var(--accent-soft)"
            : "0 2px 8px rgba(0,0,0,0.25)",
        fontFamily: "var(--font)",
        opacity: status === "cached" ? 0.72 : 1,
      }}
    >
      <div
        aria-hidden
        style={{
          width: 4,
          flex: "none",
          background: rail,
          borderRadius: "5px 0 0 5px",
        }}
      />
      <div style={{ flex: 1, padding: "6px 9px", minWidth: 0 }}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "baseline",
            gap: 6,
            fontSize: 11,
            height: 16,
          }}
        >
          <span
            style={{
              fontWeight: 700,
              color: "var(--ink)",
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
            }}
          >
            {step.name === "" ? step.id : step.name}
          </span>
          {label !== "" && (
            <span style={{ color: rail, fontSize: 9, whiteSpace: "nowrap" }}>
              {label}
            </span>
          )}
        </div>

        {step.inputs.map((port) => (
          <PortRow
            key={`in-${port.name}`}
            step={step}
            port={port}
            direction="in"
          />
        ))}
        {step.outputs.map((port) => (
          <PortRow
            key={`out-${port.name}`}
            step={step}
            port={port}
            direction="out"
          />
        ))}

        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            gap: 8,
            fontSize: 8,
            color: "var(--ink3)",
            borderTop: "1px solid var(--line2)",
            marginTop: 4,
            paddingTop: 4,
          }}
        >
          <span>{effectLabel(step)}</span>
          <span
            style={{
              overflow: "hidden",
              textOverflow: "ellipsis",
              whiteSpace: "nowrap",
            }}
          >
            {step.engineType === "" ? "any engine" : step.engineType}
          </span>
        </div>

        {data.badge !== undefined && (
          <div
            style={{
              position: "absolute",
              top: -9,
              right: 8,
              fontSize: 8,
              background: "var(--panel)",
              color: data.badge.token,
              border: `1px solid ${data.badge.token}`,
              borderRadius: 3,
              padding: "1px 5px",
              whiteSpace: "nowrap",
            }}
          >
            {data.badge.text}
          </div>
        )}

        {data.taint !== undefined && (
          <div
            style={{
              position: "absolute",
              top: -9,
              left: 8,
              fontSize: 8,
              background: "var(--panel)",
              color: "var(--err)",
              border: "1px solid var(--err)",
              borderRadius: 3,
              padding: "1px 5px",
            }}
          >
            ⚠ {data.taint}
          </div>
        )}
      </div>
    </div>
  );
}
