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
 */
import { Handle, Position, type Node, type NodeProps } from "@xyflow/react";

import type { Port, Step } from "../gen/dhole/v1/pipeline_pb.js";

/** StepNodeData is what the canvas hands each node: the step itself. */
export type StepNodeData = { readonly step: Step };

/** StepNodeType is this node's type as React Flow sees it. */
export type StepNodeType = Node<StepNodeData, "step">;

/** The geometry of the ports down each side of a node. */
const firstPortTop = 44;
const portSpacing = 24;

/** describePort is the port's type in a word, for the label beside it. Its
 * only job is to make a mistyped wire obvious BEFORE it is attempted. */
function describePort(port: Port): string {
  switch (port.type?.kind.case) {
    case "blob":
      return "blob";
    case "structured":
      return `structured ${port.type.kind.value.schemaId}`;
    default:
      return "untyped";
  }
}

/** StepNode renders one step and its ports. */
export function StepNode({ data }: NodeProps<StepNodeType>) {
  const { step } = data;
  const height =
    firstPortTop +
    portSpacing * Math.max(step.inputs.length, step.outputs.length, 1);

  return (
    <div
      data-testid={`step-node-${step.id}`}
      style={{
        position: "relative",
        width: 220,
        minHeight: height,
        padding: "8px 10px",
        border: "1px solid #4a5568",
        borderRadius: 6,
        background: "#ffffff",
        fontSize: 12,
      }}
    >
      <strong>{step.name === "" ? step.id : step.name}</strong>
      <div style={{ color: "#718096" }}>{step.pluginRef || "no plugin"}</div>

      {step.inputs.map((port, index) => (
        <Handle
          key={`in-${port.name}`}
          type="target"
          position={Position.Left}
          id={port.name}
          data-testid={`port-in-${step.id}-${port.name}`}
          title={`${port.name}: ${describePort(port)}`}
          style={{
            top: firstPortTop + index * portSpacing,
            width: 14,
            height: 14,
            background: "#2b6cb0",
          }}
        />
      ))}

      {step.outputs.map((port, index) => (
        <Handle
          key={`out-${port.name}`}
          type="source"
          position={Position.Right}
          id={port.name}
          data-testid={`port-out-${step.id}-${port.name}`}
          title={`${port.name}: ${describePort(port)}`}
          style={{
            top: firstPortTop + index * portSpacing,
            width: 14,
            height: 14,
            background: "#2f855a",
          }}
        />
      ))}

      <ul style={{ margin: "6px 0 0", padding: 0, listStyle: "none" }}>
        {step.inputs.map((port) => (
          <li key={`label-in-${port.name}`} style={{ color: "#2b6cb0" }}>
            {port.name}: {describePort(port)}
          </li>
        ))}
        {step.outputs.map((port) => (
          <li
            key={`label-out-${port.name}`}
            style={{ color: "#2f855a", textAlign: "right" }}
          >
            {port.name}: {describePort(port)}
          </li>
        ))}
      </ul>
    </div>
  );
}
