/**
 * A generator, drawn twice, and the two drawings deliberately disagree.
 *
 * IN THE EDITOR IT IS OPAQUE. A generator decides what work there is at
 * RUNTIME — a matrix from an API, one step per file it found — so the
 * authored graph does not contain the steps and cannot be made to. A canvas
 * that drew three boxes inside it would be drawing a guess, and the first
 * time the generator emitted four the picture would be a lie nobody could
 * see. So the authored node says "expands at runtime", draws the ports the
 * step actually declares — it is a step like any other, and those ports are
 * what the rest of the graph was type-checked against — and stops there.
 *
 * IN THE RUN VIEW IT IS THE STEPS THAT RAN. Not because the browser guessed
 * better the second time, but because the fragment is IN THE RUN LOG: a run
 * is replayed from its events, so internal/dynamic records what the generator
 * emitted (ADR 0003) and this renders that record. Everything here comes out
 * of the GENERATOR_FRAGMENT_REALISED payload; nothing is recomputed from the
 * definition, which never had these steps in it.
 */
import { Handle, Position, type Node, type NodeProps } from "@xyflow/react";

import type { Port, Step } from "../gen/dhole/v1/pipeline_pb.js";
// The record is READ where the run log is folded, in ../run/events.ts, and
// this half only draws it: a second decoder living here would be a second
// opinion about what internal/dynamic wrote.
import type { RealisedFragment } from "../run/events.js";

/** The plugin ref a generator node carries — internal/dynamic.PluginRef. It
 * is a well-known builtin rather than a plugin the registry resolves, because
 * the control plane splices the fragment itself. */
export const generatorPluginRef = "builtin:generator";

/** isGenerator says whether a step is one of these. The canvas needs it to
 * choose a node type, and it is one comparison in one place so the editor and
 * the run view cannot disagree about what a generator is. */
export function isGenerator(step: Step): boolean {
  return step.pluginRef === generatorPluginRef;
}

/** GeneratorNodeData is what the canvas hands the node: the step itself. */
export type GeneratorNodeData = { readonly step: Step };

/** GeneratorNodeType is this node's type as React Flow sees it. */
export type GeneratorNodeType = Node<GeneratorNodeData, "generator">;

/** The geometry of the ports down each side, matching StepNode so a
 * generator sits in the same grid as everything else. */
const firstPortTop = 60;
const portSpacing = 24;

/** describePort is the port's type in a word, the same wording StepNode
 * uses — a generator's ports are ordinary ports. */
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

/** GeneratorNode renders the authored, opaque form. */
export function GeneratorNode({ data }: NodeProps<GeneratorNodeType>) {
  const { step } = data;
  const height =
    firstPortTop +
    portSpacing * Math.max(step.inputs.length, step.outputs.length, 1);

  return (
    <div
      data-testid={`generator-node-${step.id}`}
      // Read by the run view's own assertions and by anything that needs to
      // know this box is not a claim about what will run.
      data-opaque="true"
      style={{
        position: "relative",
        width: 220,
        minHeight: height,
        padding: "8px 10px",
        // Dashed, because the contents are not decided yet. The border is the
        // only thing on screen that says so at a glance.
        border: "1px dashed #6b46c1",
        borderRadius: 6,
        background: "#faf5ff",
        fontSize: 12,
      }}
    >
      <strong>{step.name === "" ? step.id : step.name}</strong>
      <div style={{ color: "#6b46c1" }}>expands at runtime</div>
      <div style={{ color: "#718096" }}>{step.pluginRef}</div>

      {step.inputs.map((port, index) => (
        <Handle
          key={`in-${port.name}`}
          type="target"
          position={Position.Left}
          id={port.name}
          data-testid={`generator-port-in-${step.id}-${port.name}`}
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
          data-testid={`generator-port-out-${step.id}-${port.name}`}
          title={`${port.name}: ${describePort(port)}`}
          style={{
            top: firstPortTop + index * portSpacing,
            width: 14,
            height: 14,
            background: "#2f855a",
          }}
        />
      ))}
    </div>
  );
}

/** RealisedGeneratorProps is the recorded fragment and nothing else: there is
 * no second source for what a generator produced. */
export interface RealisedGeneratorProps {
  readonly record: RealisedFragment;
}

/** RealisedGenerator renders what a generator actually emitted, for the run
 * view. It never says "expands at runtime": by the time this is on screen the
 * expansion has happened and is in the log. */
export function RealisedGenerator({ record }: RealisedGeneratorProps) {
  return (
    <section
      data-testid={`realised-generator-${record.generator}`}
      data-step-id={record.generator}
      data-realised-count={String(record.steps.length)}
    >
      <strong>{record.generator}</strong>
      <span> realised {record.steps.length} steps</span>
      <ol>
        {record.steps.map((id) => (
          <li key={id} data-testid="realised-step" data-step-id={id}>
            {id}
          </li>
        ))}
      </ol>
    </section>
  );
}
