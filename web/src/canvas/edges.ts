/**
 * Whether an edge can carry data - the browser's copy of internal/dag.
 *
 * The type system exists so the editor can say NO BEFORE A RUN STARTS (ADR
 * 0001). Connecting two ports is type-checked at edit time, not at minute
 * eight of a build, and "at edit time" means at drop time, in the browser,
 * without a round trip. That is the only reason this file exists, because the
 * control plane already knows the answer.
 *
 * Which creates the one hazard worth naming: two implementations of one rule
 * drift. A canvas that accepts an edge dag.TypeCheck rejects sends the user
 * into a run that cannot start; one that rejects an edge dag.TypeCheck accepts
 * makes the type system look broken. So this file is a LINE-FOR-LINE MIRROR of
 * internal/dag/typecheck.go, down to the wording of the diagnostics, and the
 * mirror is not left to good intentions:
 *
 *   - src/canvas/edges.agreement.test.ts runs the REAL dag.TypeCheck, through
 *     a small Go program, over the same pipelines this file checks, and fails
 *     on any disagreement about any of them. It is a differential test against
 *     the actual implementation, not against a transcription of it.
 *   - describe() below switches over the generated oneof and assigns the
 *     unhandled case to `never`, so a PortType arm added to the proto stops
 *     the build here rather than being silently treated as untyped.
 *
 * Change dag.TypeCheck and the agreement test goes red until this file is
 * changed with it. That is the intended workflow, not an accident.
 */
import type {
  Edge,
  Pipeline,
  Port,
  PortType,
} from "../gen/dhole/v1/pipeline_pb.js";

/**
 * Diagnostic is one problem found in a pipeline, addressed to the port it is
 * about so the canvas can put a marker on that port.
 *
 * dag.Diagnostic also carries Line and Col for a definition that came from
 * authored source. A canvas has no source positions - the graph IS the
 * document here - so they are omitted rather than reported as zero.
 */
export interface Diagnostic {
  readonly stepId: string;
  readonly portName: string;
  readonly message: string;
}

/** untyped names the state of a port whose oneof arm was never set. */
const untyped = "an untyped port";

/** quote renders a string the way Go's %q does, which is what makes these
 * messages comparable with the ones dag.TypeCheck produces. */
function quote(value: string): string {
  return JSON.stringify(value);
}

/**
 * typeCheck reports every edge that cannot carry data, in edge order: a
 * dangling reference to a step or a port, or two ends whose types do not
 * match. It returns findings rather than the first error because the canvas
 * draws all of them at once, and it never stops early for the same reason.
 *
 * The type system is the PortType oneof itself: a blob and a structured value
 * are unrelated, and two structured values are compatible only when they name
 * the same schema id. Comparing schema ids rather than schema bodies is what
 * keeps the check cheap enough to run on every keystroke.
 */
export function typeCheck(pipeline: Pipeline | undefined): Diagnostic[] {
  if (pipeline === undefined) {
    return [];
  }

  const steps = new Map(pipeline.steps.map((step) => [step.id, step]));

  const diagnostics: Diagnostic[] = [];
  for (const edge of pipeline.edges) {
    const from = steps.get(edge.fromStep);
    if (from === undefined) {
      diagnostics.push({
        stepId: edge.fromStep,
        portName: edge.fromPort,
        message:
          `edge source step ${quote(edge.fromStep)} is not defined in ` +
          `pipeline ${quote(pipeline.id)}`,
      });
      continue;
    }
    const to = steps.get(edge.toStep);
    if (to === undefined) {
      diagnostics.push({
        stepId: edge.toStep,
        portName: edge.toPort,
        message:
          `edge target step ${quote(edge.toStep)} is not defined in ` +
          `pipeline ${quote(pipeline.id)}`,
      });
      continue;
    }

    const out = findPort(from.outputs, edge.fromPort);
    if (out === undefined) {
      diagnostics.push({
        stepId: from.id,
        portName: edge.fromPort,
        message: `step ${quote(from.id)} has no output port ${from.id}.${edge.fromPort}`,
      });
      continue;
    }
    const into = findPort(to.inputs, edge.toPort);
    if (into === undefined) {
      diagnostics.push({
        stepId: to.id,
        portName: edge.toPort,
        message: `step ${quote(to.id)} has no input port ${to.id}.${edge.toPort}`,
      });
      continue;
    }

    const why = incompatible(out.type, into.type);
    if (why !== "") {
      // The diagnostic is attached to the input port: that is the end the
      // author usually has to change, and it keeps one marker per edge even
      // when an output feeds several inputs.
      diagnostics.push({
        stepId: to.id,
        portName: into.name,
        message: `cannot connect ${from.id}.${out.name} to ${to.id}.${into.name}: ${why}`,
      });
    }
  }
  return diagnostics;
}

/**
 * refusalFor is the drop-time question: may this edge be added to this
 * pipeline? It answers with the message to show, or null to allow the
 * connection.
 *
 * It is defined AS typeCheck over the pipeline the drop would produce, rather
 * than as a second rule that happens to agree. There is no separate drop-time
 * check to keep in step.
 */
export function refusalFor(
  pipeline: Pipeline | undefined,
  edge: Edge,
): string | null {
  if (pipeline === undefined) {
    return null;
  }
  const [first] = typeCheck({ ...pipeline, edges: [edge] });
  return first?.message ?? null;
}

/** findPort returns the port with that name, or undefined. */
function findPort(ports: readonly Port[], name: string): Port | undefined {
  return ports.find((port) => port.name === name);
}

/**
 * incompatible returns why the two port types cannot be connected, or the
 * empty string when they can.
 */
function incompatible(
  from: PortType | undefined,
  to: PortType | undefined,
): string {
  const fromKind = describe(from);
  const toKind = describe(to);
  if (fromKind !== toKind) {
    return `${fromKind} is not ${toKind}`;
  }

  if (from?.kind.case === "structured" && to?.kind.case === "structured") {
    if (from.kind.value.schemaId !== to.kind.value.schemaId) {
      return (
        `schema ${quote(from.kind.value.schemaId)} is not ` +
        `schema ${quote(to.kind.value.schemaId)}`
      );
    }
    return "";
  }
  if (fromKind === untyped) {
    // Equal kinds, but an unfinished port has no type to agree on.
    return "both ports are untyped";
  }
  // Both blobs. Blobs are opaque; the media type is advisory and is
  // deliberately not type-checked.
  return "";
}

/**
 * describe names a port type's oneof arm for a message, including the case of
 * a port whose type was never set.
 *
 * The `never` in the default arm is deliberate: it is a compile-time alarm on
 * a PortType member added to the proto, which would otherwise arrive here as a
 * silently untyped port that connects to nothing and explains nothing.
 */
function describe(type: PortType | undefined): string {
  const kind = type?.kind;
  switch (kind?.case) {
    case "blob":
      return "blob";
    case "structured":
      return "structured";
    case undefined:
      return untyped;
    default: {
      const unhandled: never = kind;
      throw new Error(`unhandled port type ${JSON.stringify(unhandled)}`);
    }
  }
}
