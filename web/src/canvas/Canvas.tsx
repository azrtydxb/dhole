/**
 * The canvas: a pipeline drawn from its definition, edited one operation at a
 * time.
 *
 * Two rules govern everything below, and both are architecture rather than
 * taste.
 *
 * NO DOCUMENT EVER LEAVES HERE. Every edit is a single dhole.v1.Operation sent
 * to ApplyOperation, which answers with the new revision, the diff and the
 * operation that undoes it (ADR 0013, ADR 0020). A canvas that PUT a whole
 * pipeline could not merge two people editing at once and could not undo
 * anything the API had not already done, so there is no code path here that
 * assembles a Pipeline to send - the only Pipeline this component holds is the
 * one the server last handed back.
 *
 * NO POSITION EVER LEAVES HERE EITHER. Node coordinates come from
 * layout.autoLayout on every render and are never written into the document.
 * Nodes are not draggable for exactly that reason: there is nowhere to put a
 * dragged position, and a position that survived only until the next render
 * would be a worse lie than not offering the gesture at all.
 *
 * The third rule is ADR 0001's: a wire is type-checked at DROP TIME, in the
 * browser, by edges.ts - the mirror of internal/dag.TypeCheck - so a bad
 * connection is refused where it is made rather than at minute eight of a run.
 */
import { create } from "@bufbuild/protobuf";
import { useMutation, useQuery } from "@tanstack/react-query";
import {
  Controls,
  ReactFlow,
  type Connection,
  type Edge as FlowEdge,
  type Node as FlowNode,
} from "@xyflow/react";
import { useCallback, useMemo, useState } from "react";

import "@xyflow/react/dist/style.css";

import { pipelineClient } from "../api/client.js";
import type { Operation, Revision } from "../gen/dhole/v1/api_pb.js";
import {
  EdgeSchema,
  type Edge,
  type Pipeline,
  type Port,
} from "../gen/dhole/v1/pipeline_pb.js";
import { refusalFor } from "./edges.js";
import {
  GeneratorNode,
  isGenerator,
  type GeneratorNodeType,
} from "./GeneratorNode.js";
import { Presence, RebasePrompt, rebaseNotice } from "./Presence.js";
import { autoLayout } from "./layout.js";
import { asPortTypeName, portTypeColour } from "../design/PortGlyph.js";
import { createPortal } from "react-dom";

import {
  CanvasGrid,
  CanvasMiniMap,
  HintBar,
  ZoomControl,
} from "./CanvasChrome.js";
import type { Theme } from "../design/useTheme.js";
import { planBadge } from "./stepStatus.js";
import {
  StepNode,
  type StepStatus,
  nodeChrome,
  nodeWidth,
  portSpacing,
  type StepNodeType,
} from "./StepNode.js";

/** CanvasProps names the revision being edited. Both are required: an edit
 * without a base revision cannot conflict, and one that cannot conflict
 * overwrites somebody else's work silently. */
export interface CanvasProps {
  readonly pipelineId: string;
  readonly revisionId: string;
  /** Where this canvas is being drawn.
   *
   * `standalone` keeps its own side panel — the operation controls and the
   * conflict notice — and is what the component's own tests drive. `embedded`
   * draws only the graph, because the editor shell around it already provides
   * a catalog on the left and an inspector on the right, and two panels
   * offering the same thing is how a user learns to trust neither. */
  readonly variant?: "standalone" | "embedded";
  /** Told which step the user selected, so the shell's inspector can follow
   * the canvas. Selection lives in React Flow; this is how it gets out. */
  readonly onSelect?: (stepId: string | null) => void;
  /** What a dry run said, keyed by step id. Empty when no plan has been asked
   * for — the canvas draws a definition, and a stale plan is worse than none. */
  readonly planned?: ReadonlyMap<
    string,
    { cacheHit: boolean; engineKind: string }
  >;
  /** What a run says about each step, keyed by step id. */
  readonly runStatus?: ReadonlyMap<string, StepStatus>;
  /** The active theme, passed down rather than read here, because the grid and
   * the minimap need RESOLVED colours and resolving depends on which theme is
   * on the document. */
  readonly theme?: Theme;
}

/** nodeTypes is module-level because React Flow re-mounts every node when the
 * object identity changes.
 *
 * A generator is drawn by its own node rather than as an ordinary step: the
 * authored graph cannot contain the steps it will emit — they are decided at
 * runtime — so it is drawn OPAQUE, saying so, instead of as a box that looks
 * like everything else and hides that it expands. */
const nodeTypes = { step: StepNode, generator: GeneratorNode };

/** The schema id the structured ports in the palette carry. A canvas cannot
 * invent types; these stand in until Task 47 reads them off a plugin. */
const reportSchema = "https://dhole.dev/schemas/report.json";

/** A palette entry is a set of declared ports, because a step IS its ports. */
interface StepKind {
  readonly label: string;
  readonly inputs: readonly Port[];
  readonly outputs: readonly Port[];
}

function blobPort(name: string): Port {
  return {
    $typeName: "dhole.v1.Port",
    name,
    type: {
      $typeName: "dhole.v1.PortType",
      kind: {
        case: "blob",
        value: { $typeName: "dhole.v1.BlobType", mediaType: "" },
      },
    },
  };
}

function structuredPort(name: string): Port {
  return {
    $typeName: "dhole.v1.Port",
    name,
    type: {
      $typeName: "dhole.v1.PortType",
      kind: {
        case: "structured",
        value: {
          $typeName: "dhole.v1.StructType",
          schemaId: reportSchema,
          schema: "",
        },
      },
    },
  };
}

/** The palette. "no-ports" is in it deliberately: a step with no declared
 * ports is legal, and the canvas has to draw it rather than assume every node
 * has something to wire. */
const stepKinds: Record<string, StepKind> = {
  "blob-source": {
    label: "blob source (out: blob)",
    inputs: [],
    outputs: [blobPort("out")],
  },
  "blob-sink": {
    label: "blob sink (in: blob)",
    inputs: [blobPort("in")],
    outputs: [],
  },
  "structured-source": {
    label: "structured source (out: report)",
    inputs: [],
    outputs: [structuredPort("out")],
  },
  "structured-sink": {
    label: "structured sink (in: report)",
    inputs: [structuredPort("in")],
    outputs: [],
  },
  "no-ports": { label: "no ports", inputs: [], outputs: [] },
};

/** The properties set_property accepts, from api.proto's own list. */
const properties = ["plugin_ref", "effect_class", "lease_scope"];

/** edgeKey names an edge the way the DOM and React Flow both need it. */
function edgeKey(edge: Edge): string {
  return `${edge.fromStep}-${edge.fromPort}-${edge.toStep}-${edge.toPort}`;
}

/** Canvas draws one revision of one pipeline and edits it in place. */
export function Canvas({
  pipelineId,
  revisionId,
  variant = "standalone",
  onSelect,
  planned,
  runStatus,
  theme = "dark",
}: CanvasProps) {
  // head is the revision the next edit is based on. It moves with every
  // applied operation, because every operation produces a revision: there is
  // no separate save, and nothing here holds unsaved state.
  const [head, setHead] = useState(revisionId);
  const [edited, setEdited] = useState<Pipeline | undefined>(undefined);
  const [refusal, setRefusal] = useState<string | null>(null);
  // The revision somebody else moved this pipeline to while this editor was
  // making a change. Holding it is NOT a queued retry: the refused operation
  // is gone, and rebasing is a read (Presence.tsx).
  const [movedTo, setMovedTo] = useState<Revision | null>(null);

  const [newStepId, setNewStepId] = useState("");
  const [newStepKind, setNewStepKind] = useState("blob-source");
  const [propertyStep, setPropertyStep] = useState("");
  const [propertyName, setPropertyName] = useState("plugin_ref");
  const [propertyValue, setPropertyValue] = useState("");

  const loaded = useQuery({
    queryKey: ["pipeline", pipelineId, revisionId],
    queryFn: () => pipelineClient.getPipeline({ pipelineId, revisionId }),
  });

  const pipeline = edited ?? loaded.data?.pipeline;

  const apply = useMutation({
    mutationFn: (operation: Operation) =>
      pipelineClient.applyOperation({
        pipelineId,
        baseRevision: head,
        operation,
      }),
    onSuccess: (response) => {
      // The server's answer replaces what is on screen. The canvas never
      // computes the next document itself: doing so is how a client's idea of
      // a pipeline starts to differ from the revision that will actually run.
      if (response.pipeline !== undefined) {
        setEdited(response.pipeline);
      }
      if (response.revision !== undefined) {
        setHead(response.revision.id);
      }
      setRefusal(null);
      setMovedTo(null);
    },
    onError: (error: Error) => {
      // A conflict is not a failure to be retried. The edit did not happen,
      // and re-sending it against the new head would overwrite the change
      // that beat it to the head — silently, and looking identical on screen.
      const moved = rebaseNotice(error);
      if (moved !== null) {
        setMovedTo(moved);
        return;
      }
      setRefusal(error.message);
    },
  });

  const steps = pipeline?.steps ?? [];
  const edges = pipeline?.edges ?? [];

  // Derived on every render, from the DAG, and never stored anywhere.
  const nodes = useMemo<FlowNode[]>(() => {
    const drawn = pipeline?.steps ?? [];
    const positions = autoLayout(drawn, pipeline?.edges ?? []);
    return drawn.map((step): StepNodeType | GeneratorNodeType => {
      const at = positions.get(step.id) ?? { x: 0, y: 0 };
      // Declared width and height, not only measured ones. The MINIMAP
      // projects nodes before React Flow has measured them and draws nothing
      // for a node whose size it does not know yet — which is why the map was
      // an empty rectangle. The height is the node's real geometry: a title
      // row, one row per declared port, and the footer.
      const size = {
        width: nodeWidth,
        height:
          nodeChrome + (step.inputs.length + step.outputs.length) * portSpacing,
      };
      // One comparison, in one place (isGenerator), so the editor and the run
      // view cannot disagree about what a generator is.
      // A run's verdict wins over a plan's prediction: once something has
      // actually happened, what would have happened is no longer interesting.
      const status = runStatus?.get(step.id);
      const badge =
        status === undefined
          ? planBadge(step, planned?.get(step.id))
          : undefined;
      const extra = {
        ...(status === undefined ? {} : { status }),
        ...(badge === undefined
          ? {}
          : { badge: { text: badge.text, token: badge.token } }),
      };
      return isGenerator(step)
        ? {
            id: step.id,
            type: "generator",
            position: { x: at.x, y: at.y },
            ...size,
            data: { step },
          }
        : {
            id: step.id,
            type: "step",
            position: { x: at.x, y: at.y },
            ...size,
            data: { step, ...extra },
          };
    });
  }, [pipeline, planned, runStatus]);

  const flowEdges = useMemo<FlowEdge[]>(
    () =>
      (pipeline?.edges ?? []).map((edge) => {
        // A wire takes its colour from the port it LEAVES, so the line says
        // what travels along it rather than only where it goes. A grey graph
        // makes the reader open both ends to learn what a connection carries.
        const from = (pipeline?.steps ?? []).find(
          (step) => step.id === edge.fromStep,
        );
        const port = from?.outputs.find((out) => out.name === edge.fromPort);
        const type =
          port?.type?.kind.case === "structured"
            ? port.type.kind.value.schemaId
            : port?.type?.kind.case === "blob"
              ? "blob"
              : undefined;
        return {
          id: edgeKey(edge),
          source: edge.fromStep,
          sourceHandle: edge.fromPort,
          target: edge.toStep,
          targetHandle: edge.toPort,
          // Orthogonal with square corners, which is what the design draws:
          // `M x1 y1 L mx y1 L mx y2 L x2 y2`. React Flow's default is a
          // bezier, and a canvas of curves reads as a mind map rather than as
          // a wiring diagram — the corners are what make a fan of eight edges
          // followable.
          type: "step",
          style: {
            stroke: portTypeColour(asPortTypeName(type)),
            strokeWidth: 1.5,
            opacity: 0.65,
          },
        };
      }),
    [pipeline],
  );

  const onConnect = useCallback(
    (connection: Connection) => {
      const edge = create(EdgeSchema, {
        fromStep: connection.source,
        fromPort: connection.sourceHandle ?? "",
        toStep: connection.target,
        toPort: connection.targetHandle ?? "",
      });

      // The refusal happens HERE, before anything is sent. A canvas that
      // showed this message and applied the operation anyway would look
      // identical on screen and would have thrown the type system away.
      const why = refusalFor(pipeline, edge);
      if (why !== null) {
        setRefusal(why);
        return;
      }
      apply.mutate({
        $typeName: "dhole.v1.Operation",
        kind: {
          case: "connect",
          value: { $typeName: "dhole.v1.Connect", edge },
        },
      });
    },
    [apply, pipeline],
  );

  const addStep = useCallback(() => {
    const kind = stepKinds[newStepKind];
    if (newStepId === "" || kind === undefined) {
      return;
    }
    apply.mutate({
      $typeName: "dhole.v1.Operation",
      kind: {
        case: "addStep",
        value: {
          $typeName: "dhole.v1.AddStep",
          step: {
            $typeName: "dhole.v1.Step",
            id: newStepId,
            name: "",
            pluginRef: "",
            effectClass: 0,
            leaseScope: 0,
            capabilities: [],
            // No plugin values yet: a new step names no plugin, so there is
            // nothing it declares to configure.
            config: {},
            // Nor an image or an engine type: both are step-level overrides,
            // and empty means "whatever the tier this lands on runs" rather
            // than a choice this canvas made on the author's behalf.
            image: "",
            engineType: "",
            // Nor a timeout: zero means unbounded, which is what every
            // pipeline written before the field existed carries, and a number
            // this canvas invented would be a limit nobody chose.
            timeoutSeconds: 0,
            // Nor a file: a step reads a file the DEFINITION carries by
            // binding a port to it (ADR 0023), and a new step has no port a
            // binding could name yet. Attaching one is `push-file` followed by
            // a set_file operation, which is a deliberate act and not a
            // default this canvas invents.
            fileInputs: [],
            inputs: [...kind.inputs],
            outputs: [...kind.outputs],
          },
        },
      },
    });
  }, [apply, newStepId, newStepKind]);

  const setProperty = useCallback(() => {
    if (propertyStep === "") {
      return;
    }
    apply.mutate({
      $typeName: "dhole.v1.Operation",
      kind: {
        case: "setProperty",
        value: {
          $typeName: "dhole.v1.SetProperty",
          stepId: propertyStep,
          property: propertyName,
          value: propertyValue,
        },
      },
    });
  }, [apply, propertyStep, propertyName, propertyValue]);

  if (loaded.isError) {
    return (
      <p role="alert">
        could not load {pipelineId}: {loaded.error.message}
      </p>
    );
  }

  // The authoring controls — add a step, set a property — as one block, so
  // both layouts show the SAME controls rather than two that drift. In the
  // editor shell they are portalled into the left rail, which is where the
  // design puts "add something to this pipeline"; standalone they stay beside
  // the graph.
  const authoringHost =
    globalThis.document?.getElementById("dh-authoring") ?? null;

  const authoring = (
    <section className="dh-authoring" style={{ width: 300 }}>
      <h2>{pipelineId}</h2>
      <p>
        saved as <code data-testid="revision-id">{head}</code>
      </p>
      <p style={{ color: "#718096" }}>
        Every edit is one operation; there is nothing to save separately, and
        nothing on this screen holds a position the pipeline will remember.
      </p>

      <fieldset>
        <legend>add a step</legend>
        <input
          data-testid="step-id"
          aria-label="step id"
          value={newStepId}
          onChange={(event) => setNewStepId(event.target.value)}
        />
        <select
          data-testid="step-kind"
          aria-label="step kind"
          value={newStepKind}
          onChange={(event) => setNewStepKind(event.target.value)}
        >
          {Object.entries(stepKinds).map(([value, kind]) => (
            <option key={value} value={value}>
              {kind.label}
            </option>
          ))}
        </select>
        <button data-testid="add-step" type="button" onClick={addStep}>
          add step
        </button>
      </fieldset>

      <fieldset>
        <legend>set a property</legend>
        <select
          data-testid="property-step"
          aria-label="step"
          value={propertyStep}
          onChange={(event) => setPropertyStep(event.target.value)}
        >
          <option value="">choose a step</option>
          {steps.map((step) => (
            <option key={step.id} value={step.id}>
              {step.id}
            </option>
          ))}
        </select>
        <select
          data-testid="property-name"
          aria-label="property"
          value={propertyName}
          onChange={(event) => setPropertyName(event.target.value)}
        >
          {properties.map((name) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </select>
        <input
          data-testid="property-value"
          aria-label="value"
          value={propertyValue}
          onChange={(event) => setPropertyValue(event.target.value)}
        />
        <button data-testid="set-property" type="button" onClick={setProperty}>
          set property
        </button>
      </fieldset>

      <h3>wires</h3>
      <ul data-testid="edge-list">
        {edges.map((edge) => (
          <li key={edgeKey(edge)} data-testid={`edge-${edgeKey(edge)}`}>
            {edge.fromStep}.{edge.fromPort} to {edge.toStep}.{edge.toPort}
          </li>
        ))}
      </ul>

      <Presence pipelineId={pipelineId} selection={propertyStep} />

      {movedTo !== null && (
        <RebasePrompt
          revision={movedTo}
          onRebase={(revisionId) => {
            // A read, and only a read. The canvas moves onto the revision
            // the pipeline is actually at and shows it; whether the refused
            // edit is worth making again is its author's decision.
            void pipelineClient
              .getPipeline({ pipelineId, revisionId })
              .then((response) => {
                if (response.pipeline !== undefined) {
                  setEdited(response.pipeline);
                }
                setHead(response.revision?.id ?? revisionId);
                setMovedTo(null);
              })
              .catch((error: Error) => setRefusal(error.message));
          }}
        />
      )}

      {refusal !== null && (
        <p data-testid="edge-error" role="alert" style={{ color: "#c53030" }}>
          {refusal}
        </p>
      )}
    </section>
  );

  if (variant === "embedded") {
    // Read on render rather than held in state: the host is rendered by the
    // shell in the same commit, and a ref would be null on the first pass.
    return (
      <div style={{ position: "absolute", inset: 0 }}>
        <ReactFlow
          nodes={nodes}
          edges={flowEdges}
          nodeTypes={nodeTypes}
          onConnect={onConnect}
          onSelectionChange={({ nodes: picked }) => {
            onSelect?.(picked[0]?.id ?? null);
          }}
          nodesDraggable={false}
          nodesConnectable
          fitView
          fitViewOptions={{ maxZoom: 1, padding: 0.12 }}
          proOptions={{ hideAttribution: false }}
        >
          <CanvasGrid theme={theme} />
          <ZoomControl />
          <CanvasMiniMap theme={theme} />
        </ReactFlow>
        <HintBar />
        {/* The authoring controls belong in the left rail, which this
            component does not own. A portal puts them there without lifting
            the whole edit state out of this file: the controls and the
            mutations they drive stay together, which is what keeps a refused
            operation reported next to the button that caused it. */}
        {authoringHost !== null && createPortal(authoring, authoringHost)}
        {refusal !== null && (
          <p
            data-testid="edge-error"
            role="alert"
            style={{
              position: "absolute",
              top: 12,
              left: "50%",
              transform: "translateX(-50%)",
              margin: 0,
              padding: "7px 14px",
              fontSize: 10,
              color: "var(--err)",
              background: "var(--panel)",
              border: "1px solid var(--err)",
              borderRadius: 5,
            }}
          >
            {refusal}
          </p>
        )}
      </div>
    );
  }

  return (
    <div style={{ display: "flex", gap: 16, height: "90vh" }}>
      {authoring}

      <div style={{ flex: 1, border: "1px solid var(--line2)" }}>
        <ReactFlow
          nodes={nodes}
          edges={flowEdges}
          nodeTypes={nodeTypes}
          onConnect={onConnect}
          onSelectionChange={({ nodes: picked }) => {
            onSelect?.(picked[0]?.id ?? null);
          }}
          // Not draggable, and this is the point rather than an omission: a
          // position has nowhere to be stored, so offering the gesture would
          // promise something the document cannot keep.
          nodesDraggable={false}
          nodesConnectable
          fitView
          // Never magnified: fitView on its own scales a two-node pipeline up
          // until the ports are the size of buttons and the graph runs off the
          // pane, which is neither readable nor droppable.
          fitViewOptions={{ maxZoom: 1, padding: 0.1 }}
        >
          <CanvasGrid theme={theme} />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>
    </div>
  );
}
