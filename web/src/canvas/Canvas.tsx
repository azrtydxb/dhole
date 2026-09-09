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
  Background,
  Controls,
  ReactFlow,
  type Connection,
  type Edge as FlowEdge,
  type Node as FlowNode,
} from "@xyflow/react";
import { useCallback, useMemo, useState } from "react";

import "@xyflow/react/dist/style.css";

import { pipelineClient } from "../api/client.js";
import type { Operation } from "../gen/dhole/v1/api_pb.js";
import {
  EdgeSchema,
  type Edge,
  type Pipeline,
  type Port,
} from "../gen/dhole/v1/pipeline_pb.js";
import { refusalFor } from "./edges.js";
import { autoLayout } from "./layout.js";
import { StepNode, type StepNodeType } from "./StepNode.js";

/** CanvasProps names the revision being edited. Both are required: an edit
 * without a base revision cannot conflict, and one that cannot conflict
 * overwrites somebody else's work silently. */
export interface CanvasProps {
  readonly pipelineId: string;
  readonly revisionId: string;
}

/** nodeTypes is module-level because React Flow re-mounts every node when the
 * object identity changes. */
const nodeTypes = { step: StepNode };

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
export function Canvas({ pipelineId, revisionId }: CanvasProps) {
  // head is the revision the next edit is based on. It moves with every
  // applied operation, because every operation produces a revision: there is
  // no separate save, and nothing here holds unsaved state.
  const [head, setHead] = useState(revisionId);
  const [edited, setEdited] = useState<Pipeline | undefined>(undefined);
  const [refusal, setRefusal] = useState<string | null>(null);

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
    },
    onError: (error: Error) => setRefusal(error.message),
  });

  const steps = pipeline?.steps ?? [];
  const edges = pipeline?.edges ?? [];

  // Derived on every render, from the DAG, and never stored anywhere.
  const nodes = useMemo<FlowNode[]>(() => {
    const drawn = pipeline?.steps ?? [];
    const positions = autoLayout(drawn, pipeline?.edges ?? []);
    return drawn.map((step): StepNodeType => {
      const at = positions.get(step.id) ?? { x: 0, y: 0 };
      return {
        id: step.id,
        type: "step",
        position: { x: at.x, y: at.y },
        data: { step },
      };
    });
  }, [pipeline]);

  const flowEdges = useMemo<FlowEdge[]>(
    () =>
      (pipeline?.edges ?? []).map((edge) => ({
        id: edgeKey(edge),
        source: edge.fromStep,
        sourceHandle: edge.fromPort,
        target: edge.toStep,
        targetHandle: edge.toPort,
      })),
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

  return (
    <div style={{ display: "flex", gap: 16, height: "90vh" }}>
      <section style={{ width: 300 }}>
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
          <button
            data-testid="set-property"
            type="button"
            onClick={setProperty}
          >
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

        {refusal !== null && (
          <p data-testid="edge-error" role="alert" style={{ color: "#c53030" }}>
            {refusal}
          </p>
        )}
      </section>

      <div style={{ flex: 1, border: "1px solid #cbd5e0" }}>
        <ReactFlow
          nodes={nodes}
          edges={flowEdges}
          nodeTypes={nodeTypes}
          onConnect={onConnect}
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
          <Background />
          <Controls showInteractive={false} />
        </ReactFlow>
      </div>
    </div>
  );
}
