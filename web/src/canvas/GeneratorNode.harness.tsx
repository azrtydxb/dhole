/**
 * The module that mounts a generator node on the running application page.
 *
 * It exists for the same reason src/panel/harness.tsx does: the application
 * shell has one screen, the canvas, and the screen that puts an editor and a
 * run view side by side belongs to a later task. This is how the two
 * renderings of a generator are opened against a REAL control plane in the
 * meantime — the same providers main.tsx installs, the same API client, and no
 * behaviour of its own. It goes away the day Canvas routes generator steps to
 * GeneratorNode and RunView draws realised fragments, and nothing but the e2e
 * suite loads it.
 *
 * The authored half comes from the plane: GetPipeline, then the step the query
 * string names. The realised half comes from a recorded run-log payload passed
 * in the query string, because no generator step runs on the plane yet — the
 * step type is Task 55 and the scheduler that dispatches one is not wired. The
 * payload is not invented here: internal/dynamic writes it, and its own tests
 * fail if the file the suite passes stops being a record it can produce.
 */
import {
  QueryClient,
  QueryClientProvider,
  useQuery,
} from "@tanstack/react-query";
import { ReactFlow, type Node as FlowNode } from "@xyflow/react";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import "@xyflow/react/dist/style.css";

import { pipelineClient } from "../api/client.js";
import {
  GeneratorNode,
  parseRealisedRecord,
  RealisedGenerator,
  type GeneratorNodeType,
} from "./GeneratorNode.js";

const queryClient = new QueryClient();
const parameters = new URLSearchParams(globalThis.location?.search ?? "");

const pipelineId = parameters.get("generator.pipeline") ?? "";
const revisionId = parameters.get("generator.revision") ?? "";
const stepId = parameters.get("generator.step") ?? "";
const record = parseRealisedRecord(parameters.get("generator.record") ?? "");

const nodeTypes = { generator: GeneratorNode };

function Harness() {
  const loaded = useQuery({
    queryKey: ["generator", pipelineId, revisionId],
    queryFn: () => pipelineClient.getPipeline({ pipelineId, revisionId }),
  });

  const step = loaded.data?.pipeline?.steps.find((s) => s.id === stepId);
  const nodes: FlowNode[] =
    step === undefined
      ? []
      : [
          {
            id: step.id,
            type: "generator",
            position: { x: 0, y: 0 },
            data: { step },
          } satisfies GeneratorNodeType,
        ];

  return (
    <div>
      <div style={{ height: 320, border: "1px solid #cbd5e0" }}>
        <ReactFlow
          nodes={nodes}
          edges={[]}
          nodeTypes={nodeTypes}
          nodesDraggable={false}
          fitView
          fitViewOptions={{ maxZoom: 1, padding: 0.2 }}
        />
      </div>
      {record !== null && <RealisedGenerator record={record} />}
    </div>
  );
}

const existing = document.getElementById("generator-root");
const container = existing ?? document.createElement("div");
container.id = "generator-root";
if (existing === null) {
  document.body.append(container);
}

createRoot(container).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <Harness />
    </QueryClientProvider>
  </StrictMode>,
);
