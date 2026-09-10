/**
 * The realised graph of one run.
 *
 * REALISED, not declared: the nodes here are the steps the run actually
 * produced events for, in the order the plane recorded them, including the
 * unrolled iterations of a bounded loop. A view built from the pipeline
 * definition instead would show a step that never ran as though it had, and
 * would have nowhere to put an iteration the definition does not contain.
 *
 * Everything displayed is folded out of the event stream by ./events.ts —
 * durations, cache hits and the cache-ineligibility reason included. Nothing
 * is computed from the browser's clock and no reason string is retyped here.
 */
import { useEffect, useReducer, useState } from "react";

import { RealisedGenerator } from "../canvas/GeneratorNode.js";
import { getToken } from "../api/client.js";
import {
  applyEvent,
  emptyRun,
  formatDuration,
  runIsFinished,
  type RunEvent,
  type RunModel,
  type RunNode,
} from "./events.js";
import { LogStream } from "./LogStream.js";
import { subscribe } from "./sse.js";

export interface RunViewProps {
  runId: string;
  /** Overrides the control-plane origin; the Vite dev proxy supplies it. */
  baseUrl?: string;
}

type RunAction =
  { kind: "event"; event: RunEvent } | { kind: "reset"; runId: string };

function reducer(model: RunModel, action: RunAction): RunModel {
  return action.kind === "reset"
    ? emptyRun(action.runId)
    : applyEvent(model, action.event);
}

export function RunView({ runId, baseUrl }: RunViewProps) {
  const [model, dispatch] = useReducer(reducer, runId, emptyRun);
  const [selected, setSelected] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<Record<string, boolean>>({});

  useEffect(() => {
    const token = getToken();
    if (token === null) {
      return;
    }
    const controller = new AbortController();
    dispatch({ kind: "reset", runId });
    const origin = baseUrl ?? import.meta.env.VITE_DHOLE_API_URL ?? "";
    void subscribe({
      url: `${origin}/v1/runs/${runId}/events`,
      token,
      signal: controller.signal,
      onFrame: (frame) => {
        if (frame.event === "end" || frame.data === "") {
          return;
        }
        try {
          dispatch({
            kind: "event",
            event: JSON.parse(frame.data) as RunEvent,
          });
        } catch {
          // A frame this client cannot parse is dropped rather than taking
          // the whole stream down with it.
        }
      },
    });
    return () => controller.abort();
  }, [runId, baseUrl]);

  const step = selected ?? model.nodes[0]?.id ?? null;
  return (
    <section
      data-testid="run-view"
      data-run-id={runId}
      data-run-state={model.state}
    >
      <ol
        data-testid="run-graph"
        data-finished={runIsFinished(model) ? "true" : "false"}
      >
        {model.nodes.map((node) => (
          <NodeView
            key={node.id}
            node={node}
            expanded={expanded[node.id] === true}
            onToggle={() => {
              setExpanded((current) => ({
                ...current,
                [node.id]: current[node.id] !== true,
              }));
            }}
            onSelect={() => {
              setSelected(node.id);
            }}
          />
        ))}
      </ol>
      {step !== null && (
        <LogStream
          runId={runId}
          stepId={step}
          {...(baseUrl !== undefined && { baseUrl })}
        />
      )}
    </section>
  );
}

interface NodeViewProps {
  node: RunNode;
  expanded: boolean;
  onToggle: () => void;
  onSelect: () => void;
}

function NodeView({ node, expanded, onToggle, onSelect }: NodeViewProps) {
  return (
    <li
      data-testid={`run-node-${node.id}`}
      data-step-id={node.id}
      data-state={node.state}
      data-cached={node.cached ? "true" : "false"}
      {...(node.loop && { "data-container": "loop" })}
    >
      <button type="button" onClick={onSelect}>
        {node.id}
      </button>
      {/* A cached step took no time to run, and saying "0ms" would read as a
          suspiciously fast build rather than as work that never happened. */}
      <span data-testid="node-duration">
        {node.cached ? "cached" : formatDuration(node.durationMs)}
      </span>
      {node.cacheIneligibleReason !== "" && (
        // The control plane's own words, from internal/cache.Eligible.
        <span data-testid="node-cache-reason">
          {node.cacheIneligibleReason}
        </span>
      )}
      {/* A generator's realised steps. They are in the run log and nowhere
          else — the definition never contained them — so this is drawn from
          the record the plane wrote and from nothing this view computed. */}
      {node.realised !== undefined && (
        <RealisedGenerator record={node.realised} />
      )}
      {node.loop && (
        <>
          {/* Collapsed by default: a loop of fifty iterations must not be
              fifty nodes on the canvas before anyone asked for them. */}
          <button type="button" data-testid="expand-loop" onClick={onToggle}>
            {expanded
              ? "collapse"
              : `expand ${String(node.iterations.length)} iterations`}
          </button>
          {expanded && (
            <ol>
              {node.iterations.map((iteration) => (
                <li
                  key={iteration.stepId}
                  data-testid="loop-iteration"
                  data-step-id={iteration.stepId}
                  data-finished={iteration.finished ? "true" : "false"}
                >
                  {iteration.stepId}
                </li>
              ))}
            </ol>
          )}
        </>
      )}
    </li>
  );
}
