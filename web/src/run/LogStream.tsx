/**
 * One step's log, live while it runs and authoritative once it does not.
 *
 * The component never decides which copy it is showing: the server announces
 * it with a `source` frame and this renders what it was told, in
 * `data-source`. That attribute is the visible half of the wire contract's
 * two-copy rule, and it is what an end-to-end test asserts the switch on.
 */
import { useEffect, useReducer } from "react";

import { getToken } from "../api/client.js";
import { applyLogFrame, emptyLog, logLines, type LogState } from "./logs.js";
import { subscribe, type SSEFrame } from "./sse.js";

export interface LogStreamProps {
  runId: string;
  stepId: string;
  /** Overrides the control-plane origin; the Vite dev proxy supplies it. */
  baseUrl?: string;
}

type LogAction = { kind: "frame"; frame: SSEFrame } | { kind: "reset" };

function reducer(state: LogState, action: LogAction): LogState {
  return action.kind === "reset"
    ? emptyLog()
    : applyLogFrame(state, action.frame);
}

export function LogStream({ runId, stepId, baseUrl }: LogStreamProps) {
  const [state, dispatch] = useReducer(reducer, undefined, emptyLog);

  useEffect(() => {
    const token = getToken();
    if (token === null) {
      return;
    }
    const controller = new AbortController();
    dispatch({ kind: "reset" });
    const origin = baseUrl ?? import.meta.env.VITE_DHOLE_API_URL ?? "";
    void subscribe({
      url: `${origin}/v1/runs/${runId}/steps/${stepId}/logs`,
      token,
      signal: controller.signal,
      onFrame: (frame) => dispatch({ kind: "frame", frame }),
    });
    return () => controller.abort();
  }, [runId, stepId, baseUrl]);

  const lines = logLines(state);
  return (
    <section
      data-testid="log-stream"
      data-source={state.source}
      data-step-id={stepId}
      data-ended={state.ended ? "true" : "false"}
    >
      <header data-testid="log-source">{state.source}</header>
      {state.dropped > 0 && (
        // A viewer that lost live chunks is TOLD. The authoritative copy that
        // arrives at the end is complete regardless, but a gap nobody
        // mentioned is a log quietly missing lines.
        <p data-testid="log-gap">
          {state.dropped} live chunks were dropped; the stored log will be
          complete
        </p>
      )}
      {state.source === "none" && state.reason !== "" && (
        <p data-testid="log-reason">{state.reason}</p>
      )}
      <pre>
        {lines.map((line, index) => (
          <span data-testid="log-line" key={`${String(index)}:${line}`}>
            {line}
            {"\n"}
          </span>
        ))}
      </pre>
    </section>
  );
}
