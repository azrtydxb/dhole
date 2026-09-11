/**
 * One run's live state, for anything that wants to draw it.
 *
 * The run view already builds this model; the EDITOR needs the same thing, so
 * the subscription lives here rather than being written a second time. Two
 * copies of "apply the event stream to a model" is two places for a step to be
 * shown as still running after it has finished.
 *
 * The run id is optional because the editor usually has none: a definition
 * being edited is not a run, and this hook subscribing to nothing is the
 * normal case rather than an error.
 */
import { useEffect, useReducer } from "react";

import { getToken } from "../api/client.js";
import {
  applyEvent,
  emptyRun,
  type RunEvent,
  type RunModel,
} from "./events.js";
import { subscribe } from "./sse.js";

type RunAction =
  { kind: "event"; event: RunEvent } | { kind: "reset"; runId: string };

function reducer(model: RunModel, action: RunAction): RunModel {
  return action.kind === "reset"
    ? emptyRun(action.runId)
    : applyEvent(model, action.event);
}

/** useRunModel follows one run, or nothing when `runId` is null. */
export function useRunModel(runId: string | null): RunModel {
  const [model, dispatch] = useReducer(reducer, runId ?? "", emptyRun);

  useEffect(() => {
    const token = getToken();
    if (runId === null || runId === "" || token === null) return;

    const controller = new AbortController();
    dispatch({ kind: "reset", runId });
    const origin = import.meta.env.VITE_DHOLE_API_URL ?? "";
    void subscribe({
      url: `${origin}/v1/runs/${runId}/events`,
      token,
      signal: controller.signal,
      onFrame: (frame) => {
        if (frame.event === "end" || frame.data === "") return;
        try {
          dispatch({
            kind: "event",
            event: JSON.parse(frame.data) as RunEvent,
          });
        } catch {
          // A frame this client cannot parse is dropped rather than taking the
          // whole stream down with it.
        }
      },
    });
    return () => controller.abort();
  }, [runId]);

  return model;
}
