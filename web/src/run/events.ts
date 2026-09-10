/**
 * The run, reduced from its event stream.
 *
 * This is a REDUCTION of the log and nothing else: no state is invented here
 * that the control plane did not record. A run is a state machine driven by a
 * persisted event log (ADR 0003), and a view that kept its own idea of what a
 * step is doing would disagree with the plane the moment a reconnection
 * replayed history in a different order.
 *
 * Everything a node displays therefore comes off an event:
 *
 *   - the duration is the distance between the dispatch and the terminal
 *     event, in the plane's own clock, never the browser's;
 *   - `cached` is the dispatch's own cache_hit flag, which is the ONLY place
 *     a cache hit is visible — the event is otherwise identical to a real
 *     dispatch, deliberately (internal/scheduler);
 *   - the ineligibility reason is the dispatch's cache_ineligible_reason
 *     VERBATIM, produced by internal/cache.Eligible. It is never rewritten or
 *     prettified here, because a reason the view paraphrases is a reason that
 *     silently drifts away from the rule that produced it.
 */

/** RunEvent is one frame of GET /v1/runs/{id}/events. */
export interface RunEvent {
  runId: string;
  stepId?: string;
  attempt?: number;
  sequence: number;
  type: string;
  atUnixNano: number;
  payload?: unknown;
}

/** LoopIteration is one unrolled pass of a bounded loop. */
export interface LoopIteration {
  /** The unrolled step id the plane recorded, e.g. `retry#2`. */
  stepId: string;
  iteration: number;
  finished: boolean;
}

/**
 * RealisedFragment is one GENERATOR_FRAGMENT_REALISED payload, as
 * internal/dynamic.Record writes it.
 *
 * `steps` is enough to draw the realised graph; `fragment` is the pipeline
 * itself on the wire, kept because it is what a replay rebuilds the graph from
 * and a view that threw it away would be holding a summary of the run rather
 * than the run.
 */
export interface RealisedFragment {
  readonly generator: string;
  readonly steps: readonly string[];
  readonly fragment: string;
}

/** parseRealisedRecord reads one realised record off an event, returning null
 * for anything that is not one. A frame this client cannot read is dropped
 * rather than taking the run view down with it. */
export function parseRealisedRecord(payload: unknown): RealisedFragment | null {
  if (typeof payload !== "object" || payload === null) {
    return null;
  }
  const record = payload as {
    generator?: unknown;
    steps?: unknown;
    fragment?: unknown;
  };
  if (typeof record.generator !== "string" || record.generator === "") {
    return null;
  }
  if (
    !Array.isArray(record.steps) ||
    !record.steps.every((id) => typeof id === "string")
  ) {
    return null;
  }
  return {
    generator: record.generator,
    steps: record.steps,
    fragment: typeof record.fragment === "string" ? record.fragment : "",
  };
}

/** RunNode is one node of the realised graph. */
export interface RunNode {
  id: string;
  state: string;
  startedAtNanos?: number;
  finishedAtNanos?: number;
  /** Milliseconds between dispatch and terminal event, once both are in. */
  durationMs?: number;
  /** True when the plane SERVED this step from the cache instead of running it. */
  cached: boolean;
  /** The plane's own words for why this step could not be cached, or "". */
  cacheIneligibleReason: string;
  /** A bounded loop is one container node with its iterations inside it. */
  loop: boolean;
  iterations: LoopIteration[];
  /** What a generator step emitted, once it has. It is undefined until the
   * plane records the fragment, because until then nobody — not this view,
   * not the definition, not the generator's author — knows what it will be. */
  realised?: RealisedFragment;
}

/** RunModel is the whole view state. */
export interface RunModel {
  runId: string;
  state: string;
  /** The highest event sequence applied, which is what a resume sends back. */
  lastSequence: number;
  nodes: RunNode[];
}

export const emptyRun = (runId: string): RunModel => ({
  runId,
  state: "PENDING",
  lastSequence: 0,
  nodes: [],
});

const loopEvents = new Set([
  "LOOP_ITERATION_STARTED",
  "LOOP_ITERATION_FINISHED",
  "LOOP_EXITED",
  "LOOP_CEILING_REACHED",
  "LOOP_FAILED",
]);

interface DispatchPayload {
  cache_hit?: boolean;
  cache_ineligible_reason?: string;
}

interface LoopPayload {
  step_id?: string;
  iteration?: number;
}

function newNode(id: string): RunNode {
  return {
    id,
    state: "PENDING",
    cached: false,
    cacheIneligibleReason: "",
    loop: false,
    iterations: [],
  };
}

/** applyEvent folds one event into the model, returning a new one. */
export function applyEvent(model: RunModel, event: RunEvent): RunModel {
  const next: RunModel = {
    ...model,
    lastSequence: Math.max(model.lastSequence, event.sequence),
    nodes: model.nodes.map((n) => ({ ...n, iterations: [...n.iterations] })),
  };

  if (event.type === "RUN_CREATED") {
    next.state = "RUNNING";
    return next;
  }
  if (event.type === "RUN_COMPLETED" || event.type === "RUN_FAILED") {
    next.state = event.type;
    return next;
  }

  const stepId = event.stepId ?? "";
  if (stepId === "") {
    return next;
  }
  let node = next.nodes.find((n) => n.id === stepId);
  if (node === undefined) {
    node = newNode(stepId);
    next.nodes.push(node);
  }

  // A generator is the one step whose subgraph the DEFINITION does not
  // contain: it decided what to run at runtime. The fragment it emitted is in
  // the log and nowhere else (ADR 0003), so this is where the run view learns
  // the realised steps — and it reads them rather than recomputing anything.
  if (event.type === "GENERATOR_FRAGMENT_REALISED") {
    const record = parseRealisedRecord(event.payload);
    if (record !== null) {
      node.realised = record;
    }
    return next;
  }

  if (loopEvents.has(event.type)) {
    node.loop = true;
    const payload = (event.payload ?? {}) as LoopPayload;
    const iterationStep = payload.step_id ?? "";
    if (iterationStep !== "") {
      const existing = node.iterations.find((i) => i.stepId === iterationStep);
      if (existing === undefined) {
        node.iterations.push({
          stepId: iterationStep,
          iteration: payload.iteration ?? node.iterations.length + 1,
          finished: event.type === "LOOP_ITERATION_FINISHED",
        });
      } else if (event.type === "LOOP_ITERATION_FINISHED") {
        existing.finished = true;
      }
    }
    if (
      event.type !== "LOOP_ITERATION_STARTED" &&
      event.type !== "LOOP_ITERATION_FINISHED"
    ) {
      node.state = event.type;
    }
    return next;
  }

  switch (event.type) {
    case "STEP_READY":
      node.state = "READY";
      break;
    case "STEP_DISPATCHED": {
      const payload = (event.payload ?? {}) as DispatchPayload;
      node.state = "DISPATCHED";
      node.startedAtNanos = event.atUnixNano;
      // The dispatch is the only place either of these is visible.
      node.cached = payload.cache_hit === true;
      node.cacheIneligibleReason = payload.cache_ineligible_reason ?? "";
      break;
    }
    case "STEP_SUCCEEDED":
    case "STEP_FAILED":
      node.state = event.type === "STEP_SUCCEEDED" ? "SUCCEEDED" : "FAILED";
      node.finishedAtNanos = event.atUnixNano;
      if (node.startedAtNanos !== undefined) {
        node.durationMs = (event.atUnixNano - node.startedAtNanos) / 1e6;
      }
      break;
    default:
      node.state = event.type;
      break;
  }
  return next;
}

/** applyEvents folds a whole batch, oldest first. */
export function applyEvents(model: RunModel, events: RunEvent[]): RunModel {
  return events.reduce(applyEvent, model);
}

/** formatDuration is how long a step took, for a person. */
export function formatDuration(ms: number | undefined): string {
  if (ms === undefined) {
    return "";
  }
  if (ms < 1000) {
    return `${Math.max(0, Math.round(ms))}ms`;
  }
  return `${(ms / 1000).toFixed(1)}s`;
}

/** runIsFinished says whether the plane has closed this run. */
export function runIsFinished(model: RunModel): boolean {
  return model.state === "RUN_COMPLETED" || model.state === "RUN_FAILED";
}
