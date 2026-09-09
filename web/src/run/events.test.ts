/**
 * What the run view shows, and the fact that it shows nothing the plane did
 * not say.
 *
 * These run in `make check`, unlike the Playwright suite, so they are where a
 * broken reduction is caught in seconds rather than minutes.
 */
import { describe, expect, it } from "vitest";

import {
  applyEvents,
  emptyRun,
  formatDuration,
  type RunEvent,
} from "./events.js";

const at = (seconds: number): number => seconds * 1_000_000_000;

function event(
  partial: Partial<RunEvent> & { type: string; sequence: number },
): RunEvent {
  return {
    runId: "run_1",
    atUnixNano: at(partial.sequence),
    ...partial,
  };
}

describe("the realised graph", () => {
  it("gives every node the duration the plane's own clock recorded", () => {
    const model = applyEvents(emptyRun("run_1"), [
      event({ type: "RUN_CREATED", sequence: 1 }),
      event({
        type: "STEP_DISPATCHED",
        sequence: 2,
        stepId: "build",
        atUnixNano: at(10),
      }),
      event({
        type: "STEP_SUCCEEDED",
        sequence: 3,
        stepId: "build",
        atUnixNano: at(13),
      }),
    ]);
    const build = model.nodes[0]!;
    expect(build.state).toEqual("SUCCEEDED");
    expect(build.durationMs).toEqual(3000);
    expect(formatDuration(build.durationMs)).toEqual("3.0s");
  });

  it("marks a step the plane served from the cache, and only that step", () => {
    const model = applyEvents(emptyRun("run_1"), [
      event({
        type: "STEP_DISPATCHED",
        sequence: 1,
        stepId: "build",
        payload: { cache_hit: true },
      }),
      event({
        type: "STEP_DISPATCHED",
        sequence: 2,
        stepId: "test",
        payload: {},
      }),
    ]);
    expect(model.nodes.find((n) => n.id === "build")?.cached).toBe(true);
    // A dispatch that says nothing about the cache is a step that RAN. A view
    // that defaulted to "cached" would report a cold run as a warm one.
    expect(model.nodes.find((n) => n.id === "test")?.cached).toBe(false);
  });

  it("shows the control plane's ineligibility reason verbatim", () => {
    // This string is internal/cache.Eligible's, reproduced here only as the
    // INPUT the server sent. The assertion is that it survives the reduction
    // unchanged — no rewording, no truncation — which is what keeps the view
    // from drifting away from the rule that produced it.
    const reason =
      "effect class is undeclared, so the step has not promised it is pure";
    const model = applyEvents(emptyRun("run_1"), [
      event({
        type: "STEP_DISPATCHED",
        sequence: 1,
        stepId: "notify",
        payload: { cacheable: false, cache_ineligible_reason: reason },
      }),
    ]);
    expect(model.nodes[0]!.cacheIneligibleReason).toEqual(reason);
  });

  it("folds a bounded loop into one container node holding its unrolled iterations", () => {
    const model = applyEvents(emptyRun("run_1"), [
      event({
        type: "LOOP_ITERATION_STARTED",
        sequence: 1,
        stepId: "retry",
        payload: { loop: "retry", iteration: 1, of: 3, step_id: "retry#1" },
      }),
      event({
        type: "LOOP_ITERATION_FINISHED",
        sequence: 2,
        stepId: "retry",
        payload: { loop: "retry", iteration: 1, of: 3, step_id: "retry#1" },
      }),
      event({
        type: "LOOP_ITERATION_STARTED",
        sequence: 3,
        stepId: "retry",
        payload: { loop: "retry", iteration: 2, of: 3, step_id: "retry#2" },
      }),
      event({
        type: "LOOP_EXITED",
        sequence: 4,
        stepId: "retry",
        payload: { loop: "retry", iteration: 2, of: 3 },
      }),
    ]);
    // ONE node on the graph, not one per pass.
    expect(model.nodes).toHaveLength(1);
    const loop = model.nodes[0]!;
    expect(loop.loop).toBe(true);
    expect(loop.state).toEqual("LOOP_EXITED");
    expect(loop.iterations.map((i) => i.stepId)).toEqual([
      "retry#1",
      "retry#2",
    ]);
    expect(loop.iterations[0]!.finished).toBe(true);
    expect(loop.iterations[1]!.finished).toBe(false);
  });

  it("tracks the last sequence, which is what a resume sends back", () => {
    const model = applyEvents(emptyRun("run_1"), [
      event({ type: "RUN_CREATED", sequence: 4 }),
      event({ type: "RUN_COMPLETED", sequence: 9 }),
    ]);
    expect(model.lastSequence).toEqual(9);
    expect(model.state).toEqual("RUN_COMPLETED");
  });
});

describe("formatDuration", () => {
  it("says nothing about a step that has not finished", () => {
    expect(formatDuration(undefined)).toEqual("");
  });

  it("keeps sub-second work in milliseconds", () => {
    expect(formatDuration(340)).toEqual("340ms");
  });
});
