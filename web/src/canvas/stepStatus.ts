/**
 * What the canvas paints on a step, and where that comes from.
 *
 * Two different questions produce a badge, and conflating them is how a canvas
 * lies. `plan` answers "what WOULD happen if you ran this" — it is a dry run,
 * nothing has executed, and a step marked `HIT · cached` is a prediction. A
 * run answers "what DID happen". The same node can carry either, never both,
 * and the words differ so nobody reads a prediction as a result.
 */
import { EffectClass } from "../gen/dhole/v1/common_pb.js";
import type { Step } from "../gen/dhole/v1/pipeline_pb.js";
import type { StepStatus } from "./StepNode.js";

/** PlannedBadge is one step's line in a dry run. */
export type PlannedBadge = {
  readonly text: string;
  readonly token: string;
  /** Dimmed, because a cached step is one the run will not spend time on and
   * the eye should skip it to find the ones it will. */
  readonly dim: boolean;
};

/** planBadge turns a PlannedStep into the design's badge.
 *
 * The order matters. A step that cannot be retried is called out as a GATE
 * even when the planner would also have run it, because "this one cannot be
 * taken back" is the fact a person scanning a dry run most needs. */
export function planBadge(
  step: Step,
  planned: { cacheHit: boolean; engineKind: string } | undefined,
): PlannedBadge | undefined {
  if (planned === undefined) return undefined;
  if (planned.cacheHit) {
    return { text: "HIT · cached", token: "var(--ok)", dim: true };
  }
  if (step.effectClass === EffectClass.AT_MOST_ONCE) {
    return { text: "gate", token: "var(--err)", dim: false };
  }
  return {
    text: `exec · ${planned.engineKind === "" ? "any" : planned.engineKind}`,
    token: "var(--accent)",
    dim: false,
  };
}

/** statusOf maps a run's own word for a step onto what the node draws.
 *
 * The run model's vocabulary is the wire's — DISPATCHED, SUCCEEDED — and the
 * node's is the design's. Translating here rather than in the node keeps the
 * node ignorant of runs, which is what lets the same component draw a
 * definition nobody has executed. */
export function statusOf(node: { state: string; cached: boolean }): StepStatus {
  if (node.cached) return "cached";
  switch (node.state) {
    case "READY":
    case "DISPATCHED":
      return "queued";
    case "RUNNING":
    case "STEP_STARTED":
      return "running";
    case "SUCCEEDED":
      return "succeeded";
    case "FAILED":
      return "failed";
    case "STEP_AWAITING_REPLAY":
    case "STEP_BLOCKED":
      return "blocked";
    default:
      return "none";
  }
}
