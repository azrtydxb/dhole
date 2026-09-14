/**
 * The call a gate's decision becomes.
 *
 * It exists as its own function because the gate and the contract both insist
 * on a reason — the gate will not decide without one and DecideApproval
 * refuses a request that carries none — and the only place the sentence could
 * still be lost is the hop between them. `GateDecision` makes the reason part
 * of the decision's type, so a caller cannot build one without it.
 */
import type { Client } from "@connectrpc/connect";

import type { PipelineService } from "../../gen/dhole/v1/api_pb.js";

/** GateDecisionClient is the one RPC a decision needs. */
export type GateDecisionClient = Pick<
  Client<typeof PipelineService>,
  "decideApproval"
>;

/** GateDecision is what ApprovalGate's onDecide reports: the verdict and the
 * reason given for it, already trimmed by the gate. */
export type GateDecision = {
  readonly approved: boolean;
  readonly reason: string;
};

/** decideGate sends one decision about one gate, reason included. */
export function decideGate(
  client: GateDecisionClient,
  runId: string,
  stepId: string,
  decision: GateDecision,
) {
  return client.decideApproval({
    runId,
    stepId,
    approved: decision.approved,
    reason: decision.reason,
  });
}
