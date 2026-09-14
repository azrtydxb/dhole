// @vitest-environment jsdom
/**
 * The gate collects a reason and refuses to decide without one; the contract
 * requires one too. What sits between them is the call, and a call that drops
 * the sentence the gate made a person type records a decision with no reason
 * beside it — or, since the plane refuses that, a decision that never lands.
 */
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ApprovalGate } from "./ApprovalGate.js";
import { decideGate, type GateDecisionClient } from "./decide.js";

afterEach(cleanup);

function fakeClient() {
  const decideApproval = vi.fn<GateDecisionClient["decideApproval"]>(() =>
    Promise.resolve(
      {} as Awaited<ReturnType<GateDecisionClient["decideApproval"]>>,
    ),
  );
  return { decideApproval };
}

describe("decideGate", () => {
  for (const [button, approved] of [
    ["APPROVE", true],
    ["DENY", false],
  ] as const) {
    it(`passes the gate's reason through on ${button}`, () => {
      const client = fakeClient();
      render(
        <ApprovalGate
          run={{ id: "run_01HZ" }}
          step={{ id: "push", name: "push-registry" }}
          onDecide={(decidedApproved, reason) => {
            void decideGate(client, "run_01HZ", "push", {
              approved: decidedApproved,
              reason,
            });
          }}
          onClose={vi.fn()}
        />,
      );

      fireEvent.change(screen.getByLabelText(/reason/), {
        target: { value: "  the freeze ends on Monday  " },
      });
      fireEvent.click(screen.getByRole("button", { name: button }));

      expect(client.decideApproval).toHaveBeenCalledTimes(1);
      expect(client.decideApproval).toHaveBeenCalledWith({
        runId: "run_01HZ",
        stepId: "push",
        approved,
        reason: "the freeze ends on Monday",
      });
    });
  }
});
