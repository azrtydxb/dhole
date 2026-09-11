// @vitest-environment jsdom
/**
 * The gate is the one surface here that releases an at-most-once effect, so
 * these tests are about the two ways it could go wrong quietly: a decision
 * recorded with no reason beside it, and a denial that is harder to reach than
 * an approval.
 */
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { ApprovalGate } from "./ApprovalGate.js";

afterEach(cleanup);

const run = {
  id: "run_01HZ",
  label: "#129",
  pipelineName: "ci",
  revisionId: "r47",
};
const step = {
  id: "push",
  name: "push-registry",
  effect: "at-most-once",
  engine: "containerd@gpu-01",
  approvers: "platform-team",
  timeout: "72h",
  summary: "pushes ghcr.io/acme/api:sha-8f31d2",
};

describe("ApprovalGate", () => {
  it("refuses to decide until the decision is given a reason", () => {
    const onDecide = vi.fn();
    render(
      <ApprovalGate
        run={run}
        step={step}
        onDecide={onDecide}
        onClose={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "APPROVE" }));
    fireEvent.click(screen.getByRole("button", { name: "DENY" }));
    expect(onDecide).not.toHaveBeenCalled();
    expect(screen.getByTestId("gate-hint").textContent).toContain(
      "needs a reason",
    );

    // Whitespace is not a reason.
    fireEvent.change(screen.getByLabelText(/reason/), {
      target: { value: "   " },
    });
    fireEvent.click(screen.getByRole("button", { name: "APPROVE" }));
    expect(onDecide).not.toHaveBeenCalled();
  });

  it("reports the decision and the trimmed reason", () => {
    const onDecide = vi.fn();
    render(
      <ApprovalGate
        run={run}
        step={step}
        onDecide={onDecide}
        onClose={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByLabelText(/reason/), {
      target: { value: "  scan is clean, release approved  " },
    });
    fireEvent.click(screen.getByRole("button", { name: "APPROVE" }));
    expect(onDecide).toHaveBeenCalledWith(
      true,
      "scan is clean, release approved",
    );
  });

  it("denies on the same terms as it approves — one click, same reason", () => {
    const onDecide = vi.fn();
    render(
      <ApprovalGate
        run={run}
        step={step}
        onDecide={onDecide}
        onClose={vi.fn()}
      />,
    );

    fireEvent.change(screen.getByLabelText(/reason/), {
      target: { value: "critical CVE in the image" },
    });
    fireEvent.click(screen.getByRole("button", { name: "DENY" }));
    expect(onDecide).toHaveBeenCalledTimes(1);
    expect(onDecide).toHaveBeenCalledWith(false, "critical CVE in the image");
  });

  it("names the step, the run and the effect class it is about", () => {
    render(
      <ApprovalGate
        run={run}
        step={step}
        onDecide={vi.fn()}
        onClose={vi.fn()}
      />,
    );

    const subject = screen.getByTestId("gate-subject");
    expect(subject.textContent).toContain("push-registry");
    expect(subject.textContent).toContain("at-most-once");
    expect(subject.textContent).toContain("run_01HZ");
    expect(subject.textContent).toContain("ci · rev r47");
  });

  it("closes on Escape without deciding anything", () => {
    const onClose = vi.fn();
    const onDecide = vi.fn();
    render(
      <ApprovalGate
        run={run}
        step={step}
        onDecide={onDecide}
        onClose={onClose}
      />,
    );

    fireEvent.keyDown(screen.getByRole("dialog"), { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(onDecide).not.toHaveBeenCalled();
  });

  it("puts focus in the dialog, on the field the decision needs", () => {
    render(
      <ApprovalGate
        run={run}
        step={step}
        onDecide={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    expect(document.activeElement).toBe(screen.getByLabelText(/reason/));
  });

  it("keeps Tab inside the dialog", () => {
    render(
      <ApprovalGate
        run={run}
        step={step}
        onDecide={vi.fn()}
        onClose={vi.fn()}
      />,
    );
    const dialog = screen.getByRole("dialog");
    const stops = dialog.querySelectorAll("button, input");
    const last = stops[stops.length - 1] as HTMLElement;
    const first = stops[0] as HTMLElement;

    last.focus();
    fireEvent.keyDown(dialog, { key: "Tab" });
    expect(document.activeElement).toBe(first);
  });
});
