// @vitest-environment jsdom
/**
 * The review exists so an agent's edit lands only when a person says so. These
 * tests hold that line: both buttons hand back exactly the change list that
 * was shown, and closing is neither an approval nor a rejection.
 */
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { DiffReview, type ReviewChange } from "./DiffReview.js";

afterEach(cleanup);

const changes: readonly ReviewChange[] = [
  {
    kind: "added",
    op: "add_step",
    summary: "sbom",
    detail: "anchore/syft@1.4.2",
  },
  { kind: "added", op: "connect", summary: "build-image.image → sbom.image" },
  { kind: "changed", op: "set_property", summary: 'sbom.format = "spdx-json"' },
];

describe("DiffReview", () => {
  it("approves with the exact change list it displayed", () => {
    const onApprove = vi.fn();
    const onReject = vi.fn();
    render(
      <DiffReview
        changes={changes}
        onApprove={onApprove}
        onReject={onReject}
        onClose={vi.fn()}
      />,
    );

    expect(screen.getAllByTestId("diff-review-change")).toHaveLength(3);
    fireEvent.click(screen.getByRole("button", { name: "APPLY" }));
    expect(onApprove).toHaveBeenCalledWith(changes);
    expect(onReject).not.toHaveBeenCalled();
  });

  it("rejects with the same list, and does not approve on the way", () => {
    const onApprove = vi.fn();
    const onReject = vi.fn();
    render(
      <DiffReview
        changes={changes}
        onApprove={onApprove}
        onReject={onReject}
        onClose={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "REJECT" }));
    expect(onReject).toHaveBeenCalledWith(changes);
    expect(onApprove).not.toHaveBeenCalled();
  });

  it("cannot apply an empty diff", () => {
    const onApprove = vi.fn();
    render(
      <DiffReview
        changes={[]}
        onApprove={onApprove}
        onReject={vi.fn()}
        onClose={vi.fn()}
      />,
    );

    expect(screen.getByTestId("diff-review-empty")).toBeDefined();
    fireEvent.click(screen.getByRole("button", { name: "APPLY" }));
    expect(onApprove).not.toHaveBeenCalled();
  });

  it("closes on Escape without deciding", () => {
    const onClose = vi.fn();
    const onApprove = vi.fn();
    const onReject = vi.fn();
    render(
      <DiffReview
        changes={changes}
        onApprove={onApprove}
        onReject={onReject}
        onClose={onClose}
      />,
    );

    fireEvent.keyDown(screen.getByRole("dialog"), { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
    expect(onApprove).not.toHaveBeenCalled();
    expect(onReject).not.toHaveBeenCalled();
  });

  it("does nothing while the plane is busy applying", () => {
    const onApprove = vi.fn();
    const onReject = vi.fn();
    render(
      <DiffReview
        changes={changes}
        busy
        onApprove={onApprove}
        onReject={onReject}
        onClose={vi.fn()}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "APPLY" }));
    fireEvent.click(screen.getByRole("button", { name: "REJECT" }));
    expect(onApprove).not.toHaveBeenCalled();
    expect(onReject).not.toHaveBeenCalled();
  });
});
