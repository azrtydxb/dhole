// @vitest-environment jsdom
/**
 * Two risks, and the tests are about both. A prompt left in the box after send
 * is a prompt that gets sent twice, which against a pipeline means two op sets
 * proposed on the same revision. And a sample assistant that does not SAY it
 * is a sample is a reply somebody trusts, so the marker is not a detail of the
 * markup — it is the condition this panel exists under.
 */
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  AssistantPanel,
  type AssistantMessage,
  type AssistantOp,
} from "./AssistantPanel.js";

afterEach(cleanup);

const ops: readonly AssistantOp[] = [
  { op: "add_step", summary: "sbom · anchore/syft@1.4.2" },
  { op: "connect", summary: "build-image.image → sbom.image" },
];

const messages: readonly AssistantMessage[] = [
  { id: "m1", role: "user", text: "add an sbom step after the build" },
  { id: "m2", role: "assistant", text: "two operations:", ops },
];

/** Composer wires the panel the way the editor does — the input is state the
 * caller holds — so "send clears the box" is tested through the real path. */
function Composer({ onSend }: { readonly onSend: (text: string) => void }) {
  const [input, setInput] = useState("");
  return (
    <AssistantPanel
      messages={[]}
      busy={false}
      input={input}
      onInput={setInput}
      onSend={onSend}
      onClose={vi.fn()}
      onApplyOps={vi.fn()}
    />
  );
}

describe("AssistantPanel", () => {
  it("sends the prompt and empties the box", () => {
    const onSend = vi.fn();
    render(<Composer onSend={onSend} />);

    const box = screen.getByLabelText("ask the assistant");
    fireEvent.change(box, { target: { value: "  add an sbom step  " } });
    fireEvent.click(screen.getByLabelText("send"));

    expect(onSend).toHaveBeenCalledWith("add an sbom step");
    expect((box as HTMLInputElement).value).toBe("");
  });

  it("sends on Enter as well as on the button", () => {
    const onSend = vi.fn();
    render(<Composer onSend={onSend} />);

    const box = screen.getByLabelText("ask the assistant");
    fireEvent.change(box, { target: { value: "notify slack on push" } });
    fireEvent.keyDown(box, { key: "Enter" });

    expect(onSend).toHaveBeenCalledWith("notify slack on push");
    expect((box as HTMLInputElement).value).toBe("");
  });

  it("sends nothing for an empty prompt, and nothing while busy", () => {
    const onSend = vi.fn();
    const { rerender } = render(
      <AssistantPanel
        messages={[]}
        busy={false}
        input="   "
        onInput={vi.fn()}
        onSend={onSend}
        onClose={vi.fn()}
        onApplyOps={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByLabelText("send"));
    expect(onSend).not.toHaveBeenCalled();

    rerender(
      <AssistantPanel
        messages={[]}
        busy
        input="add an sbom step"
        onInput={vi.fn()}
        onSend={onSend}
        onClose={vi.fn()}
        onApplyOps={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByLabelText("send"));
    expect(onSend).not.toHaveBeenCalled();
  });

  it("hands the proposed operations back untouched, with the message they came from", () => {
    const onApplyOps = vi.fn();
    render(
      <AssistantPanel
        messages={messages}
        busy={false}
        input=""
        onInput={vi.fn()}
        onSend={vi.fn()}
        onClose={vi.fn()}
        onApplyOps={onApplyOps}
      />,
    );

    fireEvent.click(screen.getByRole("button", { name: "APPLY" }));
    expect(onApplyOps).toHaveBeenCalledWith(ops, "m2");
  });

  it("offers no APPLY on an op set that has already been applied", () => {
    render(
      <AssistantPanel
        messages={[
          { id: "m2", role: "assistant", text: "done", ops, applied: true },
        ]}
        busy={false}
        input=""
        onInput={vi.fn()}
        onSend={vi.fn()}
        onClose={vi.fn()}
        onApplyOps={vi.fn()}
      />,
    );

    expect(screen.queryByRole("button", { name: "APPLY" })).toBeNull();
    expect(screen.getByTestId("assistant-ops").textContent).toContain(
      "✓ applied",
    );
  });

  it("says on its face that nothing is behind it, until an endpoint is", () => {
    const { rerender } = render(
      <AssistantPanel
        messages={messages}
        busy={false}
        input=""
        onInput={vi.fn()}
        onSend={vi.fn()}
        onClose={vi.fn()}
        onApplyOps={vi.fn()}
      />,
    );
    expect(screen.getByTestId("assistant-sample-marker").textContent).toBe(
      "sample · no endpoint",
    );

    rerender(
      <AssistantPanel
        messages={messages}
        busy={false}
        input=""
        sample={false}
        onInput={vi.fn()}
        onSend={vi.fn()}
        onClose={vi.fn()}
        onApplyOps={vi.fn()}
      />,
    );
    expect(screen.queryByTestId("assistant-sample-marker")).toBeNull();
  });

  it("closes on Escape", () => {
    const onClose = vi.fn();
    render(
      <AssistantPanel
        messages={messages}
        busy={false}
        input=""
        onInput={vi.fn()}
        onSend={vi.fn()}
        onClose={onClose}
        onApplyOps={vi.fn()}
      />,
    );

    fireEvent.keyDown(screen.getByRole("complementary"), { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
  });
});
