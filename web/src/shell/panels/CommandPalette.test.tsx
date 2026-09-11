/**
 * The palette is keyboard-only in practice, so the keyboard is what is tested.
 *
 * Three breaks these catch: a filter that leaves the highlight on a row that
 * scrolled out of existence (Enter then runs an invisible command), arrow keys
 * that walk off the end of the list, and an Escape that does not close — which
 * turns a full-screen cover into a hang with no mouse target.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { CommandPalette, type Command } from "./CommandPalette.js";

afterEach(cleanup);

const commands: readonly Command[] = [
  { id: "new", label: "new pipeline", group: "file", hint: "⌘N" },
  { id: "import", label: "import YAML…", group: "file" },
  { id: "save", label: "save revision", group: "file", hint: "⌘S" },
  { id: "theme", label: "toggle theme", group: "view" },
];

function open(onRun = vi.fn(), onClose = vi.fn()) {
  render(
    <CommandPalette commands={commands} onRun={onRun} open onClose={onClose} />,
  );
  return { onRun, onClose, input: screen.getByRole("combobox") };
}

describe("CommandPalette", () => {
  it("shows every command until something is typed", () => {
    open();
    expect(screen.getAllByRole("option")).toHaveLength(4);
  });

  it("filters on the label", () => {
    const { input } = open();
    fireEvent.change(input, { target: { value: "revis" } });
    const options = screen.getAllByRole("option");
    expect(options).toHaveLength(1);
    expect(options[0]?.textContent).toContain("save revision");
  });

  it("filters on the group, so 'file' finds the file menu", () => {
    const { input } = open();
    fireEvent.change(input, { target: { value: "file" } });
    expect(screen.getAllByRole("option")).toHaveLength(3);
  });

  it("puts the selection back on the first match after filtering", () => {
    // Down twice lands on "save revision". Typing "file" leaves three matches,
    // so a highlight that survived the keystroke would still be valid — and
    // Enter would run the third row while the user is looking at the first.
    const { input, onRun, onClose } = open();
    fireEvent.keyDown(input, { key: "ArrowDown" });
    fireEvent.keyDown(input, { key: "ArrowDown" });
    fireEvent.change(input, { target: { value: "file" } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onRun).toHaveBeenCalledWith("new");
    expect(onClose).toHaveBeenCalled();
  });

  it("moves the selection with the arrow keys and wraps", () => {
    const { input, onRun } = open();
    fireEvent.keyDown(input, { key: "ArrowUp" });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onRun).toHaveBeenCalledWith("theme");
  });

  it("runs the highlighted command on Enter", () => {
    const { input, onRun } = open();
    fireEvent.keyDown(input, { key: "ArrowDown" });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onRun).toHaveBeenCalledWith("import");
  });

  it("runs nothing when no command matches", () => {
    const { input, onRun } = open();
    fireEvent.change(input, { target: { value: "zzz" } });
    fireEvent.keyDown(input, { key: "Enter" });
    expect(onRun).not.toHaveBeenCalled();
    expect(screen.queryAllByRole("option")).toHaveLength(0);
  });

  it("closes on Escape", () => {
    const { onClose } = open();
    fireEvent.keyDown(document, { key: "Escape" });
    expect(onClose).toHaveBeenCalled();
  });

  it("renders nothing while closed", () => {
    render(
      <CommandPalette
        commands={commands}
        onRun={vi.fn()}
        open={false}
        onClose={vi.fn()}
      />,
    );
    expect(screen.queryByRole("dialog")).toBeNull();
  });
});
