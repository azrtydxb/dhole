/**
 * Toasts have to go away on their own.
 *
 * A toast that never expires stacks over the canvas until it hides the thing
 * the user is editing, and one whose timer is shared with its neighbours
 * disappears before it has been read. These cover both: each toast expires on
 * its own clock, and the list the caller holds is the only list.
 */
// @vitest-environment jsdom
import { cleanup, render, screen } from "@testing-library/react";
import { act } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { Toasts, type Toast } from "./Toasts.js";

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

const ok: Toast = { id: "a", text: "connected · git-tree ✓", tone: "ok" };
const bad: Toast = { id: "b", text: "rejected · report → blob", tone: "error" };

describe("Toasts", () => {
  it("dismisses a toast once its time is up", () => {
    const onDismiss = vi.fn();
    render(<Toasts toasts={[ok]} onDismiss={onDismiss} durationMs={2800} />);
    expect(onDismiss).not.toHaveBeenCalled();
    act(() => {
      vi.advanceTimersByTime(2799);
    });
    expect(onDismiss).not.toHaveBeenCalled();
    act(() => {
      vi.advanceTimersByTime(1);
    });
    expect(onDismiss).toHaveBeenCalledWith("a");
  });

  it("gives a toast that arrived later its own full time", () => {
    const onDismiss = vi.fn();
    const { rerender } = render(
      <Toasts toasts={[ok]} onDismiss={onDismiss} durationMs={1000} />,
    );
    act(() => {
      vi.advanceTimersByTime(900);
    });
    rerender(
      <Toasts toasts={[ok, bad]} onDismiss={onDismiss} durationMs={1000} />,
    );
    act(() => {
      vi.advanceTimersByTime(100);
    });
    expect(onDismiss).toHaveBeenCalledWith("a");
    expect(onDismiss).not.toHaveBeenCalledWith("b");
    act(() => {
      vi.advanceTimersByTime(900);
    });
    expect(onDismiss).toHaveBeenCalledWith("b");
  });

  it("stacks every toast it is given", () => {
    render(<Toasts toasts={[ok, bad]} onDismiss={vi.fn()} />);
    expect(screen.getAllByRole("status")).toHaveLength(2);
  });

  it("dismisses on click, without waiting", () => {
    const onDismiss = vi.fn();
    render(<Toasts toasts={[ok]} onDismiss={onDismiss} />);
    act(() => {
      screen.getByRole("status").click();
    });
    expect(onDismiss).toHaveBeenCalledWith("a");
  });
});
