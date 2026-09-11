/**
 * The keyboard contract every decision surface in this folder owes the person
 * using it.
 *
 * Two things, and both are about the same failure. A panel that asks for a
 * decision and then lets Tab walk out of it leaves the keyboard user editing
 * the canvas BEHIND the question — they cannot see what has focus, and the
 * next Enter presses something they never looked at. And a panel with no
 * Escape is a panel you can only leave by finding its close button with a
 * mouse, which for a modal over the whole editor means the answer to "I opened
 * this by accident" is a decision.
 *
 * So: focus moves into the surface when it opens, Tab cycles inside it while
 * it overlays the editor, and Escape leaves without deciding anything. Escape is never a decision — it
 * closes, and closing an approval gate is not a denial.
 */
import { useEffect, useRef } from "react";

/** The elements a browser would let you Tab to, in the order it would. */
const FOCUSABLE = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled])",
  "textarea:not([disabled])",
  "select:not([disabled])",
  '[tabindex]:not([tabindex="-1"])',
].join(",");

/**
 * useDialogChrome returns a ref to put on the surface's outermost element.
 * That element needs `tabIndex={-1}` so it can hold focus itself while it
 * contains nothing focusable.
 *
 * The element marked `data-autofocus` takes focus on open; without one the
 * surface itself does, which is what a screen reader needs in order to read
 * the question before the buttons.
 */
export function useDialogChrome<T extends HTMLElement>(
  onClose: () => void,
  /** Trapping is right for a surface that OVERLAYS the editor and wrong for a
   * rail that sits beside it: trapping the assistant rail would mean Tab could
   * never reach the canvas again. */
  trap = true,
): React.RefObject<T | null> {
  const ref = useRef<T | null>(null);

  useEffect(() => {
    const root = ref.current;
    if (root === null) return;

    const preferred = root.querySelector<HTMLElement>("[data-autofocus]");
    (preferred ?? root).focus();

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.stopPropagation();
        onClose();
        return;
      }
      if (event.key !== "Tab" || !trap) return;

      const stops = Array.from(root.querySelectorAll<HTMLElement>(FOCUSABLE));
      if (stops.length === 0) {
        event.preventDefault();
        return;
      }
      const first = stops[0];
      const last = stops[stops.length - 1];
      if (first === undefined || last === undefined) return;

      const active = document.activeElement;
      if (event.shiftKey && (active === first || active === root)) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && active === last) {
        event.preventDefault();
        first.focus();
      }
    };

    root.addEventListener("keydown", onKeyDown);
    return () => {
      root.removeEventListener("keydown", onKeyDown);
    };
  }, [onClose, trap]);

  return ref;
}
