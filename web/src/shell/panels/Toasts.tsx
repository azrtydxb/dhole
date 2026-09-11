/**
 * The transient answer to an action, over the canvas.
 *
 * Toasts here carry OUTCOMES, not decoration: a connection refused because a
 * report cannot feed a blob port, a plugin installed and mirrored, a run
 * finished. They sit over the canvas because that is where the user was
 * looking when they did the thing, and a message in a panel they are not
 * watching is a message that did not happen.
 *
 * They stack and they expire. Stacking matters because actions come in bursts
 * — three rejected wires in a row is three facts, and a single slot would show
 * the last one and swallow the two that explain it. Expiring matters because
 * nothing here is an error the user must acknowledge: anything that has to be
 * acted on belongs in the diagnostics strip, which does not disappear.
 *
 * Each toast keeps its own timer rather than one sweep over the list, so a
 * toast that arrives late gets its full reading time instead of inheriting
 * whatever was left of an earlier one's.
 */
import { useEffect } from "react";

/** ToastTone maps an outcome to the palette. It is the tone, not a colour:
 * a component that took "var(--err)" would let a caller paint a success
 * green-on-red by accident. */
export type ToastTone = "ok" | "error" | "warning" | "info";

export type Toast = {
  readonly id: string;
  readonly text: string;
  readonly tone: ToastTone;
};

const toneToken: Record<ToastTone, string> = {
  ok: "var(--ok)",
  error: "var(--err)",
  warning: "var(--warn)",
  info: "var(--accent)",
};

function ToastRow({
  toast,
  onDismiss,
  durationMs,
}: {
  readonly toast: Toast;
  readonly onDismiss: (id: string) => void;
  readonly durationMs: number;
}) {
  useEffect(() => {
    const timer = setTimeout(() => {
      onDismiss(toast.id);
    }, durationMs);
    return () => {
      clearTimeout(timer);
    };
  }, [toast.id, durationMs, onDismiss]);

  const colour = toneToken[toast.tone];
  return (
    <div
      role="status"
      onClick={() => {
        onDismiss(toast.id);
      }}
      style={{
        fontSize: 10,
        color: "var(--ink)",
        background: "var(--panel)",
        border: `1px solid ${colour}`,
        borderLeft: `3px solid ${colour}`,
        borderRadius: 6,
        padding: "8px 14px",
        boxShadow: "0 8px 28px rgba(0, 0, 0, 0.4)",
        whiteSpace: "nowrap",
        cursor: "pointer",
        pointerEvents: "auto",
      }}
    >
      {toast.text}
    </div>
  );
}

export function Toasts({
  toasts,
  onDismiss,
  durationMs = 2800,
}: {
  readonly toasts: readonly Toast[];
  /** Removes one toast from the caller's list. The component holds no list of
   * its own: two sources of truth for what is on screen is how a toast comes
   * back after being dismissed. */
  readonly onDismiss: (id: string) => void;
  readonly durationMs?: number;
}) {
  return (
    <div
      aria-live="polite"
      style={{
        position: "absolute",
        left: "50%",
        top: 64,
        transform: "translateX(-50%)",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        gap: 6,
        zIndex: 70,
        pointerEvents: "none",
      }}
    >
      {toasts.map((toast) => (
        <ToastRow
          key={toast.id}
          toast={toast}
          onDismiss={onDismiss}
          durationMs={durationMs}
        />
      ))}
    </div>
  );
}
