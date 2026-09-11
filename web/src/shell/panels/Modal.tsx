/**
 * The frame every panel in this editor sits in, and the keyboard contract that
 * comes with it.
 *
 * A modal is the one place in the editor where the rest of the UI is still on
 * screen but unreachable, so the ways OUT of it are the load-bearing part, not
 * the border radius. Escape closes, a click on the backdrop closes, focus is
 * moved into the panel on open and cycled inside it on Tab, and focus is
 * handed back to whatever opened the panel on close. Leave any one of those
 * out and a keyboard user who opens settings is simply stuck: Tab walks them
 * into a menu bar they cannot see and cannot operate.
 *
 * It also carries the SAMPLE chip. Four of these panels — settings, registry,
 * git mirror, import — draw content that no RPC produces yet; the chip is how
 * a reader tells that apart from fleet data that came off the wire, without
 * having to know which services exist. It is part of the frame rather than
 * each panel's body so it cannot be styled into invisibility one panel at a
 * time.
 */
import { useCallback, useEffect, useRef, type ReactNode } from "react";

/** Everything inside a dialog that a Tab can land on. `[href]` is here because
 * the git and registry panels explain themselves with links. */
const focusableSelector = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  '[tabindex]:not([tabindex="-1"])',
].join(",");

/**
 * useDialogKeys wires Escape and the Tab cycle to one element, and restores
 * focus when it goes away.
 *
 * It is exported because the command palette is a dialog that is deliberately
 * NOT a Modal — it has no title row and no backdrop panel — and duplicating
 * the trap there is how the two drift until only one of them actually holds
 * focus.
 */
export function useDialogKeys(
  open: boolean,
  onClose: () => void,
): React.RefObject<HTMLDivElement | null> {
  const ref = useRef<HTMLDivElement | null>(null);
  // The latest onClose, read by the listener. Kept in a ref so a caller that
  // passes a fresh closure every render does not tear the listener down and
  // rebuild it mid-keystroke.
  const closeRef = useRef(onClose);
  useEffect(() => {
    closeRef.current = onClose;
  });

  useEffect(() => {
    if (!open) return;
    const opener = document.activeElement;
    const panel = ref.current;
    // Something inside may have claimed focus already (the palette's input
    // autofocuses); pulling it back to the panel would undo that.
    if (panel !== null && !panel.contains(document.activeElement))
      panel.focus();

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.preventDefault();
        closeRef.current();
        return;
      }
      if (event.key !== "Tab") return;
      const host = ref.current;
      if (host === null) return;
      const targets = Array.from(
        host.querySelectorAll<HTMLElement>(focusableSelector),
      );
      // Nothing to cycle between: keep the caret on the panel rather than
      // letting Tab escape into the disabled UI behind the backdrop.
      if (targets.length === 0) {
        event.preventDefault();
        host.focus();
        return;
      }
      const first = targets[0];
      const last = targets[targets.length - 1];
      if (first === undefined || last === undefined) return;
      const active = document.activeElement;
      if (event.shiftKey && (active === first || active === host)) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && active === last) {
        event.preventDefault();
        first.focus();
      }
    };

    document.addEventListener("keydown", onKeyDown, true);
    return () => {
      document.removeEventListener("keydown", onKeyDown, true);
      if (opener instanceof HTMLElement) opener.focus();
    };
  }, [open]);

  return ref;
}

/** SampleChip says, permanently and on the panel's own face, that what is
 * below it is drawn from a fixture. It is not a tooltip and not a footnote:
 * invented rows that look like plane data are the failure this exists to
 * prevent. */
function SampleChip() {
  return (
    <span
      style={{
        border: "1px solid var(--warn)",
        color: "var(--warn)",
        borderRadius: 3,
        padding: "0 5px",
        fontSize: 9,
        whiteSpace: "nowrap",
        flex: "none",
      }}
    >
      sample · no endpoint
    </span>
  );
}

export function Modal({
  title,
  subtitle,
  width,
  height,
  sample = false,
  onClose,
  footer,
  children,
}: {
  readonly title: string;
  /** The grey line beside the title — what the panel is reading, not what it
   * does. */
  readonly subtitle?: string;
  readonly width: number;
  /** Panels with their own internal scroll (settings, registry) are a fixed
   * height so the scroll region does not move as content changes. */
  readonly height?: number;
  readonly sample?: boolean;
  readonly onClose: () => void;
  readonly footer?: ReactNode;
  readonly children: ReactNode;
}) {
  const panelRef = useDialogKeys(true, onClose);
  const onBackdrop = useCallback(
    (event: React.MouseEvent<HTMLDivElement>) => {
      if (event.target === event.currentTarget) onClose();
    },
    [onClose],
  );

  return (
    <div
      onMouseDown={onBackdrop}
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0, 0, 0, 0.45)",
        zIndex: 80,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <div
        ref={panelRef}
        role="dialog"
        aria-modal
        aria-label={title}
        tabIndex={-1}
        style={{
          width,
          height,
          background: "var(--panel)",
          border: "1px solid var(--line)",
          borderRadius: 8,
          boxShadow: "0 24px 60px rgba(0, 0, 0, 0.5)",
          display: "flex",
          flexDirection: "column",
          minHeight: 0,
          outline: "none",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            padding: "13px 16px",
            borderBottom: "1px solid var(--line2)",
            flex: "none",
          }}
        >
          <span style={{ fontSize: 12, fontWeight: 700 }}>{title}</span>
          {sample && <SampleChip />}
          {subtitle !== undefined && (
            <span
              style={{
                fontSize: 9,
                color: "var(--ink3)",
                whiteSpace: "nowrap",
                overflow: "hidden",
                textOverflow: "ellipsis",
              }}
            >
              {subtitle}
            </span>
          )}
          <button
            type="button"
            onClick={onClose}
            aria-label="close"
            style={{
              marginLeft: "auto",
              background: "none",
              border: "none",
              color: "var(--ink3)",
              fontSize: 12,
              cursor: "pointer",
              flex: "none",
            }}
          >
            ✕
          </button>
        </div>

        {children}

        {footer !== undefined && (
          <div
            style={{
              display: "flex",
              justifyContent: "flex-end",
              gap: 8,
              padding: "12px 16px",
              borderTop: "1px solid var(--line2)",
              flex: "none",
            }}
          >
            {footer}
          </div>
        )}
      </div>
    </div>
  );
}

/** ModalButton is the footer's two weights, from the design: a bordered
 * secondary and a filled accent primary. They are here rather than repeated in
 * five panels so "cancel" is the same object everywhere it appears. */
export function ModalButton({
  kind,
  onClick,
  children,
}: {
  readonly kind: "primary" | "secondary";
  readonly onClick: () => void;
  readonly children: ReactNode;
}) {
  const primary = kind === "primary";
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        background: primary ? "var(--accent)" : "none",
        border: primary ? "none" : "1px solid var(--line)",
        borderRadius: 5,
        color: primary ? "#fff" : "var(--ink2)",
        fontSize: 10,
        fontWeight: primary ? 700 : 400,
        padding: "6px 14px",
        cursor: "pointer",
      }}
    >
      {children}
    </button>
  );
}
