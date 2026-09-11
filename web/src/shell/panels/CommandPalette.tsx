/**
 * ⌘K: every command in the editor, reachable without knowing which menu it is
 * under.
 *
 * The menu bar is the discoverable path and this is the fast one, and they are
 * fed from the SAME command list for a reason: a command that exists only in a
 * menu is one the palette silently cannot run, and a user who learns to trust
 * the palette will conclude the feature was removed.
 *
 * It is keyboard-first, so the keyboard has to be complete. Arrow keys move
 * the selection and the selection is always valid — filtering resets it to the
 * first match, because a highlight left on row four of a list that now has two
 * rows means Enter runs something the user cannot see. Enter runs, Escape
 * closes, and Escape closing is not optional: this thing covers the canvas,
 * and a cover with no way out is a hang.
 *
 * Commands come from props. There is nothing invented here to mark — the
 * palette shows what its caller can actually do.
 */
import { useMemo, useState } from "react";

import { useDialogKeys } from "./Modal.js";

/** Command is one runnable thing. `hint` is the shortcut as text and is shown,
 * never bound here — the binding belongs with whatever listens for it, so the
 * palette cannot advertise a key nothing handles. */
export type Command = {
  readonly id: string;
  readonly label: string;
  /** Where it lives in the menus, e.g. "file" — shown so the palette teaches
   * the slow path rather than replacing it. */
  readonly group?: string;
  readonly hint?: string;
  readonly disabled?: boolean;
};

/** matches is the filter, exported so its behaviour is testable without a
 * keyboard: a case-insensitive substring over the label and its group. */
export function matches(command: Command, query: string): boolean {
  const needle = query.trim().toLowerCase();
  if (needle === "") return true;
  return (
    command.label.toLowerCase().includes(needle) ||
    (command.group ?? "").toLowerCase().includes(needle)
  );
}

export function CommandPalette({
  commands,
  onRun,
  open,
  onClose,
}: {
  readonly commands: readonly Command[];
  /** Runs one command by id. The palette closes itself first: leaving it open
   * over the thing the command just changed hides the result. */
  readonly onRun: (id: string) => void;
  readonly open: boolean;
  readonly onClose: () => void;
}) {
  // Mounting a fresh body per opening is what resets the query and the
  // highlight. Holding them across an open would run a stale filter against a
  // pipeline that has since changed — and would put Enter on whatever row the
  // user last hovered, minutes ago.
  if (!open) return null;
  return <PaletteBody commands={commands} onRun={onRun} onClose={onClose} />;
}

function PaletteBody({
  commands,
  onRun,
  onClose,
}: {
  readonly commands: readonly Command[];
  readonly onRun: (id: string) => void;
  readonly onClose: () => void;
}) {
  const [query, setQuery] = useState("");
  const [index, setIndex] = useState(0);
  const panelRef = useDialogKeys(true, onClose);

  const shown = useMemo(
    () => commands.filter((command) => matches(command, query)),
    [commands, query],
  );

  const clamped = Math.min(index, Math.max(shown.length - 1, 0));
  const selected = shown[clamped];

  const run = (command: Command | undefined) => {
    if (command === undefined || command.disabled === true) return;
    onClose();
    onRun(command.id);
  };

  const onKeyDown = (event: React.KeyboardEvent) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      setIndex(shown.length === 0 ? 0 : (clamped + 1) % shown.length);
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      setIndex(
        shown.length === 0 ? 0 : (clamped - 1 + shown.length) % shown.length,
      );
    } else if (event.key === "Enter") {
      event.preventDefault();
      run(selected);
    }
  };

  return (
    <div
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0, 0, 0, 0.45)",
        zIndex: 80,
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "center",
        paddingTop: "14vh",
      }}
    >
      <div
        ref={panelRef}
        role="dialog"
        aria-modal
        aria-label="command palette"
        tabIndex={-1}
        onKeyDown={onKeyDown}
        style={{
          width: 460,
          maxHeight: 340,
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
            gap: 8,
            padding: "10px 14px",
            borderBottom: "1px solid var(--line2)",
            flex: "none",
          }}
        >
          <span style={{ fontSize: 10, color: "var(--ink3)" }}>⌘K</span>
          <input
            autoFocus
            value={query}
            placeholder="run a command…"
            aria-label="run a command"
            role="combobox"
            aria-expanded
            aria-controls="dh-palette-list"
            aria-activedescendant={
              selected === undefined ? undefined : `dh-cmd-${selected.id}`
            }
            onChange={(event) => {
              setQuery(event.currentTarget.value);
              setIndex(0);
            }}
            style={{
              flex: 1,
              minWidth: 0,
              background: "none",
              border: "none",
              color: "var(--ink)",
              fontSize: 11,
              outline: "none",
            }}
          />
        </div>

        <div
          id="dh-palette-list"
          role="listbox"
          aria-label="commands"
          style={{
            flex: 1,
            overflowY: "auto",
            padding: 4,
            display: "flex",
            flexDirection: "column",
            gap: 1,
            minHeight: 0,
          }}
        >
          {shown.length === 0 && (
            <div
              style={{
                fontSize: 9,
                color: "var(--ink3)",
                textAlign: "center",
                padding: "14px 0",
              }}
            >
              no command matches
            </div>
          )}
          {shown.map((command, position) => {
            const active = position === clamped;
            return (
              <div
                key={command.id}
                id={`dh-cmd-${command.id}`}
                role="option"
                aria-selected={active}
                aria-disabled={command.disabled === true}
                onMouseEnter={() => {
                  setIndex(position);
                }}
                onClick={() => {
                  run(command);
                }}
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  gap: 18,
                  padding: "6px 10px",
                  fontSize: 10,
                  borderRadius: 4,
                  cursor: command.disabled === true ? "default" : "pointer",
                  whiteSpace: "nowrap",
                  background: active ? "var(--accent-soft)" : "none",
                  color:
                    command.disabled === true
                      ? "var(--ink3)"
                      : active
                        ? "var(--ink)"
                        : "var(--ink2)",
                }}
              >
                <span
                  style={{
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                  }}
                >
                  {command.group !== undefined && (
                    <span style={{ color: "var(--ink3)" }}>
                      {command.group} ·{" "}
                    </span>
                  )}
                  {command.label}
                </span>
                {command.hint !== undefined && (
                  <span
                    style={{
                      color: "var(--ink3)",
                      fontSize: 9,
                      flex: "none",
                    }}
                  >
                    {command.hint}
                  </span>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
