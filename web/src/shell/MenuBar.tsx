/**
 * The top bar: what this is, what you can do to it, and which thing you are
 * looking at.
 *
 * The breadcrumb is the load-bearing part. Every edit in Dhole is applied
 * against a BASE REVISION (ADR 0020), so "which revision am I editing" is not
 * a detail for a status bar — it decides whose work an operation is about to
 * land on. It sits in the title bar, beside the draft badge, because a user
 * who cannot see it can still edit.
 */
import { useEffect, useRef, useState } from "react";

/** MenuItem is one row of a dropdown. `shortcut` is shown, never bound here:
 * the binding lives with the command so a menu cannot claim a key nothing
 * listens for. */
export type MenuItem = {
  readonly id: string;
  readonly label: string;
  readonly shortcut?: string;
  readonly disabled?: boolean;
};

export type Menu = {
  readonly id: string;
  readonly label: string;
  readonly items: readonly MenuItem[];
};

export function MenuBar({
  menus,
  onCommand,
  tenant,
  pipelineName,
  revisionId,
  revisionState,
  themeLabel,
  onToggleTheme,
  user,
}: {
  readonly menus: readonly Menu[];
  readonly onCommand: (menuId: string, itemId: string) => void;
  readonly tenant: string;
  readonly pipelineName: string;
  readonly revisionId: string;
  readonly revisionState: "draft" | "active" | "unknown";
  readonly themeLabel: string;
  readonly onToggleTheme: () => void;
  readonly user: string;
}) {
  const [open, setOpen] = useState<string | null>(null);
  const barRef = useRef<HTMLDivElement>(null);

  // A menu that stays open after the pointer leaves is a menu that covers the
  // canvas you are trying to click. Escape closes it too, because a keyboard
  // user who opened one has no other way out.
  useEffect(() => {
    if (open === null) return;
    const onDown = (event: MouseEvent) => {
      if (!barRef.current?.contains(event.target as globalThis.Node)) {
        setOpen(null);
      }
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpen(null);
    };
    globalThis.addEventListener("mousedown", onDown);
    globalThis.addEventListener("keydown", onKey);
    return () => {
      globalThis.removeEventListener("mousedown", onDown);
      globalThis.removeEventListener("keydown", onKey);
    };
  }, [open]);

  return (
    <div
      ref={barRef}
      style={{
        display: "flex",
        alignItems: "center",
        gap: 14,
        padding: "0 14px",
        background: "var(--panel2)",
        borderBottom: "1px solid var(--line2)",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 7,
          marginRight: 4,
        }}
      >
        <span
          aria-hidden
          style={{
            width: 9,
            height: 9,
            background: "var(--rust)",
            transform: "rotate(45deg)",
            borderRadius: 2,
          }}
        />
        <span style={{ fontSize: 13, fontWeight: 700, color: "var(--ink)" }}>
          Dhole
        </span>
      </div>

      <div style={{ display: "flex", alignItems: "center", gap: 2 }}>
        {menus.map((menu) => (
          <span key={menu.id} style={{ position: "relative" }}>
            <button
              type="button"
              aria-haspopup="menu"
              aria-expanded={open === menu.id}
              onClick={() => setOpen(open === menu.id ? null : menu.id)}
              style={{
                background: open === menu.id ? "var(--panel)" : "transparent",
                border: "none",
                color: "var(--ink2)",
                fontSize: 10,
                padding: "4px 9px",
                borderRadius: 4,
                cursor: "pointer",
              }}
            >
              {menu.label}
            </button>
            {open === menu.id && (
              <div
                role="menu"
                style={{
                  position: "absolute",
                  top: 26,
                  left: 0,
                  minWidth: 210,
                  background: "var(--panel)",
                  border: "1px solid var(--line)",
                  borderRadius: 6,
                  padding: 4,
                  zIndex: 60,
                  boxShadow: "0 8px 24px rgba(0,0,0,0.35)",
                }}
              >
                {menu.items.map((item) => (
                  <div
                    key={item.id}
                    role="menuitem"
                    tabIndex={item.disabled === true ? -1 : 0}
                    aria-disabled={item.disabled === true}
                    onClick={() => {
                      if (item.disabled === true) return;
                      setOpen(null);
                      onCommand(menu.id, item.id);
                    }}
                    style={{
                      display: "flex",
                      justifyContent: "space-between",
                      alignItems: "center",
                      gap: 18,
                      padding: "6px 10px",
                      fontSize: 10,
                      color:
                        item.disabled === true ? "var(--ink3)" : "var(--ink2)",
                      borderRadius: 4,
                      cursor: item.disabled === true ? "default" : "pointer",
                    }}
                  >
                    <span>{item.label}</span>
                    {item.shortcut !== undefined && (
                      <span style={{ color: "var(--ink3)" }}>
                        {item.shortcut}
                      </span>
                    )}
                  </div>
                ))}
              </div>
            )}
          </span>
        ))}
      </div>

      <div style={{ width: 1, height: 16, background: "var(--line2)" }} />

      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          fontSize: 10,
          color: "var(--ink3)",
          minWidth: 0,
        }}
      >
        <span>{tenant}</span>
        <span>/</span>
        <span style={{ color: "var(--ink)" }}>{pipelineName}</span>
        <span>/</span>
        <span className="dh-selectable">
          rev {revisionId === "" ? "—" : revisionId.slice(0, 11)}
        </span>
        {revisionState !== "unknown" && (
          <span
            style={{
              fontSize: 8,
              letterSpacing: 0.5,
              textTransform: "uppercase",
              color: revisionState === "draft" ? "var(--warn)" : "var(--ok)",
              border: `1px solid ${
                revisionState === "draft" ? "var(--warn)" : "var(--ok)"
              }`,
              borderRadius: 3,
              padding: "1px 5px",
            }}
          >
            {revisionState}
          </span>
        )}
      </div>

      <div style={{ flex: 1 }} />

      <button
        type="button"
        onClick={onToggleTheme}
        style={{
          background: "var(--panel)",
          border: "1px solid var(--line)",
          color: "var(--ink2)",
          fontSize: 10,
          padding: "4px 10px",
          borderRadius: 4,
          cursor: "pointer",
        }}
      >
        {themeLabel}
      </button>
      <span
        title={user}
        style={{
          width: 22,
          height: 22,
          borderRadius: "50%",
          background: "var(--accent-soft)",
          border: "1px solid var(--accent)",
          color: "var(--accent)",
          fontSize: 9,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        {user.slice(0, 2)}
      </span>
    </div>
  );
}
