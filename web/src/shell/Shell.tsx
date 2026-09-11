/**
 * The frame every other piece hangs in.
 *
 * Four rows, fixed at the top and bottom, with the middle free to grow: menu
 * bar, toolbar, work area, status bar. The work area is three columns —
 * catalog, canvas, inspector — and only the canvas is elastic, because the two
 * rails hold lists whose readable width does not change with the window.
 *
 * Diagnostics sit INSIDE the work area's bottom edge rather than above the
 * status bar as a fifth row, so they overlay the canvas's own footprint and a
 * long list cannot push the status bar off the screen.
 */

export function Shell({
  menuBar,
  toolbar,
  sidebar,
  canvas,
  inspector,
  diagnostics,
  statusBar,
  overlays,
  sidebarWidth = 285,
  inspectorWidth = 370,
}: {
  readonly menuBar: React.ReactNode;
  readonly toolbar: React.ReactNode;
  readonly sidebar: React.ReactNode;
  readonly canvas: React.ReactNode;
  readonly inspector: React.ReactNode;
  readonly diagnostics: React.ReactNode;
  readonly statusBar: React.ReactNode;
  /** Things that float above the whole shell — modals, the assistant rail, a
   * gate. They sit outside the grid so a modal is not clipped by the pane it
   * was opened from, which is what happens when a dialog is rendered inside a
   * scrolling column. */
  readonly overlays?: React.ReactNode;
  readonly sidebarWidth?: number;
  readonly inspectorWidth?: number;
}) {
  return (
    <div
      className="dh-app"
      style={{
        width: "100vw",
        height: "100vh",
        display: "grid",
        gridTemplateRows: "38px 44px 1fr 26px",
        background: "var(--bg)",
        color: "var(--ink)",
        overflow: "hidden",
      }}
    >
      {menuBar}
      {toolbar}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: `${sidebarWidth}px 1fr ${inspectorWidth}px`,
          minHeight: 0,
        }}
      >
        {sidebar}
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            minWidth: 0,
            minHeight: 0,
          }}
        >
          <div style={{ flex: 1, minHeight: 0, position: "relative" }}>
            {canvas}
          </div>
          {diagnostics}
        </div>
        {inspector}
      </div>
      {statusBar}
      {overlays}
    </div>
  );
}
