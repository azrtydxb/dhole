/**
 * The furniture around the graph: zoom, a minimap, and one line telling you
 * how to drive it.
 *
 * The hint bar is not a tooltip and does not dismiss itself. Direct
 * manipulation has no affordances of its own — nothing about a port says it
 * can be dragged — and a hint that disappears after first use is missing for
 * everybody who comes back a week later.
 *
 * These are drawn over the canvas rather than beside it so they cost the graph
 * no width, and every one of them sits in a corner the graph rarely occupies.
 */
import {
  Background,
  BackgroundVariant,
  MiniMap,
  useReactFlow,
  useStore,
} from "@xyflow/react";

import { svgTokens } from "../design/tokens.js";
import type { Theme } from "../design/useTheme.js";

/** The zoom control. It shows the current percentage rather than only +/-,
 * because "why is everything tiny" is answered by a number, not by a button. */
export function ZoomControl() {
  const { zoomIn, zoomOut, fitView } = useReactFlow();
  // Subscribed rather than read once: a pinch or a wheel changes the zoom
  // without going through these buttons, and a label that only updated when
  // clicked would be wrong most of the time.
  const zoom = useStore((state) => state.transform[2]);

  const button: React.CSSProperties = {
    background: "transparent",
    border: "none",
    color: "var(--ink2)",
    fontSize: 13,
    width: 28,
    height: 26,
    cursor: "pointer",
  };

  return (
    <div
      style={{
        position: "absolute",
        left: 14,
        bottom: 14,
        zIndex: 5,
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        background: "var(--panel)",
        border: "1px solid var(--line)",
        borderRadius: 6,
        overflow: "hidden",
      }}
    >
      <button
        type="button"
        onClick={() => void zoomIn()}
        style={button}
        aria-label="zoom in"
      >
        +
      </button>
      <button
        type="button"
        onClick={() => void fitView({ padding: 0.2 })}
        title="fit the whole pipeline"
        style={{
          ...button,
          fontSize: 9,
          borderTop: "1px solid var(--line2)",
          borderBottom: "1px solid var(--line2)",
        }}
      >
        {Math.round(zoom * 100)}%
      </button>
      <button
        type="button"
        onClick={() => void zoomOut()}
        style={button}
        aria-label="zoom out"
      >
        −
      </button>
    </div>
  );
}

/** HintBar states the three gestures the canvas supports. */
export function HintBar() {
  return (
    <div
      style={{
        position: "absolute",
        left: "50%",
        transform: "translateX(-50%)",
        bottom: 14,
        zIndex: 5,
        background: "var(--panel)",
        border: "1px solid var(--line2)",
        borderRadius: 5,
        padding: "6px 16px",
        fontSize: 9,
        color: "var(--ink3)",
        pointerEvents: "none",
      }}
    >
      drag canvas to pan · drag a port to connect · click a node for properties
    </div>
  );
}

/** CanvasGrid is the design's background: a 1px crosshatch at the grid pitch,
 * NOT React Flow's default dots. The difference is visible at a glance — a dot
 * field reads as texture, a ruled grid reads as a drawing surface — and it is
 * the surface every node position is judged against. */
export function CanvasGrid({ theme }: { readonly theme: Theme }) {
  return (
    <Background
      variant={BackgroundVariant.Lines}
      gap={24}
      lineWidth={1}
      // A concrete colour, not var(--grid): see tokens.ts.
      color={svgTokens[theme].grid}
    />
  );
}

/** CanvasMiniMap is React Flow's, wearing the editor's tokens. */
export function CanvasMiniMap({ theme }: { readonly theme: Theme }) {
  const { ink3, line, panel2 } = svgTokens[theme];
  return (
    <MiniMap
      pannable
      zoomable
      // Sits clear of the hint bar rather than on top of it: the two were
      // overlapping at the bottom-right corner, and the map won because it is
      // drawn later — which hid the only instructions on the screen.
      style={{
        background: panel2,
        border: `1px solid ${line}`,
        borderRadius: 6,
        bottom: 52,
        right: 14,
      }}
      maskColor="rgba(0,0,0,0.45)"
      // Resolved colours, not var(): the minimap writes these into SVG
      // attributes. Hard-coding them instead would leave the dark palette on a
      // light canvas.
      nodeColor={() => ink3}
      nodeStrokeColor={line}
    />
  );
}
