/**
 * The left rail: what you can add, what the wires mean, and what happened last
 * time.
 *
 * The PORT TYPES legend is not filler. The canvas draws a type as a shape and
 * a colour, and a legend is the only place that mapping is written down — a
 * user meeting a purple circle for the first time has nowhere else to look.
 * It is built from the same table the glyphs come from, so a type cannot be
 * drawn on the canvas and missing from the legend.
 *
 * The catalog and the run list are fed from props and render an EMPTY STATE
 * that names what is missing when there is nothing to show. The control plane
 * has no list-plugins and no list-runs RPC today: a sidebar that invented
 * plausible rows would be the most convincing part of the editor and the only
 * part that is not true.
 */
import { PortGlyph, type PortTypeName } from "../design/PortGlyph.js";

export type CatalogEntry = {
  readonly ref: string;
  readonly name: string;
  readonly category: string;
  readonly origin: "core" | "community";
  /** The type this plugin's primary output carries, for the row's glyph. */
  readonly produces: PortTypeName;
};

export type RunSummary = {
  readonly id: string;
  readonly label: string;
  readonly outcome: "succeeded" | "failed" | "running";
  readonly detail: string;
};

const legend: readonly PortTypeName[] = [
  "git-tree",
  "blob",
  "oci-image",
  "report",
  "approval",
];

function SectionLabel({ children }: { readonly children: React.ReactNode }) {
  return (
    <div
      style={{
        fontSize: 9,
        letterSpacing: 0.6,
        color: "var(--ink3)",
        padding: "12px 14px 6px",
      }}
    >
      {children}
    </div>
  );
}

function Empty({ children }: { readonly children: React.ReactNode }) {
  return (
    <div
      style={{
        margin: "0 14px",
        padding: "10px 12px",
        fontSize: 9,
        lineHeight: 1.5,
        color: "var(--ink3)",
        border: "1px dashed var(--line)",
        borderRadius: 5,
      }}
    >
      {children}
    </div>
  );
}

export function Sidebar({
  catalog,
  categories,
  activeCategory,
  onCategory,
  query,
  onQuery,
  runs,
  onOpenRun,
}: {
  readonly catalog: readonly CatalogEntry[];
  readonly categories: readonly string[];
  readonly activeCategory: string;
  readonly onCategory: (category: string) => void;
  readonly query: string;
  readonly onQuery: (query: string) => void;
  readonly runs: readonly RunSummary[];
  readonly onOpenRun: (runId: string) => void;
}) {
  const shown = catalog.filter(
    (entry) =>
      (activeCategory === "all" || entry.category === activeCategory) &&
      (query === "" ||
        entry.name.toLowerCase().includes(query.toLowerCase()) ||
        entry.ref.toLowerCase().includes(query.toLowerCase())),
  );

  return (
    <div
      style={{
        borderRight: "1px solid var(--line2)",
        background: "var(--panel)",
        overflowY: "auto",
        display: "flex",
        flexDirection: "column",
      }}
    >
      <SectionLabel>
        CATALOG{catalog.length > 0 ? ` · ${catalog.length} plugins` : ""}
      </SectionLabel>

      <div style={{ padding: "0 14px 8px" }}>
        <input
          value={query}
          onChange={(event) => onQuery(event.target.value)}
          placeholder="search plugins…"
          style={{
            width: "100%",
            boxSizing: "border-box",
            background: "var(--panel2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            color: "var(--ink)",
            fontSize: 10,
            padding: "7px 10px",
          }}
        />
      </div>

      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          gap: 5,
          padding: "0 14px 10px",
        }}
      >
        {categories.map((category) => {
          const on = category === activeCategory;
          return (
            <button
              key={category}
              type="button"
              onClick={() => onCategory(category)}
              style={{
                background: on ? "var(--accent-soft)" : "transparent",
                border: `1px solid ${on ? "var(--accent)" : "var(--line)"}`,
                color: on ? "var(--accent)" : "var(--ink3)",
                fontSize: 9,
                padding: "3px 9px",
                borderRadius: 10,
                cursor: "pointer",
              }}
            >
              {category}
            </button>
          );
        })}
      </div>

      {catalog.length === 0 ? (
        <Empty>
          no plugin catalog. The control plane serves <code>GetPlugin</code> for
          a reference it is given, but has no list RPC yet — so there is nothing
          honest to put here until one exists.
        </Empty>
      ) : (
        <div style={{ padding: "0 8px" }}>
          {shown.map((entry) => (
            <div
              key={entry.ref}
              draggable
              onDragStart={(event) => {
                // The canvas reads this on drop. A plugin reference is the
                // whole payload: the step it becomes is built by the plane
                // from the plugin's own schema, not guessed here.
                event.dataTransfer.setData(
                  "application/dhole-plugin",
                  entry.ref,
                );
              }}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 9,
                padding: "7px 10px",
                fontSize: 10,
                color: "var(--ink2)",
                borderRadius: 4,
                cursor: "grab",
              }}
            >
              <PortGlyph type={entry.produces} />
              <span
                style={{
                  flex: 1,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}
              >
                {entry.name}
              </span>
              <span style={{ fontSize: 8, color: "var(--ink3)" }}>
                {entry.origin}
              </span>
            </div>
          ))}
          {shown.length === 0 && (
            <div
              style={{
                padding: "8px 10px",
                fontSize: 9,
                color: "var(--ink3)",
              }}
            >
              nothing matches “{query}”
            </div>
          )}
        </div>
      )}

      {/* Where the canvas portals its authoring controls. It is a host rather
          than a component because the controls are driven by the canvas's own
          edit state, and splitting the two would put a refused operation on a
          different screen from the button that caused it. */}
      <div id="dh-authoring" />

      <SectionLabel>PORT TYPES</SectionLabel>
      <div style={{ padding: "0 14px 4px" }}>
        {legend.map((type) => (
          <div
            key={type}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 9,
              padding: "4px 0",
              fontSize: 10,
              color: "var(--ink2)",
            }}
          >
            <PortGlyph type={type} />
            {type}
          </div>
        ))}
      </div>

      <SectionLabel>RUNS</SectionLabel>
      {runs.length === 0 ? (
        <Empty>
          no runs listed. A run is addressable — <code>WatchRun</code> streams
          one by id — but the contract has no ListRuns, so this stays empty
          rather than showing a list nobody can page.
        </Empty>
      ) : (
        <div style={{ padding: "0 8px 12px" }}>
          {runs.map((run) => (
            <button
              key={run.id}
              type="button"
              onClick={() => onOpenRun(run.id)}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 9,
                width: "100%",
                padding: "7px 10px",
                fontSize: 10,
                background: "transparent",
                border: "none",
                borderRadius: 4,
                color: "var(--ink2)",
                cursor: "pointer",
                textAlign: "left",
              }}
            >
              <span
                style={{
                  color:
                    run.outcome === "failed"
                      ? "var(--err)"
                      : run.outcome === "running"
                        ? "var(--accent)"
                        : "var(--ok)",
                }}
              >
                {run.outcome === "failed"
                  ? "✗"
                  : run.outcome === "running"
                    ? "▶"
                    : "✓"}
              </span>
              <span style={{ flex: 1 }}>{run.label}</span>
              <span style={{ color: "var(--ink3)" }}>{run.detail}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
