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
 * Every measurement here — 9px labels at 0.1em, 10px rows in a 1px box with a
 * 5px radius, 12px gutters — is the design's. They are small on purpose: this
 * rail is read at a glance beside a graph, not studied.
 */
import { PortGlyph, type PortTypeName } from "../design/PortGlyph.js";

export type CatalogEntry = {
  readonly ref: string;
  readonly name: string;
  readonly category: string;
  /** Where the plugin came from — `core`, an upstream's name. Shown small and
   * last, because it decides trust and nothing else. */
  readonly origin: string;
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

const labelStyle: React.CSSProperties = {
  fontSize: 9,
  color: "var(--ink3)",
  letterSpacing: "0.1em",
  marginBottom: 7,
};

/** Sample marks a pane the control plane cannot answer for.
 *
 * It is permanent and sits in the pane's own chrome, because sample rows that
 * look like real ones are worse than an empty list: nobody re-checks a list
 * that appears to have been answered. */
function Sample() {
  return (
    <span
      title="no RPC behind this pane — these rows are examples, not the plane's answer"
      style={{
        fontSize: 8,
        letterSpacing: 0,
        color: "var(--warn)",
        border: "1px solid var(--warn)",
        borderRadius: 3,
        padding: "0 4px",
        marginLeft: 6,
        whiteSpace: "nowrap",
      }}
    >
      sample · no endpoint
    </span>
  );
}

export function Sidebar({
  catalog,
  catalogIsSample,
  categories,
  activeCategory,
  onCategory,
  query,
  onQuery,
  onAdd,
  onBrowseRegistry,
  runs,
  runsAreSample,
  onOpenRun,
}: {
  readonly catalog: readonly CatalogEntry[];
  readonly catalogIsSample: boolean;
  readonly categories: readonly string[];
  readonly activeCategory: string;
  readonly onCategory: (category: string) => void;
  readonly query: string;
  readonly onQuery: (query: string) => void;
  readonly onAdd: (entry: CatalogEntry) => void;
  readonly onBrowseRegistry: () => void;
  readonly runs: readonly RunSummary[];
  readonly runsAreSample: boolean;
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
        display: "flex",
        flexDirection: "column",
        minHeight: 0,
        overflow: "hidden",
      }}
    >
      <div
        style={{
          padding: 12,
          display: "flex",
          flexDirection: "column",
          minHeight: 0,
          flex: "1 1 auto",
        }}
      >
        <div style={labelStyle}>
          CATALOG{" "}
          <span style={{ letterSpacing: 0 }}>· {catalog.length} plugins</span>
          {catalogIsSample && <Sample />}
        </div>

        <input
          value={query}
          onChange={(event) => onQuery(event.target.value)}
          placeholder="search plugins…  ⌘K"
          style={{
            width: "100%",
            boxSizing: "border-box",
            background: "var(--panel2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            color: "var(--ink)",
            fontSize: 10,
            padding: "6px 9px",
            outline: "none",
            marginBottom: 8,
          }}
        />

        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            gap: 4,
            marginBottom: 10,
          }}
        >
          {categories.map((category) => {
            const on = category === activeCategory;
            return (
              <span
                key={category}
                role="button"
                tabIndex={0}
                onClick={() => onCategory(category)}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ")
                    onCategory(category);
                }}
                style={{
                  fontSize: 9,
                  padding: "3px 8px",
                  borderRadius: 10,
                  border: `1px solid ${on ? "var(--accent)" : "var(--line2)"}`,
                  color: on ? "var(--accent)" : "var(--ink3)",
                  background: on ? "var(--accent-soft)" : "transparent",
                  cursor: "pointer",
                }}
              >
                {category}
              </span>
            );
          })}
        </div>

        <div
          style={{
            flex: 1,
            minHeight: 110,
            overflowY: "auto",
            display: "flex",
            flexDirection: "column",
            gap: 4,
            paddingBottom: 8,
            borderBottom: "1px solid var(--line2)",
          }}
        >
          {shown.map((entry) => (
            <div
              key={entry.ref}
              role="button"
              tabIndex={0}
              title={entry.ref}
              onClick={() => onAdd(entry)}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") onAdd(entry);
              }}
              style={{
                flex: "none",
                display: "flex",
                alignItems: "center",
                gap: 8,
                padding: "5px 8px",
                border: "1px solid var(--line2)",
                borderRadius: 5,
                fontSize: 10,
                color: "var(--ink2)",
                cursor: "pointer",
              }}
            >
              <PortGlyph type={entry.produces} />
              <span
                style={{
                  flex: 1,
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                }}
              >
                {entry.name}
              </span>
              <span style={{ fontSize: 8, color: "var(--ink3)", flex: "none" }}>
                {entry.origin}
              </span>
            </div>
          ))}
          {shown.length === 0 && (
            <div
              style={{
                fontSize: 9,
                color: "var(--ink3)",
                textAlign: "center",
                padding: "14px 0",
              }}
            >
              {catalog.length === 0
                ? "no catalog · the plane serves GetPlugin but has no list RPC"
                : "no match · federated upstreams searchable via ⌘K"}
            </div>
          )}
        </div>
      </div>

      <div style={{ flex: "0 1 auto", overflowY: "auto", padding: 12 }}>
        <div
          role="button"
          tabIndex={0}
          onClick={onBrowseRegistry}
          onKeyDown={(event) => {
            if (event.key === "Enter" || event.key === " ") onBrowseRegistry();
          }}
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            gap: 7,
            padding: "6px 8px",
            marginBottom: 12,
            border: "1px dashed var(--accent)",
            borderRadius: 5,
            fontSize: 10,
            color: "var(--accent)",
            cursor: "pointer",
          }}
        >
          ⇣ browse plugin registry
        </div>

        <div style={labelStyle}>PORT TYPES</div>
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 5,
            fontSize: 9,
            color: "var(--ink2)",
            marginBottom: 16,
          }}
        >
          {legend.map((type) => (
            <div
              key={type}
              style={{ display: "flex", alignItems: "center", gap: 8 }}
            >
              <PortGlyph type={type} />
              {type}
            </div>
          ))}
        </div>

        <div style={labelStyle}>
          RUNS
          {runsAreSample && <Sample />}
        </div>
        <div
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 4,
            fontSize: 9,
          }}
        >
          {runs.length === 0 && (
            <div style={{ color: "var(--ink3)", padding: "4px 0" }}>
              no runs · the contract has no ListRuns
            </div>
          )}
          {runs.map((run) => (
            <div
              key={run.id}
              role="button"
              tabIndex={0}
              onClick={() => onOpenRun(run.id)}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ")
                  onOpenRun(run.id);
              }}
              style={{
                display: "flex",
                justifyContent: "space-between",
                padding: "4px 8px",
                border: "1px solid var(--line2)",
                borderRadius: 5,
                color: "var(--ink2)",
                cursor: "pointer",
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
                    : "✓"}{" "}
                {run.label}
              </span>
              <span>{run.detail}</span>
            </div>
          ))}
        </div>

        {/* Where the canvas portals its authoring controls. It is a host rather
            than a component because the controls are driven by the canvas's own
            edit state, and splitting the two would put a refused operation on a
            different screen from the button that caused it. */}
        <div id="dh-authoring" />
      </div>
    </div>
  );
}
