/**
 * The fleet, along the bottom.
 *
 * Engines are here rather than behind a menu because where a step can run is a
 * property of the fleet, not of the pipeline: a step pinned to an engine that
 * has stopped heartbeating will queue rather than fail, and the only place
 * that is visible before somebody waits ten minutes is a line like this one.
 *
 * An engine with no heartbeat is drawn in the error colour AND labelled
 * "offline". The dot alone would be a colour-only signal, which is the same
 * mistake the port glyphs exist to avoid.
 */

/** EngineStatus is what the status bar needs to know about one engine. It is
 * deliberately not the API's Engine message: this bar shows a summary, and
 * binding it to the wire type would make every field the API gains a field
 * this component has an opinion about. */
export type EngineStatus = {
  readonly id: string;
  readonly label: string;
  readonly online: boolean;
  /** Drained engines are online and deliberately taking no work. Saying
   * "draining" rather than showing green stops somebody waiting for a step
   * that is never going to be picked up here. */
  readonly draining?: boolean;
};

export function StatusBar({
  engines,
  busConnected,
  runsToday,
  tenant,
}: {
  readonly engines: readonly EngineStatus[];
  readonly busConnected: boolean;
  readonly runsToday: number | null;
  readonly tenant: string;
}) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 14,
        padding: "0 14px",
        background: "var(--panel2)",
        borderTop: "1px solid var(--line2)",
        fontSize: 9,
        color: "var(--ink3)",
      }}
    >
      <span style={{ letterSpacing: 0.5 }}>ENGINES</span>
      {engines.length === 0 && <span>none registered</span>}
      {engines.map((engine) => {
        const token = !engine.online
          ? "var(--err)"
          : engine.draining === true
            ? "var(--warn)"
            : "var(--ok)";
        return (
          <span
            key={engine.id}
            style={{ display: "flex", alignItems: "center", gap: 5 }}
          >
            <span
              aria-hidden
              style={{
                width: 6,
                height: 6,
                borderRadius: "50%",
                background: token,
              }}
            />
            <span
              style={{ color: engine.online ? "var(--ink2)" : "var(--err)" }}
            >
              {engine.label}
            </span>
            {!engine.online && (
              <span style={{ color: "var(--err)" }}>· offline</span>
            )}
            {engine.online && engine.draining === true && (
              <span style={{ color: "var(--warn)" }}>· draining</span>
            )}
          </span>
        );
      })}

      <span style={{ flex: 1 }} />

      <span style={{ color: busConnected ? "var(--ink3)" : "var(--err)" }}>
        bus {busConnected ? "✓" : "unreachable"}
      </span>
      {runsToday !== null && <span>{runsToday} runs today</span>}
      <span>tenant: {tenant}</span>
    </div>
  );
}
