/**
 * The right panel: everything about the selected step, and the run's logs.
 *
 * Properties and logs are TABS rather than a split, because they answer
 * questions at different times — you edit a step before a run and read its log
 * during one — and a split would give each half too little room to be useful.
 *
 * The effect class sits near the top, above the parameters, because it decides
 * what the scheduler is allowed to do with the step: PURE may be cached and
 * retried freely, IDEMPOTENT may be retried but never cached, AT_MOST_ONCE may
 * be neither (ADR 0002). Bury it under a parameter list and somebody will pick
 * it by accident and wonder why their deploy ran twice.
 */
import { PortGlyph, type PortTypeName } from "../design/PortGlyph.js";

/** EffectClass is the wire's three, in the spelling the editor shows. */
export type EffectClass = "pure" | "idempotent" | "at-most-once";

export type InspectorStep = {
  readonly id: string;
  readonly name: string;
  readonly pluginRef: string;
  readonly effect: EffectClass;
  readonly engine: string;
  readonly produces: PortTypeName;
  /** Whether the plane resolved the plugin reference to a digest. A tag that
   * was never resolved is the difference between a reproducible step and one
   * that will drift, so it is stated rather than implied. */
  readonly resolvedDigest?: string;
};

const effects: readonly EffectClass[] = ["pure", "idempotent", "at-most-once"];

const effectExplains: Record<EffectClass, string> = {
  pure: "cacheable · retried freely · no observable effect beyond its outputs",
  idempotent:
    "never cached · retried with the same idempotency key · repeating it is indistinguishable from running it once",
  "at-most-once":
    "never cached · never auto-retried · a failure waits for a person to replay it",
};

export function Label({ children }: { readonly children: React.ReactNode }) {
  return (
    <div
      style={{
        fontSize: 9,
        letterSpacing: 0.6,
        color: "var(--ink3)",
        margin: "16px 0 7px",
      }}
    >
      {children}
    </div>
  );
}

export function Inspector({
  tab,
  onTab,
  step,
  onEffect,
  children,
  declarations,
  logs,
}: {
  readonly tab: "properties" | "logs";
  readonly onTab: (tab: "properties" | "logs") => void;
  readonly step: InspectorStep | null;
  readonly onEffect: (effect: EffectClass) => void;
  /** The parameter form. It is passed in rather than built here because the
   * fields come from the plugin's own schema and that form already exists. */
  readonly children?: React.ReactNode;
  /** The step's capabilities and secret bindings, each edited by an operation
   * of its own (ADR 0028). A slot for the same reason the parameters are: the
   * inspector draws, and whoever holds the revision applies. */
  readonly declarations?: React.ReactNode;
  readonly logs?: React.ReactNode;
}) {
  return (
    <div
      style={{
        borderLeft: "1px solid var(--line2)",
        background: "var(--panel)",
        display: "flex",
        flexDirection: "column",
        minHeight: 0,
      }}
    >
      <div style={{ display: "flex", flex: "none" }}>
        {(["properties", "logs"] as const).map((name) => (
          <button
            key={name}
            type="button"
            onClick={() => onTab(name)}
            style={{
              flex: 1,
              background: "transparent",
              border: "none",
              borderBottom: `2px solid ${
                tab === name ? "var(--accent)" : "var(--line2)"
              }`,
              color: tab === name ? "var(--ink)" : "var(--ink3)",
              fontSize: 11,
              padding: "12px 0",
              cursor: "pointer",
            }}
          >
            {name}
          </button>
        ))}
      </div>

      <div style={{ overflowY: "auto", padding: "0 18px 20px", minHeight: 0 }}>
        {tab === "logs" ? (
          (logs ?? (
            <div
              style={{
                fontSize: 10,
                color: "var(--ink3)",
                paddingTop: 18,
                lineHeight: 1.6,
              }}
            >
              no run attached. Open a run to stream its log here.
            </div>
          ))
        ) : step === null ? (
          <div
            style={{
              fontSize: 10,
              color: "var(--ink3)",
              paddingTop: 18,
              lineHeight: 1.6,
            }}
          >
            nothing selected — click a step on the canvas.
          </div>
        ) : (
          <>
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 8,
                paddingTop: 16,
              }}
            >
              <PortGlyph type={step.produces} size={10} />
              <span style={{ fontSize: 14, fontWeight: 700 }}>{step.name}</span>
            </div>

            <div
              className="dh-selectable"
              style={{
                fontSize: 9,
                color: "var(--ink3)",
                lineHeight: 1.6,
                marginTop: 7,
                wordBreak: "break-all",
              }}
            >
              {/* Clamped, with the whole thing on hover and still selectable.
                  A `command:` reference carries an entire shell script, and
                  left unclamped it pushed the effect class — the field that
                  decides whether this step may be retried — off the screen. */}
              <span
                title={step.pluginRef}
                style={{
                  display: "-webkit-box",
                  WebkitLineClamp: 3,
                  WebkitBoxOrient: "vertical",
                  overflow: "hidden",
                }}
              >
                plugin: {step.pluginRef === "" ? "—" : step.pluginRef}
              </span>
              {step.resolvedDigest === undefined ? (
                <div style={{ marginTop: 4 }}>
                  <span style={{ color: "var(--warn)" }}>
                    unresolved — a reference that is not pinned to a digest has
                    no stable identity and is refused by cache
                  </span>
                </div>
              ) : (
                <div style={{ marginTop: 4 }}>
                  resolved{" "}
                  <span style={{ color: "var(--ok)" }}>
                    {step.resolvedDigest}
                  </span>
                </div>
              )}
            </div>

            <Label>EFFECT CLASS</Label>
            <div style={{ display: "flex", gap: 6 }}>
              {effects.map((effect) => {
                const on = effect === step.effect;
                // at-most-once is drawn in the error colour when chosen. It is
                // not an error — it is the one that cannot be retried, and the
                // colour is the reminder.
                const token =
                  effect === "at-most-once" ? "var(--err)" : "var(--accent)";
                return (
                  <button
                    key={effect}
                    type="button"
                    onClick={() => onEffect(effect)}
                    style={{
                      fontSize: 9,
                      padding: "5px 9px",
                      borderRadius: 4,
                      border: `1px solid ${on ? token : "var(--line2)"}`,
                      background: on ? "var(--accent-soft)" : "transparent",
                      color: on ? token : "var(--ink3)",
                      cursor: "pointer",
                    }}
                  >
                    {effect}
                  </button>
                );
              })}
            </div>
            <div
              style={{
                fontSize: 9,
                color: "var(--ink3)",
                lineHeight: 1.6,
                marginTop: 8,
              }}
            >
              {effectExplains[step.effect]}
            </div>

            <Label>ENGINE</Label>
            <div
              style={{
                background: "var(--panel2)",
                border: "1px solid var(--line)",
                borderRadius: 5,
                padding: "9px 11px",
                fontSize: 10,
                color: "var(--ink2)",
              }}
            >
              {step.engine === "" ? "any · auto-assigned" : step.engine}
            </div>

            {declarations}

            <Label>PARAMETERS · from plugin schema</Label>
            {children ?? (
              <div style={{ fontSize: 9, color: "var(--ink3)" }}>
                this plugin declares no parameters
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
