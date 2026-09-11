/**
 * The fleet, in full — the one panel here that is backed by the plane.
 *
 * The status bar shows a dot per engine because that is all that fits; this is
 * where an operator finds out WHY a step is queuing. Capabilities, slots and
 * protocol version are on the row because each of them silently changes what
 * can be scheduled: a step needing `oci` will sit forever behind a fleet of
 * process engines, and an engine speaking only version N-1 will refuse a
 * dispatch whose fields it has never heard of.
 *
 * Draining is a button rather than a menu item because it is the action an
 * operator comes here to take, and it is labelled with what it does NOT do —
 * it stops new work, it kills nothing. An operator who believes drain is a
 * kill switch will use it during an incident and then wait for a step that is
 * still happily running.
 *
 * Everything on this panel comes from ListEngines, so it carries no sample
 * marker. If it is empty, the fleet is empty — that is a finding, not a
 * missing fixture, and it says so.
 */
import { Modal, ModalButton } from "./Modal.js";

/** FleetEngine is the editor's view of one `dhole.v1.Engine`. It is a view
 * model rather than the wire message on purpose: the row wants a rendered
 * activity line and a boolean for "is this thing alive", and binding straight
 * to the proto would make every field the contract gains a field this table
 * has to have an opinion about. */
export type FleetEngine = {
  readonly id: string;
  /** registering, ready or draining as the registry reports it; `offline` is
   * the editor's name for an instance whose heartbeat TTL has lapsed. */
  readonly state: "registering" | "ready" | "draining" | "offline";
  readonly capabilities: readonly string[];
  readonly os: string;
  readonly arch: string;
  readonly slots: number;
  /** Every protocol version the engine accepts. The highest is shown: that is
   * the one a dispatch will actually use. */
  readonly protocolVersions: readonly number[];
  /** How many steps its last heartbeat said it was holding. */
  readonly inFlight: number;
  /** Set when the heartbeat lapsed, e.g. "4m" — rendered, not computed here,
   * because a component that reads the clock re-renders to stay honest. */
  readonly lostFor?: string;
};

function stateToken(engine: FleetEngine): string {
  if (engine.state === "offline") return "var(--err)";
  if (engine.state === "draining") return "var(--warn)";
  if (engine.state === "registering") return "var(--ink3)";
  return "var(--ok)";
}

/** activity is the right-hand line: what this engine is doing to the run queue
 * right now, in the words the operator needs. */
function activity(engine: FleetEngine): { text: string; colour: string } {
  if (engine.state === "offline")
    return {
      text:
        engine.lostFor === undefined
          ? "heartbeat lost"
          : `heartbeat lost ${engine.lostFor} ago`,
      colour: "var(--err)",
    };
  if (engine.state === "registering")
    return { text: "registering", colour: "var(--ink3)" };
  if (engine.state === "draining")
    return {
      text:
        engine.inFlight === 0
          ? "draining · idle"
          : `draining · ${String(engine.inFlight)} finishing`,
      colour: "var(--warn)",
    };
  if (engine.inFlight === 0) return { text: "idle", colour: "var(--ink2)" };
  return {
    text: `${String(engine.inFlight)} step${engine.inFlight === 1 ? "" : "s"} running`,
    colour: "var(--ink2)",
  };
}

function meta(engine: FleetEngine): string {
  const proto = engine.protocolVersions.length
    ? `proto v${String(Math.max(...engine.protocolVersions))}`
    : "proto unknown";
  return [
    ...engine.capabilities,
    `${engine.os}/${engine.arch}`,
    `${String(engine.slots)} slots`,
    proto,
  ].join(" · ");
}

export function EnginesModal({
  engines,
  onDrain,
  onClose,
  busUrl,
}: {
  readonly engines: readonly FleetEngine[];
  /** Sends DrainEngine for one instance. The row does not optimistically
   * change state: the plane's answer is what the fleet actually is. */
  readonly onDrain: (id: string) => void;
  readonly onClose: () => void;
  /** The bus address an engine would dial, shown in the join command. Engines
   * connect outbound-only, so this is the single fact somebody setting one up
   * needs from this screen. */
  readonly busUrl: string;
}) {
  return (
    <Modal
      title="engines"
      subtitle="runtime registry · NATS KV · heartbeat TTL 15s"
      width={540}
      onClose={onClose}
      footer={
        <ModalButton kind="primary" onClick={onClose}>
          done
        </ModalButton>
      }
    >
      <div
        style={{
          padding: 16,
          display: "flex",
          flexDirection: "column",
          gap: 6,
          fontSize: 10,
          overflowY: "auto",
          minHeight: 0,
        }}
      >
        {engines.length === 0 && (
          <div
            style={{
              border: "1px solid var(--line2)",
              borderRadius: 6,
              padding: "9px 12px",
              color: "var(--ink3)",
            }}
          >
            no engines registered — every step will queue until one joins
          </div>
        )}

        {engines.map((engine) => {
          const { text, colour } = activity(engine);
          const offline = engine.state === "offline";
          return (
            <div
              key={engine.id}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                border: `1px solid ${offline ? "var(--err)" : "var(--line2)"}`,
                borderRadius: 6,
                padding: "9px 12px",
                minWidth: 0,
              }}
            >
              <span
                aria-hidden
                style={{
                  width: 7,
                  height: 7,
                  borderRadius: "50%",
                  background: stateToken(engine),
                  flex: "none",
                }}
              />
              <span style={{ fontWeight: 700, flex: "none" }}>{engine.id}</span>
              <span
                style={{
                  color: "var(--ink3)",
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                }}
              >
                {meta(engine)}
              </span>
              <span style={{ flex: 1 }} />
              <span style={{ color: colour, flex: "none" }}>{text}</span>
              {engine.state === "ready" && (
                <button
                  type="button"
                  onClick={() => {
                    onDrain(engine.id);
                  }}
                  title="stop new work reaching this engine; running steps finish"
                  style={{
                    flex: "none",
                    background: "none",
                    border: "1px solid var(--line)",
                    borderRadius: 3,
                    color: "var(--ink2)",
                    fontSize: 9,
                    padding: "1px 8px",
                    cursor: "pointer",
                  }}
                >
                  drain
                </button>
              )}
            </div>
          );
        })}

        <div
          style={{
            fontSize: 9,
            color: "var(--ink3)",
            letterSpacing: "0.1em",
            margin: "10px 0 2px",
          }}
        >
          CONNECT A NEW ENGINE
        </div>
        <div
          style={{
            border: "1px solid var(--line2)",
            borderRadius: 6,
            padding: "10px 12px",
            lineHeight: 1.7,
            color: "var(--ink2)",
          }}
        >
          Engines connect{" "}
          <span style={{ color: "var(--ink)" }}>outbound-only</span> — homelab,
          CGNAT, build Mac all work. Run this where the engine lives:
          <div
            className="dh-selectable"
            style={{
              background: "var(--panel2)",
              border: "1px solid var(--line)",
              borderRadius: 5,
              padding: "8px 10px",
              marginTop: 7,
              color: "var(--ink)",
              fontSize: 9,
              wordBreak: "break-all",
            }}
          >
            dhole engine join {busUrl} --token &lt;engine token&gt;
          </div>
          <div style={{ marginTop: 7, color: "var(--ink3)" }}>
            the token scopes the engine to a trust tier · issue and rotate it in
            settings → service tokens
          </div>
        </div>
      </div>
    </Modal>
  );
}
