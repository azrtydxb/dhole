/**
 * The assistant rail: a conversation whose output is OPERATIONS, not text.
 *
 * The point of this panel is the strip of operations under an assistant reply.
 * An agent edits this pipeline through the same operation API a person does —
 * add_step, connect, set_property — and each of those comes back with its
 * exact inverse (ADR 0020). So the assistant never applies anything: it
 * proposes a list you can read in full, and APPLY is a separate act by a
 * person. An assistant that edited the canvas as it talked would be an agent
 * with write access and no review, which is the one thing this design refuses.
 *
 * THE SAMPLE MARKER IS LOAD-BEARING. There is no assistant endpoint in the
 * wire contract yet. A chat pane that answers convincingly with nothing behind
 * it is the worst possible artefact — somebody trusts a reply that no system
 * produced — so while `sample` is true the panel wears a chip in its own
 * chrome that says so, and it cannot be styled away by a caller. When a real
 * endpoint lands the caller passes `sample={false}` and the chip disappears;
 * nothing else about the panel changes.
 */
import { useDialogChrome } from "./dialog.js";

/** AssistantOp is one proposed operation, named the way the API names it.
 * `summary` is the sentence the proposer wrote — this panel renders it and
 * composes none of it, because a browser that described an operation in its
 * own words would be describing something other than what APPLY sends. */
export type AssistantOp = {
  /** The operation name: add_step, connect, set_property, remove_edge… */
  readonly op: string;
  readonly summary: string;
};

/** AssistantMessage is one turn. `ops` present and `applied` false is the only
 * state that offers a button: an op set that has already been applied is a
 * record, and offering APPLY on it again invites a duplicate edit. */
export type AssistantMessage = {
  readonly id: string;
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly ops?: readonly AssistantOp[];
  readonly applied?: boolean;
  /** Dismissed op sets keep their list and lose their buttons. */
  readonly dismissed?: boolean;
};

/** opTag is the one-glyph verdict in front of an operation: what it does to
 * the pipeline, before you read which element it does it to. */
function opTag(op: string): string {
  if (op === "add_step" || op === "connect") return `+ ${op}`;
  if (op === "set_property") return "~ set_prop";
  if (op.startsWith("remove_") || op.startsWith("disconnect")) return `- ${op}`;
  return `~ ${op}`;
}

export function AssistantPanel({
  messages,
  busy,
  input,
  onInput,
  onSend,
  onClose,
  onApplyOps,
  onDismissOps,
  sample = true,
  scope = "agent:claude · scoped: pipeline.edit",
}: {
  readonly messages: readonly AssistantMessage[];
  readonly busy: boolean;
  readonly input: string;
  readonly onInput: (value: string) => void;
  readonly onSend: (text: string) => void;
  readonly onClose: () => void;
  readonly onApplyOps: (ops: readonly AssistantOp[], messageId: string) => void;
  /** Optional: without it an op set has APPLY and no way to put it away, which
   * is fine for a caller that closes the panel instead. */
  readonly onDismissOps?: (messageId: string) => void;
  readonly sample?: boolean;
  /** What the agent is allowed to do, from its token. */
  readonly scope?: string;
}) {
  // No focus trap: this is a rail beside the canvas, not a surface over it,
  // and a rail you cannot Tab out of is a rail you are stuck in.
  const ref = useDialogChrome<HTMLDivElement>(onClose, false);

  const send = () => {
    const text = input.trim();
    // Busy is not a disabled button here: the design keeps the composer live
    // so a follow-up can be typed while a reply is in flight, and refuses only
    // the send. Two prompts against one pipeline would produce two op sets
    // proposed against the same revision.
    if (text === "" || busy) return;
    onSend(text);
    // The box empties on send because the prompt is now in the transcript
    // above it; leaving it filled is how the same edit gets proposed twice.
    onInput("");
  };

  return (
    <div
      ref={ref}
      tabIndex={-1}
      role="complementary"
      aria-label="pipeline assistant"
      style={{
        height: "100%",
        minHeight: 0,
        background: "var(--panel)",
        borderLeft: "1px solid var(--line2)",
        display: "flex",
        flexDirection: "column",
        outline: "none",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "10px 13px",
          borderBottom: "1px solid var(--line2)",
        }}
      >
        <span
          aria-hidden
          style={{
            width: 7,
            height: 7,
            flex: "none",
            borderRadius: "50%",
            background: "var(--accent)",
            animation: "dh-pulse 2s infinite",
          }}
        />
        <span style={{ fontSize: 11, fontWeight: 700 }}>assistant</span>
        {sample && (
          <span
            data-testid="assistant-sample-marker"
            title="this pane has no endpoint behind it — the replies are sample data"
            style={{
              flex: "none",
              fontSize: 8,
              color: "var(--warn)",
              border: "1px solid var(--warn)",
              borderRadius: 3,
              padding: "1px 6px",
              whiteSpace: "nowrap",
            }}
          >
            sample · no endpoint
          </span>
        )}
        <span
          // The scope loses its tail before the sample chip does: what the
          // agent is allowed to do is context, "nothing is behind this" is a
          // warning, and a 290px rail cannot hold both at full length.
          title={scope}
          style={{
            fontSize: 8,
            color: "var(--ink3)",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {scope}
        </span>
        <button
          type="button"
          onClick={onClose}
          aria-label="close assistant"
          style={{
            marginLeft: "auto",
            background: "none",
            border: "none",
            color: "var(--ink3)",
            fontSize: 12,
            cursor: "pointer",
          }}
        >
          ✕
        </button>
      </div>

      <div
        style={{
          flex: 1,
          minHeight: 0,
          overflowY: "auto",
          padding: 12,
          display: "flex",
          flexDirection: "column",
          gap: 10,
        }}
      >
        {messages.map((message) => {
          const mine = message.role === "user";
          const ops = message.ops ?? [];
          const pending =
            ops.length > 0 &&
            message.applied !== true &&
            message.dismissed !== true;
          return (
            <div
              key={message.id}
              style={{
                display: "flex",
                flexDirection: "column",
                gap: 4,
                alignItems: mine ? "flex-end" : "flex-start",
              }}
            >
              <div
                className="dh-selectable"
                style={{
                  maxWidth: "92%",
                  fontSize: 10,
                  lineHeight: 1.6,
                  padding: "8px 11px",
                  borderRadius: 8,
                  background: mine ? "var(--accent-soft)" : "var(--panel2)",
                  border: `1px solid ${mine ? "var(--accent)" : "var(--line2)"}`,
                  color: "var(--ink)",
                  whiteSpace: "pre-wrap",
                }}
              >
                {message.text}
              </div>

              {ops.length > 0 && (
                <div
                  data-testid="assistant-ops"
                  style={{
                    maxWidth: "92%",
                    width: "100%",
                    border: "1px solid var(--accent)",
                    borderRadius: 8,
                    overflow: "hidden",
                  }}
                >
                  {ops.map((op, index) => (
                    <div
                      key={`${index}-${op.op}-${op.summary}`}
                      style={{
                        display: "flex",
                        gap: 8,
                        padding: "6px 10px",
                        fontSize: 9,
                        borderBottom: "1px solid var(--line2)",
                      }}
                    >
                      <span style={{ color: "var(--ok)", flex: "none" }}>
                        {opTag(op.op)}
                      </span>
                      <span
                        className="dh-selectable"
                        style={{ color: "var(--ink2)" }}
                      >
                        {op.summary}
                      </span>
                    </div>
                  ))}

                  {pending && (
                    <div
                      style={{ display: "flex", gap: 6, padding: "8px 10px" }}
                    >
                      <button
                        type="button"
                        onClick={() => onApplyOps(ops, message.id)}
                        style={{
                          flex: 1,
                          background: "var(--ok)",
                          border: "none",
                          borderRadius: 4,
                          // On-accent text, as Toolbar's primary button does:
                          // no palette token is white in either theme.
                          color: "#fff",
                          fontSize: 9,
                          fontWeight: 700,
                          padding: "5px 0",
                          cursor: "pointer",
                        }}
                      >
                        APPLY
                      </button>
                      {onDismissOps !== undefined && (
                        <button
                          type="button"
                          onClick={() => onDismissOps(message.id)}
                          style={{
                            flex: 1,
                            background: "none",
                            border: "1px solid var(--line)",
                            borderRadius: 4,
                            color: "var(--ink2)",
                            fontSize: 9,
                            padding: "5px 0",
                            cursor: "pointer",
                          }}
                        >
                          dismiss
                        </button>
                      )}
                    </div>
                  )}

                  {message.applied === true && (
                    <div
                      style={{
                        padding: "6px 10px",
                        fontSize: 9,
                        color: "var(--ok)",
                      }}
                    >
                      ✓ applied · diff + inverse recorded
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}

        {busy && (
          <div role="status" style={{ fontSize: 9, color: "var(--ink3)" }}>
            thinking
            <span aria-hidden style={{ animation: "dh-pulse 1s infinite" }}>
              …
            </span>
          </div>
        )}
      </div>

      <div
        style={{
          display: "flex",
          gap: 6,
          padding: 10,
          borderTop: "1px solid var(--line2)",
        }}
      >
        <input
          value={input}
          aria-label="ask the assistant"
          placeholder="add an sbom step after the build…"
          onChange={(event) => onInput(event.currentTarget.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") send();
          }}
          style={{
            flex: 1,
            minWidth: 0,
            background: "var(--panel2)",
            border: "1px solid var(--line)",
            borderRadius: 5,
            color: "var(--ink)",
            fontSize: 10,
            padding: "7px 9px",
            outline: "none",
          }}
        />
        <button
          type="button"
          onClick={send}
          aria-label="send"
          style={{
            background: "var(--accent)",
            border: "none",
            borderRadius: 5,
            color: "#fff",
            fontSize: 10,
            fontWeight: 700,
            padding: "0 13px",
            cursor: "pointer",
          }}
        >
          →
        </button>
      </div>
    </div>
  );
}
