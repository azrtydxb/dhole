/**
 * The other people in this pipeline, and what to do when one of them got there
 * first.
 *
 * PRESENCE IS NOT STATE THIS COMPONENT OWNS. It is a stream: WatchPresence
 * carries who else is editing, and every entry it draws was put there by an
 * event and is removed by another one. Nothing is persisted, nothing is
 * fetched, and a peer who stops refreshing their announcement is dropped by the
 * server, not by a timer here — a browser tab that computed its own idea of who
 * was still around would keep drawing cursors for people whose laptops had
 * closed.
 *
 * THE REBASE PROMPT IS A REFUSAL, NOT A RETRY. When the plane answers an
 * operation with ABORTED, the edit did not happen and this canvas must not make
 * it happen: re-sending it against the newer revision is exactly the silent
 * overwrite base_revision exists to prevent (ADR 0013). So the prompt says what
 * the pipeline moved to and offers to move this editor onto it — a READ — and
 * the operation stays undone until its author decides to make it again.
 */
import { Code, ConnectError } from "@connectrpc/connect";
import { useCallback, useEffect, useRef, useState } from "react";

import { pipelineClient } from "../api/client.js";
import { RevisionSchema, type Revision } from "../gen/dhole/v1/api_pb.js";

/** PresenceProps names the pipeline and what this editor has selected. */
export interface PresenceProps {
  readonly pipelineId: string;
  readonly selection: string;
}

/** A peer, as this canvas currently draws them. */
interface Peer {
  readonly principal: string;
  readonly selection: string;
  readonly x: number;
  readonly y: number;
}

/** How often a moved pointer is announced. */
const cursorIntervalMs = 250;

/** newSession mints this tab's session id. One person in two tabs is two
 * editors with two cursors, so this is per mount rather than per principal. */
function newSession(): string {
  const random = globalThis.crypto?.randomUUID?.();
  return random ?? `tab-${Math.random().toString(36).slice(2)}`;
}

/**
 * Presence draws the other editors of one pipeline and announces this one.
 */
export function Presence({ pipelineId, selection }: PresenceProps) {
  const [peers, setPeers] = useState<Record<string, Peer>>({});
  // The session this editor is announced under belongs to the STREAM rather
  // than to this component: a stream that is torn down and reopened — React's
  // development double-mount does exactly that — announces its own departure
  // when it ends, and a session shared between the old stream and the new one
  // would be withdrawn a moment after being announced.
  const session = useRef("");
  // What to announce when a stream opens, kept in refs so that changing them
  // does not reopen the stream.
  const selected = useRef(selection);
  const pointer = useRef({ x: 0, y: 0 });

  useEffect(() => {
    const mine = newSession();
    session.current = mine;
    const controller = new AbortController();
    void (async () => {
      try {
        const stream = pipelineClient.watchPresence(
          {
            pipelineId,
            sessionId: mine,
            selection: selected.current,
            cursor: {
              $typeName: "dhole.v1.Cursor",
              x: pointer.current.x,
              y: pointer.current.y,
            },
          },
          { signal: controller.signal },
        );
        for await (const response of stream) {
          const event = response.event;
          if (event === undefined || event.sessionId === "") {
            continue;
          }
          setPeers((current) => {
            const next = { ...current };
            if (event.gone) {
              delete next[event.sessionId];
            } else {
              next[event.sessionId] = {
                principal: event.principal,
                selection: event.selection,
                x: event.cursor?.x ?? 0,
                y: event.cursor?.y ?? 0,
              };
            }
            return next;
          });
        }
      } catch {
        // A stream ends when this editor leaves, and that is how the others
        // are told. Presence is best-effort by construction: the canvas keeps
        // working without it, so there is nothing to report here.
      }
    })();
    // Ending the stream IS the departure — the server announces it — so there
    // is no message to send from a tab that may already be closing. A tab that
    // never gets even this far is covered by the announcement expiring.
    return () => controller.abort();
  }, [pipelineId]);

  // A change of selection, announced on the stream's session. The server
  // refreshes the last announcement for as long as that stream is open, so
  // there is no timer here: this says what changed and nothing else.
  useEffect(() => {
    selected.current = selection;
    if (session.current === "") {
      return;
    }
    void pipelineClient
      .updatePresence({ pipelineId, sessionId: session.current, selection })
      .catch(() => {
        // The next announcement, or the expiry, puts it right.
      });
  }, [pipelineId, selection]);

  // The pointer, throttled. A cursor is worth seeing and not worth a request
  // per pixel, so this sends at most one announcement per interval, and only
  // when the pointer actually moved.
  useEffect(() => {
    let pending: { x: number; y: number } | null = null;
    const onMove = (event: PointerEvent) => {
      pending = { x: event.clientX, y: event.clientY };
    };
    const timer = setInterval(() => {
      const at = pending;
      if (
        at === null ||
        session.current === "" ||
        (at.x === pointer.current.x && at.y === pointer.current.y)
      ) {
        return;
      }
      pointer.current = at;
      void pipelineClient
        .updatePresence({
          pipelineId,
          sessionId: session.current,
          selection: selected.current,
          cursor: { $typeName: "dhole.v1.Cursor", x: at.x, y: at.y },
        })
        .catch(() => {});
    }, cursorIntervalMs);
    globalThis.addEventListener?.("pointermove", onMove);
    return () => {
      globalThis.removeEventListener?.("pointermove", onMove);
      clearInterval(timer);
    };
  }, [pipelineId]);

  const others = Object.entries(peers);
  return (
    <section data-testid="presence">
      <h3>also editing</h3>
      {others.length === 0 && (
        <p style={{ color: "#718096" }}>nobody else is here</p>
      )}
      <ul data-testid="presence-list">
        {others.map(([id, peer]) => (
          <li
            key={id}
            data-testid={
              peer.selection === ""
                ? `presence-${id}`
                : `presence-selection-${peer.selection}`
            }
          >
            <span aria-hidden>●</span> {peer.principal}
            {peer.selection === "" ? " is here" : ` has ${peer.selection}`}
          </li>
        ))}
      </ul>
      {/* The remote cursors themselves, where their owners last said they
          were. They are decoration over the page and must never take a click
          meant for the canvas underneath. */}
      {others.map(([id, peer]) =>
        peer.x === 0 && peer.y === 0 ? null : (
          <div
            key={`cursor-${id}`}
            data-testid={`presence-cursor-${id}`}
            aria-hidden
            style={{
              position: "fixed",
              left: peer.x,
              top: peer.y,
              pointerEvents: "none",
              color: "#3182ce",
              fontSize: 12,
              zIndex: 10,
            }}
          >
            ▸ {peer.principal}
          </div>
        ),
      )}
    </section>
  );
}

/**
 * rebaseNotice reads a refused edit and reports the revision to rebase onto.
 *
 * The revision is a DETAIL of the error rather than a string in its message,
 * so this is a decode rather than a parse: a client that scraped the sentence
 * would break the day the sentence changed, and would have no way to tell a
 * conflict from any other aborted call.
 */
export function rebaseNotice(error: unknown): Revision | null {
  const connectError = ConnectError.from(error);
  if (connectError.code !== Code.Aborted) {
    return null;
  }
  const details = connectError.findDetails(RevisionSchema);
  return details[0] ?? null;
}

/** RebasePromptProps names the revision the pipeline moved to. */
export interface RebasePromptProps {
  readonly revision: Revision;
  readonly onRebase: (revisionId: string) => void;
}

/**
 * RebasePrompt is what a losing editor sees: what happened, what the pipeline
 * is at now, and one way forward that is a read rather than a re-send.
 */
export function RebasePrompt({ revision, onRebase }: RebasePromptProps) {
  const rebase = useCallback(
    () => onRebase(revision.id),
    [onRebase, revision.id],
  );
  return (
    <div data-testid="rebase-prompt" role="alert" style={{ color: "#975a16" }}>
      <p>
        Somebody else edited this pipeline first. Your change was NOT applied.
        The pipeline is now at <code>{revision.id}</code>.
      </p>
      <button data-testid="rebase" type="button" onClick={rebase}>
        move to that revision
      </button>
    </div>
  );
}
