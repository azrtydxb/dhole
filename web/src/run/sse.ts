/**
 * An SSE client that can carry a credential, and that resumes where it left
 * off.
 *
 * WHY NOT `EventSource`. The browser's own implementation reconnects and sends
 * Last-Event-ID for free, and cannot set a single request header. This API has
 * no unauthenticated call and no cookie session — the bearer token is the only
 * credential — so EventSource would force either a token in the query string
 * (which lands in every proxy log and browser history) or an anonymous
 * endpoint. Neither is acceptable, so the reconnect and the resume are
 * implemented here instead. That is the whole cost of the choice, and it is
 * paid once.
 *
 * A resume is not a nicety: a stream that reconnects at the BEGINNING shows a
 * run's history twice, and one that reconnects at the END loses whatever
 * happened while it was away without saying so. Both look like a working
 * viewer.
 */

/** SSEFrame is one parsed `id:`/`event:`/`data:` block. */
export interface SSEFrame {
  id: string;
  event: string;
  data: string;
}

/** parseFrames splits whatever has arrived so far into complete frames.
 *
 * It returns the unconsumed tail as well: a chunk boundary falls wherever the
 * network decides, and a parser that assumed a chunk was a frame would drop
 * every frame that arrived split in two. */
export function parseFrames(buffer: string): {
  frames: SSEFrame[];
  rest: string;
} {
  const frames: SSEFrame[] = [];
  let rest = buffer;
  for (;;) {
    const end = rest.indexOf("\n\n");
    if (end === -1) {
      break;
    }
    const block = rest.slice(0, end);
    rest = rest.slice(end + 2);
    const frame: SSEFrame = { id: "", event: "", data: "" };
    for (const line of block.split("\n")) {
      if (line.startsWith("id:")) {
        frame.id = line.slice(3).trim();
      } else if (line.startsWith("event:")) {
        frame.event = line.slice(6).trim();
      } else if (line.startsWith("data:")) {
        frame.data += line.slice(5).trim();
      }
      // Anything else, a `:` comment above all, is ignored by design.
    }
    if (frame.event !== "") {
      frames.push(frame);
    }
  }
  return { frames, rest };
}

/** SubscribeOptions configures one stream. */
export interface SubscribeOptions {
  url: string;
  token: string;
  /** Called for every frame, in order. */
  onFrame: (frame: SSEFrame) => void;
  /** Called once the server closed the stream cleanly. */
  onEnd?: () => void;
  signal: AbortSignal;
  /** Injectable for tests; defaults to globalThis.fetch. */
  fetchImpl?: typeof fetch;
  /** How long to wait before reconnecting, in ms. */
  retryDelayMs?: number;
  /** How many times to reconnect before giving up. */
  maxRetries?: number;
  /** Called when the stream failed and will not be retried again. */
  onError?: (error: unknown) => void;
}

/**
 * subscribe follows an SSE endpoint until it ends, the caller aborts, or the
 * retries run out.
 *
 * The last id seen is remembered across reconnections and sent back as
 * Last-Event-ID, which is what makes a reconnect a resume rather than a replay
 * or a gap.
 */
export async function subscribe(options: SubscribeOptions): Promise<void> {
  const doFetch = options.fetchImpl ?? globalThis.fetch.bind(globalThis);
  const maxRetries = options.maxRetries ?? 5;
  let lastEventId = "";
  let attempt = 0;

  for (;;) {
    if (options.signal.aborted) {
      return;
    }
    try {
      const headers: Record<string, string> = {
        Accept: "text/event-stream",
        Authorization: `Bearer ${options.token}`,
      };
      if (lastEventId !== "") {
        headers["Last-Event-ID"] = lastEventId;
      }
      const res = await doFetch(options.url, {
        headers,
        signal: options.signal,
      });
      if (!res.ok || res.body === null) {
        throw new Error(`stream ${options.url}: HTTP ${res.status}`);
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      let ended = false;
      for (;;) {
        const { done, value } = await reader.read();
        if (done) {
          break;
        }
        buffer += decoder.decode(value, { stream: true });
        const parsed = parseFrames(buffer);
        buffer = parsed.rest;
        for (const frame of parsed.frames) {
          if (frame.id !== "") {
            lastEventId = frame.id;
          }
          options.onFrame(frame);
          if (frame.event === "end") {
            ended = true;
          }
        }
        if (ended) {
          break;
        }
      }
      if (ended) {
        options.onEnd?.();
        return;
      }
      // The body ended without an `end` frame: the connection died. Resume.
      attempt += 1;
    } catch (error) {
      if (options.signal.aborted) {
        return;
      }
      attempt += 1;
      if (attempt > maxRetries) {
        // Failing visibly beats a viewer that silently stopped updating.
        options.onError?.(error);
        return;
      }
    }
    if (attempt > maxRetries) {
      options.onError?.(
        new Error(`stream ${options.url}: gave up after ${maxRetries} retries`),
      );
      return;
    }
    await new Promise((resolve) =>
      setTimeout(resolve, options.retryDelayMs ?? 500),
    );
  }
}
