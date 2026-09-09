/**
 * The client half of the two-copy rule, and the resume.
 *
 * The live copy and the stored copy differ in every case here, on purpose: a
 * test whose two copies say the same thing cannot tell a viewer that switched
 * from one that never did.
 */
import { describe, expect, it, vi } from "vitest";

import { applyLogFrame, emptyLog, logLines } from "./logs.js";
import { parseFrames, subscribe, type SSEFrame } from "./sse.js";

const frame = (event: string, data: unknown): SSEFrame => ({
  id: "",
  event,
  data: JSON.stringify(data),
});

describe("the log a viewer holds", () => {
  it("replaces the live tail with the authoritative copy when the server switches", () => {
    let state = emptyLog();
    state = applyLogFrame(state, frame("source", { source: "live" }));
    state = applyLogFrame(
      state,
      frame("chunk", { seq: 1, text: "live tail, possibly gappy\n" }),
    );
    expect(state.source).toEqual("live");

    state = applyLogFrame(state, frame("source", { source: "stored" }));
    state = applyLogFrame(
      state,
      frame("log", { text: "authoritative one\nauthoritative two\n" }),
    );
    state = applyLogFrame(state, frame("end", { source: "stored" }));

    expect(state.source).toEqual("stored");
    // Not appended to the live tail: the stored copy IS the log, and a viewer
    // showing both shows a log that never existed.
    expect(state.text).not.toContain("live tail");
    expect(logLines(state)).toEqual(["authoritative one", "authoritative two"]);
    expect(state.ended).toBe(true);
  });

  it("records that live chunks were dropped rather than hiding the gap", () => {
    let state = emptyLog();
    state = applyLogFrame(state, frame("source", { source: "live" }));
    state = applyLogFrame(state, frame("gap", { dropped: 12 }));
    state = applyLogFrame(state, frame("chunk", { text: "after the gap\n" }));
    expect(state.dropped).toEqual(12);
  });

  it("keeps the server's reason when there is no log at all", () => {
    let state = emptyLog();
    state = applyLogFrame(
      state,
      frame("source", {
        source: "none",
        reason: "the authoritative log is not in the object store",
      }),
    );
    expect(state.source).toEqual("none");
    expect(state.reason).toContain("not in the object store");
  });
});

describe("the SSE parser", () => {
  it("holds a frame that arrived split across two chunks", () => {
    const first = parseFrames('event: chunk\ndata: {"text":"half');
    expect(first.frames).toHaveLength(0);
    const second = parseFrames(first.rest + '"}\n\n');
    expect(second.frames).toHaveLength(1);
    expect(second.frames[0]!.data).toEqual('{"text":"half"}');
  });

  it("ignores comments and keeps ids", () => {
    const { frames } = parseFrames(
      ": keep-alive\n\nid: 7\nevent: STEP_READY\ndata: {}\n\n",
    );
    expect(frames).toHaveLength(1);
    expect(frames[0]!.id).toEqual("7");
  });
});

describe("subscribe", () => {
  function bodyOf(text: string): ReadableStream<Uint8Array> {
    return new ReadableStream({
      start(controller) {
        controller.enqueue(new TextEncoder().encode(text));
        controller.close();
      },
    });
  }

  it("resumes from the last id it saw instead of replaying or skipping", async () => {
    const seen: (string | null)[] = [];
    const bodies = [
      // A stream that dies without an `end` frame: the connection dropped.
      "id: 3\nevent: STEP_READY\ndata: {}\n\n",
      "id: 9\nevent: RUN_COMPLETED\ndata: {}\n\nevent: end\ndata: {}\n\n",
    ];
    const fetchImpl = vi.fn((_url: string, init?: RequestInit) => {
      const headers = new Headers(init?.headers);
      seen.push(headers.get("Last-Event-ID"));
      return Promise.resolve(
        new Response(bodyOf(bodies.shift() ?? ""), { status: 200 }),
      );
    });

    const frames: SSEFrame[] = [];
    await subscribe({
      url: "/v1/runs/run_1/events",
      token: "tok",
      signal: new AbortController().signal,
      onFrame: (f) => frames.push(f),
      fetchImpl: fetchImpl as unknown as typeof fetch,
      retryDelayMs: 0,
    });

    // First attempt carries no resume position; the reconnection carries the
    // id of the last frame that actually arrived.
    expect(seen).toEqual([null, "3"]);
    expect(frames.map((f) => f.event)).toEqual([
      "STEP_READY",
      "RUN_COMPLETED",
      "end",
    ]);
  });

  it("sends the bearer token, because this API has no unauthenticated call", async () => {
    let authorization: string | null = null;
    const fetchImpl = vi.fn((_url: string, init?: RequestInit) => {
      authorization = new Headers(init?.headers).get("Authorization");
      return Promise.resolve(
        new Response(bodyOf("event: end\ndata: {}\n\n"), { status: 200 }),
      );
    });
    await subscribe({
      url: "/v1/runs/run_1/events",
      token: "tok-alice",
      signal: new AbortController().signal,
      onFrame: () => undefined,
      fetchImpl: fetchImpl as unknown as typeof fetch,
      retryDelayMs: 0,
    });
    expect(authorization).toEqual("Bearer tok-alice");
  });

  it("gives up visibly rather than pretending to still be connected", async () => {
    const fetchImpl = vi.fn(() =>
      Promise.resolve(new Response("nope", { status: 500 })),
    );
    let failure: unknown = null;
    await subscribe({
      url: "/v1/runs/run_1/events",
      token: "tok",
      signal: new AbortController().signal,
      onFrame: () => undefined,
      onError: (error) => {
        failure = error;
      },
      fetchImpl: fetchImpl,
      retryDelayMs: 0,
      maxRetries: 2,
    });
    expect(failure).not.toBeNull();
    expect(fetchImpl).toHaveBeenCalledTimes(3);
  });
});
