/**
 * A step's log, as the viewer holds it.
 *
 * There are TWO copies of a step's log and they have different jobs
 * (docs/wire-contract.md). The LogChunk stream on `job.logs.<run>.<step>` is
 * live, ephemeral and explicitly droppable; the object the engine writes under
 * `JobStatus.log_key` is complete, durable, and what anyone reads after the
 * run. The stream endpoint tails the first while the step runs and then
 * announces the second with a `source` frame.
 *
 * That announcement is a RESET here, not an append. The live copy may be
 * missing chunks the viewer was told about with a `gap` frame; the stored copy
 * may not. Appending one to the other would produce a log that is neither: a
 * partial tail followed by a complete duplicate of itself. So on `source` the
 * text is cleared and refilled from the copy that just took over — which is
 * also, exactly, what a reader who arrives after the run gets on their first
 * frame.
 */
import type { SSEFrame } from "./sse.js";

/** LogSource is which copy the text currently shown came from. */
export type LogSource = "live" | "stored" | "none";

/** LogState is everything <LogStream> renders. */
export interface LogState {
  source: LogSource;
  /** Why there is no log at all, when source is "none". */
  reason: string;
  text: string;
  /** How many live chunks the server told us it had to drop. */
  dropped: number;
  /** True once the server closed the stream. */
  ended: boolean;
}

export const emptyLog = (): LogState => ({
  source: "none",
  reason: "",
  text: "",
  dropped: 0,
  ended: false,
});

interface SourcePayload {
  source?: string;
  reason?: string;
}

interface ChunkPayload {
  text?: string;
}

interface GapPayload {
  dropped?: number;
}

function decode<T>(frame: SSEFrame): T {
  if (frame.data === "") {
    return {} as T;
  }
  try {
    return JSON.parse(frame.data) as T;
  } catch {
    return {} as T;
  }
}

/** applyLogFrame folds one frame of the log stream into the state. */
export function applyLogFrame(state: LogState, frame: SSEFrame): LogState {
  switch (frame.event) {
    case "source": {
      const payload = decode<SourcePayload>(frame);
      const source = (payload.source ?? "none") as LogSource;
      // The reset. See the note at the top of this file.
      return {
        ...state,
        source,
        reason: payload.reason ?? "",
        text: "",
        dropped: 0,
      };
    }
    case "chunk":
    case "log": {
      const payload = decode<ChunkPayload>(frame);
      return { ...state, text: state.text + (payload.text ?? "") };
    }
    case "gap": {
      const payload = decode<GapPayload>(frame);
      return { ...state, dropped: state.dropped + (payload.dropped ?? 0) };
    }
    case "end": {
      const payload = decode<SourcePayload>(frame);
      return {
        ...state,
        ended: true,
        reason: payload.reason ?? state.reason,
      };
    }
    default:
      return state;
  }
}

/** logLines splits the text into the lines the view renders. */
export function logLines(state: LogState): string[] {
  if (state.text === "") {
    return [];
  }
  const lines = state.text.split("\n");
  if (lines.length > 0 && lines[lines.length - 1] === "") {
    lines.pop();
  }
  return lines;
}
