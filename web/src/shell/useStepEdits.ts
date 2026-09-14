/**
 * The chain of revisions the inspector's edits make.
 *
 * Every edit is applied against a base revision and returns a NEW one (ADR
 * 0013), so the next edit has to be based on THAT, not on the revision in the
 * URL — or an author declaring a secret and then the capability it needs is
 * told their own first edit is a conflict. The chain starts again whenever the
 * URL names a different revision.
 */
import { useCallback, useState } from "react";

import type {
  ApplyOperationResponse,
  Operation,
} from "../gen/dhole/v1/api_pb.js";
import type { Pipeline } from "../gen/dhole/v1/pipeline_pb.js";
import type { ToastTone } from "./panels/index.js";

export type ApplyAt = (
  baseRevision: string,
  operation: Operation,
) => Promise<ApplyOperationResponse>;

interface Chain {
  /** The revision in the URL the chain was started from. */
  readonly from: string;
  readonly head: string;
  readonly pipeline: Pipeline;
}

export function useStepEdits(
  revisionId: string,
  applyAt: ApplyAt,
  notify: (text: string, tone: ToastTone) => void,
) {
  const [chain, setChain] = useState<Chain | null>(null);
  const [busy, setBusy] = useState(false);

  const current = chain?.from === revisionId ? chain : null;
  const head = current?.head ?? revisionId;

  const apply = useCallback(
    async (operation: Operation) => {
      setBusy(true);
      try {
        const response = await applyAt(head, operation);
        if (
          response.revision !== undefined &&
          response.pipeline !== undefined
        ) {
          setChain({
            from: revisionId,
            head: response.revision.id,
            pipeline: response.pipeline,
          });
        }
        // The plane's own summary: what was applied is what the control plane
        // says was applied, not what this editor meant to send.
        for (const change of response.diff?.changes ?? []) {
          notify(change.summary, "ok");
        }
      } catch (cause) {
        notify(cause instanceof Error ? cause.message : String(cause), "error");
      } finally {
        setBusy(false);
      }
    },
    [applyAt, head, notify, revisionId],
  );

  return { head, pipeline: current?.pipeline ?? null, busy, apply };
}
