/**
 * The inspector's edits are a chain of revisions, not a list of edits to one.
 *
 * Every ApplyOperation returns a NEW revision (ADR 0013). The break this
 * catches is the easy one to write: an inspector that applies every edit
 * against the revision in the URL. The first edit lands; the second is based
 * on a revision that is no longer the head, and the author is told their own
 * previous edit is a conflict. Declaring a secret and then the capability it
 * needs is exactly that two-edit sequence.
 */
// @vitest-environment jsdom
import { create } from "@bufbuild/protobuf";
import { act, cleanup, renderHook } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import {
  ApplyOperationResponseSchema,
  ChangeSchema,
  DiffSchema,
  RevisionSchema,
  type ApplyOperationResponse,
  type Operation,
} from "../gen/dhole/v1/api_pb.js";
import { Capability } from "../gen/dhole/v1/common_pb.js";
import { PipelineSchema } from "../gen/dhole/v1/pipeline_pb.js";
import { capabilityOperation, secretOperation } from "./StepDeclarations.js";
import { useStepEdits } from "./useStepEdits.js";

afterEach(cleanup);

function answer(revision: string, summary: string): ApplyOperationResponse {
  return create(ApplyOperationResponseSchema, {
    revision: create(RevisionSchema, { id: revision }),
    pipeline: create(PipelineSchema, { id: "ci" }),
    diff: create(DiffSchema, {
      changes: [create(ChangeSchema, { summary })],
    }),
  });
}

describe("useStepEdits", () => {
  it("bases each edit on the revision the previous one produced", async () => {
    const apply = vi
      .fn<(base: string, op: Operation) => Promise<ApplyOperationResponse>>()
      .mockResolvedValueOnce(answer("rev_2", "bound NEXUS_PASSWORD"))
      .mockResolvedValueOnce(answer("rev_3", "declared CAPABILITY_SECRETS"));
    const notify = vi.fn();
    const { result } = renderHook(() => useStepEdits("rev_1", apply, notify));

    await act(() =>
      result.current.apply(secretOperation("image", "NEXUS_PASSWORD", "n")),
    );
    await act(() =>
      result.current.apply(
        capabilityOperation("image", Capability.SECRETS, false),
      ),
    );

    expect(apply.mock.calls.map((call) => call[0])).toEqual(["rev_1", "rev_2"]);
    expect(result.current.head).toBe("rev_3");
    expect(result.current.pipeline?.id).toBe("ci");
    expect(notify).toHaveBeenCalledWith("bound NEXUS_PASSWORD", "ok");
  });

  it("starts again from a revision the URL moves to", async () => {
    const apply = vi
      .fn<(base: string, op: Operation) => Promise<ApplyOperationResponse>>()
      .mockResolvedValue(answer("rev_2", "x"));
    const { result, rerender } = renderHook(
      ({ revision }) => useStepEdits(revision, apply, vi.fn()),
      { initialProps: { revision: "rev_1" } },
    );
    await act(() =>
      result.current.apply(secretOperation("image", "TOKEN", "t")),
    );
    rerender({ revision: "rev_9" });
    expect(result.current.head).toBe("rev_9");
    expect(result.current.pipeline).toBeNull();
  });

  it("keeps its head and says why when the plane refuses an edit", async () => {
    const apply = vi
      .fn<(base: string, op: Operation) => Promise<ApplyOperationResponse>>()
      .mockRejectedValue(new Error("set_step_secret: step binds no secret"));
    const notify = vi.fn();
    const { result } = renderHook(() => useStepEdits("rev_1", apply, notify));
    await act(() =>
      result.current.apply(secretOperation("image", "TOKEN", "", true)),
    );
    expect(result.current.head).toBe("rev_1");
    expect(result.current.busy).toBe(false);
    expect(notify).toHaveBeenCalledWith(
      "set_step_secret: step binds no secret",
      "error",
    );
  });
});
