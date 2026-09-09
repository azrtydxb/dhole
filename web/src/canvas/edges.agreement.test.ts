/**
 * edges.ts and internal/dag.TypeCheck must agree, and this is what makes them.
 *
 * The canvas refuses a bad edge in the browser so the user learns at drop time
 * instead of at minute eight of a run (ADR 0001). That means one rule with two
 * implementations, and two implementations of one rule drift - quietly, and in
 * the direction that is hardest to notice, because a canvas that is slightly
 * too permissive looks fine right up until the run refuses to start.
 *
 * So this test does not transcribe the Go rules and check the transcription.
 * It RUNS dag.TypeCheck, through tools/typecheck-oracle, over the same
 * pipelines it hands to typeCheck(), and requires the diagnostics to be equal:
 * same order, same ports, same words. Edit typecheck.go and this goes red
 * until edges.ts is edited with it.
 *
 * It needs the Go toolchain, and it fails rather than skips without one: a
 * check that quietly disappears on the machine where it matters is not a
 * check. Every Dhole developer has Go - it is a Go repository.
 */
import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";

import { fromJson, type JsonObject } from "@bufbuild/protobuf";
import { describe, expect, it } from "vitest";

import { PipelineSchema } from "../gen/dhole/v1/pipeline_pb.js";
import { typeCheck, type Diagnostic } from "./edges.js";

/** A pipeline as JSON, which is what both sides are given. */
type PipelineJson = JsonObject;

const blob = { blob: {} };
const structured = (schemaId: string) => ({ structured: { schemaId } });

/**
 * Every case is a pipeline both implementations are asked about. They cover
 * each branch dag.TypeCheck has: a missing step at either end, a missing port
 * at either end, both directions of a kind mismatch, matching and mismatching
 * schema ids, an untyped port against a typed one and against another untyped
 * one, and a blob pair whose advisory media types differ - which must be
 * allowed, because a media type is not part of the type.
 */
const cases: Record<string, PipelineJson> = {
  "an empty pipeline": { id: "p" },
  "a pipeline with no edges": {
    id: "p",
    steps: [{ id: "a", outputs: [{ name: "out", type: blob }] }],
  },
  "blob to blob": {
    id: "p",
    steps: [
      { id: "a", outputs: [{ name: "out", type: blob }] },
      { id: "b", inputs: [{ name: "in", type: blob }] },
    ],
    edges: [{ fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" }],
  },
  "blob to blob with different media types": {
    id: "p",
    steps: [
      {
        id: "a",
        outputs: [
          { name: "out", type: { blob: { mediaType: "application/gzip" } } },
        ],
      },
      {
        id: "b",
        inputs: [{ name: "in", type: { blob: { mediaType: "text/plain" } } }],
      },
    ],
    edges: [{ fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" }],
  },
  "blob to structured": {
    id: "p",
    steps: [
      { id: "a", outputs: [{ name: "out", type: blob }] },
      { id: "b", inputs: [{ name: "in", type: structured("s://report") }] },
    ],
    edges: [{ fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" }],
  },
  "structured to blob": {
    id: "p",
    steps: [
      { id: "a", outputs: [{ name: "out", type: structured("s://report") }] },
      { id: "b", inputs: [{ name: "in", type: blob }] },
    ],
    edges: [{ fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" }],
  },
  "structured to the same schema": {
    id: "p",
    steps: [
      { id: "a", outputs: [{ name: "out", type: structured("s://report") }] },
      { id: "b", inputs: [{ name: "in", type: structured("s://report") }] },
    ],
    edges: [{ fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" }],
  },
  "structured to another schema": {
    id: "p",
    steps: [
      { id: "a", outputs: [{ name: "out", type: structured("s://report") }] },
      { id: "b", inputs: [{ name: "in", type: structured("s://summary") }] },
    ],
    edges: [{ fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" }],
  },
  "untyped to untyped": {
    id: "p",
    steps: [
      { id: "a", outputs: [{ name: "out" }] },
      { id: "b", inputs: [{ name: "in" }] },
    ],
    edges: [{ fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" }],
  },
  "untyped to blob": {
    id: "p",
    steps: [
      { id: "a", outputs: [{ name: "out" }] },
      { id: "b", inputs: [{ name: "in", type: blob }] },
    ],
    edges: [{ fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" }],
  },
  "a missing source step": {
    id: "p",
    steps: [{ id: "b", inputs: [{ name: "in", type: blob }] }],
    edges: [{ fromStep: "ghost", fromPort: "out", toStep: "b", toPort: "in" }],
  },
  "a missing target step": {
    id: "p",
    steps: [{ id: "a", outputs: [{ name: "out", type: blob }] }],
    edges: [{ fromStep: "a", fromPort: "out", toStep: "ghost", toPort: "in" }],
  },
  "a missing output port": {
    id: "p",
    steps: [
      { id: "a", outputs: [{ name: "other", type: blob }] },
      { id: "b", inputs: [{ name: "in", type: blob }] },
    ],
    edges: [{ fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" }],
  },
  "a missing input port": {
    id: "p",
    steps: [
      { id: "a", outputs: [{ name: "out", type: blob }] },
      { id: "b", inputs: [{ name: "other", type: blob }] },
    ],
    edges: [{ fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" }],
  },
  "a step with no ports at all": {
    id: "p",
    steps: [{ id: "a" }, { id: "b" }],
    edges: [{ fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" }],
  },
  "several bad edges at once": {
    id: "p",
    steps: [
      { id: "a", outputs: [{ name: "out", type: blob }] },
      {
        id: "b",
        inputs: [
          { name: "in", type: structured("s://report") },
          { name: "raw", type: blob },
        ],
      },
    ],
    edges: [
      { fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" },
      { fromStep: "a", fromPort: "out", toStep: "b", toPort: "raw" },
      { fromStep: "a", fromPort: "gone", toStep: "b", toPort: "raw" },
    ],
  },
};

/** ask runs the real dag.TypeCheck over every case, in one process. */
function ask(pipelines: PipelineJson[]): Diagnostic[][] {
  const web = fileURLToPath(new URL("../..", import.meta.url));
  const stdout = execFileSync("go", ["run", "./tools/typecheck-oracle"], {
    cwd: web,
    input: JSON.stringify(pipelines),
    encoding: "utf8",
    // Compiling the oracle on a cold module cache is the slow part, once.
    timeout: 300_000,
  });
  return JSON.parse(stdout) as Diagnostic[][];
}

describe("edges.ts against internal/dag.TypeCheck", () => {
  const names = Object.keys(cases);
  const pipelines = names.map((name) => cases[name] as PipelineJson);
  const fromGo = ask(pipelines);

  it("was asked about every case", () => {
    // Guards the guard: a comparison against an empty answer list would
    // agree with anything.
    expect(fromGo).toHaveLength(names.length);
    expect(fromGo.flat().length).toBeGreaterThan(0);
  });

  it.each(names)("agrees about %s", (name) => {
    const index = names.indexOf(name);
    const pipeline = fromJson(PipelineSchema, cases[name] as PipelineJson);
    expect(typeCheck(pipeline)).toEqual(fromGo[index]);
  });
});
