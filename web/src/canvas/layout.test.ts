/**
 * autoLayout's contract is a single sentence: the same graph gives the same
 * positions, whatever order it arrives in. These tests exist because the
 * cheapest way to make a layout "stable" is to store it in the document, and
 * the only thing standing between this project and that mistake is a layout
 * function nobody has a reason to distrust.
 *
 * The browser cannot check this. A Playwright test sees one arrival order and
 * one render, so it would pass against a layout that quietly depended on both.
 */
import { describe, expect, it } from "vitest";

import {
  autoLayout,
  columnWidth,
  rowHeight,
  type LayoutEdge,
  type LayoutStep,
} from "./layout.js";

/** A diamond with a tail: two levels of fan-out and a join, which is where an
 * order-dependent layout gives itself away. */
const steps: LayoutStep[] = [
  { id: "fetch" },
  { id: "build" },
  { id: "test" },
  { id: "package" },
  { id: "publish" },
  { id: "orphan" },
];

const edges: LayoutEdge[] = [
  { fromStep: "fetch", toStep: "build" },
  { fromStep: "fetch", toStep: "test" },
  { fromStep: "build", toStep: "package" },
  { fromStep: "test", toStep: "package" },
  { fromStep: "package", toStep: "publish" },
];

function plain(
  steps: LayoutStep[],
  edges: LayoutEdge[],
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [id, at] of autoLayout(steps, edges)) {
    out[id] = `${at.x},${at.y}`;
  }
  return out;
}

/** every permutation of an array, so "any order" is tested as written rather
 * than as a couple of shuffles that happened to work. */
function permutations<T>(items: readonly T[]): T[][] {
  if (items.length <= 1) {
    return [[...items]];
  }
  const out: T[][] = [];
  for (let i = 0; i < items.length; i++) {
    const head = items[i] as T;
    const rest = [...items.slice(0, i), ...items.slice(i + 1)];
    for (const tail of permutations(rest)) {
      out.push([head, ...tail]);
    }
  }
  return out;
}

describe("autoLayout", () => {
  it("gives the same positions for the same graph, in any order", () => {
    const expected = plain(steps, edges);

    // Every order of the steps (720 of them), against a fixed edge list.
    for (const order of permutations(steps)) {
      expect(plain(order, edges)).toEqual(expected);
    }
    // Every order of the edges (120), against a fixed step list.
    for (const order of permutations(edges)) {
      expect(plain(steps, order)).toEqual(expected);
    }
    // And reversed on both axes at once, which is the case a layout that
    // seeds itself from the first element gets wrong.
    expect(plain([...steps].reverse(), [...edges].reverse())).toEqual(expected);
  });

  it("gives the same positions on every call", () => {
    const first = plain(steps, edges);
    for (let again = 0; again < 5; again++) {
      expect(plain(steps, edges)).toEqual(first);
    }
  });

  it("puts a step in a later column than everything it consumes", () => {
    const positions = autoLayout(steps, edges);
    for (const edge of edges) {
      const from = positions.get(edge.fromStep);
      const to = positions.get(edge.toStep);
      expect(from).toBeDefined();
      expect(to).toBeDefined();
      expect(from?.x ?? 0).toBeLessThan(to?.x ?? 0);
    }
  });

  it("levels a join by its longest path, not its first", () => {
    // a -> b -> c and a -> c: c belongs in column 2, behind b, or the edge
    // a->c would run backwards through the node it feeds.
    const positions = autoLayout(
      [{ id: "a" }, { id: "b" }, { id: "c" }],
      [
        { fromStep: "a", toStep: "b" },
        { fromStep: "b", toStep: "c" },
        { fromStep: "a", toStep: "c" },
      ],
    );
    expect(positions.get("c")?.x).toBe(2 * columnWidth);
  });

  it("stacks the steps sharing a level in id order", () => {
    const positions = autoLayout(
      [{ id: "zeta" }, { id: "alpha" }, { id: "mu" }],
      [],
    );
    expect(positions.get("alpha")).toEqual({ x: 0, y: 0 });
    expect(positions.get("mu")).toEqual({ x: 0, y: rowHeight });
    expect(positions.get("zeta")).toEqual({ x: 0, y: 2 * rowHeight });
  });

  it("lays out an empty pipeline and a step with no edges", () => {
    expect([...autoLayout([], [])]).toEqual([]);
    expect(autoLayout([{ id: "lonely" }], []).get("lonely")).toEqual({
      x: 0,
      y: 0,
    });
  });

  it("ignores an edge naming a step it was not given", () => {
    const positions = autoLayout(
      [{ id: "a" }],
      [{ fromStep: "ghost", toStep: "a" }],
    );
    expect(positions.get("a")).toEqual({ x: 0, y: 0 });
    expect(positions.size).toBe(1);
  });

  it("terminates on a cycle and still answers the same way twice", () => {
    const cyclic: LayoutEdge[] = [
      { fromStep: "a", toStep: "b" },
      { fromStep: "b", toStep: "a" },
    ];
    const first = plain([{ id: "a" }, { id: "b" }], cyclic);
    expect(plain([{ id: "b" }, { id: "a" }], [...cyclic].reverse())).toEqual(
      first,
    );
  });
});
