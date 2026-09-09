/**
 * Where the nodes go - computed, never stored.
 *
 * THIS FILE IS THE REASON THE DOCUMENT STAYS A PIPELINE. The moment a node's
 * position is written into the definition, two things break at once: two
 * people editing the same pipeline produce a merge conflict over pixels, and
 * a definition exported to YAML carries somebody's screen resolution into a
 * file that is supposed to describe work. So there is no position field in
 * dhole.v1.Pipeline, nothing here is ever sent to the control plane, and the
 * canvas asks this function again every time it draws.
 *
 * That only works if the answer is the same answer every time. A layout that
 * drifted between renders would flick nodes around on every keystroke, and a
 * layout that depended on the order the steps happened to arrive in would put
 * the same pipeline in two different shapes for two people - which is how
 * "just store the positions" starts to look reasonable. Both are ruled out
 * below: a node's level is its longest path from a source, which is a property
 * of the graph, and ties inside a level are broken by step id, which is a
 * property of the step. Neither can see the order of the arrays.
 */

/** LayoutStep is a step, narrowed to what a layout can see: its identity. */
export interface LayoutStep {
  readonly id: string;
}

/** LayoutEdge is a dependency, narrowed the same way. */
export interface LayoutEdge {
  readonly fromStep: string;
  readonly toStep: string;
}

/** NodePosition is where one node is drawn, in flow coordinates. */
export interface NodePosition {
  readonly x: number;
  readonly y: number;
}

/** NodePositions maps step id to position. It is a return value and never a
 * document: nothing persists it. */
export type NodePositions = ReadonlyMap<string, NodePosition>;

/** The grid. Wide enough for a node plus the edge leaving it, tall enough
 * that two siblings do not touch. */
export const columnWidth = 320;
export const rowHeight = 140;

/**
 * autoLayout places every step of a DAG in a column per dependency level.
 *
 * A step's level is the LONGEST path to it from a step with no incoming
 * edges, so an edge always points rightwards and a step never sits left of
 * something it consumes. Steps sharing a level are stacked in id order.
 *
 * Edges naming a step that is not in `steps` are ignored rather than refused:
 * this function draws what it is given, and a dangling reference is
 * dag.TypeCheck's diagnostic to make, not a reason to render nothing.
 *
 * A cycle cannot be levelled, and a pipeline is acyclic, but a canvas
 * mid-edit is not something to crash. Relaxation stops after as many rounds as
 * there are steps, which is one more than the longest possible acyclic path -
 * so an acyclic graph is always fully relaxed, and a cyclic one still
 * terminates with the same answer every time.
 */
export function autoLayout(
  steps: readonly LayoutStep[],
  edges: readonly LayoutEdge[],
): NodePositions {
  // Sorted first, and everything below reads these copies: the order of the
  // inputs is never observed, so nothing can come to depend on it by accident.
  const ids = [...new Set(steps.map((step) => step.id))].sort();
  const known = new Set(ids);
  const dependencies = edges
    .filter((edge) => known.has(edge.fromStep) && known.has(edge.toStep))
    .map((edge) => ({ from: edge.fromStep, to: edge.toStep }))
    .sort((a, b) => a.from.localeCompare(b.from) || a.to.localeCompare(b.to));

  const level = new Map<string, number>(ids.map((id) => [id, 0]));
  for (let round = 0; round < ids.length; round++) {
    let moved = false;
    for (const { from, to } of dependencies) {
      const wanted = (level.get(from) ?? 0) + 1;
      if (wanted > (level.get(to) ?? 0)) {
        level.set(to, wanted);
        moved = true;
      }
    }
    if (!moved) {
      break;
    }
  }

  const positions = new Map<string, NodePosition>();
  const filled = new Map<number, number>();
  for (const id of ids) {
    const column = level.get(id) ?? 0;
    const row = filled.get(column) ?? 0;
    filled.set(column, row + 1);
    positions.set(id, { x: column * columnWidth, y: row * rowHeight });
  }
  return positions;
}
