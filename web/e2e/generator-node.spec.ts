/**
 * The generator node, in the editor and in the run view.
 *
 * One property, two halves, and the whole point is that they DISAGREE about
 * what is on screen:
 *
 *  1. In the EDITOR a generator is opaque. The authored graph cannot know
 *     what the generator will emit — that is decided at runtime, from a
 *     directory listing or an API — so a canvas that drew any steps inside it
 *     would be drawing a guess. It says "expands at runtime" and stops.
 *  2. In the RUN VIEW the realised steps are there, by name, because the run
 *     log holds the fragment that was actually realised (ADR 0003).
 *
 * The authored half is end to end: the step is added to a real pipeline
 * through the real ApplyOperation, and the editor draws what GetPipeline
 * hands back. The realised half reads e2e/testdata/generator-record.json,
 * which is the payload internal/dynamic writes into the run log verbatim —
 * internal/dynamic's own tests fail if that file stops being a record the Go
 * code can produce and decode, so the two languages cannot drift apart
 * quietly. It is a fixture rather than a live run because no generator step
 * runs on the plane yet: Task 55 produces the step type, and the scheduler
 * that dispatches one is not wired.
 */
import { readFileSync } from "node:fs";

import {
  expect,
  test,
  type APIRequestContext,
  type Page,
} from "@playwright/test";

import {
  apiUrl,
  bootstrapToken,
  createPipeline,
  type Seeded,
} from "./plane.js";

/** The plugin ref a generator node carries — internal/dynamic.PluginRef. */
const generatorPluginRef = "builtin:generator";
const generatorStep = "fan-out";

/** The realised record, exactly as internal/dynamic wrote it into a run log. */
function realisedRecord(): string {
  const path = new URL("./testdata/generator-record.json", import.meta.url);
  return readFileSync(path, "utf8").trim();
}

/** The step ids that record realised, read out of the record itself rather
 * than retyped: a test that hard-codes them cannot notice the fixture and the
 * page disagreeing. */
function realisedStepIDs(): string[] {
  return (JSON.parse(realisedRecord()) as { steps?: string[] }).steps ?? [];
}

/** apply sends one operation through the real contract and returns the new
 * revision, so the next edit has something to base itself on. */
async function apply(
  request: APIRequestContext,
  pipelineId: string,
  baseRevision: string,
  operation: unknown,
): Promise<string> {
  const response = await request.post(
    `${apiUrl}/dhole.v1.PipelineService/ApplyOperation`,
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${bootstrapToken()}`,
      },
      data: { pipelineId, baseRevision, operation },
    },
  );
  expect(response.ok(), await response.text()).toBe(true);
  const body = (await response.json()) as { revision?: { id?: string } };
  const id = body.revision?.id;
  expect(id).toBeTruthy();
  return id ?? "";
}

/** seedGenerator authors a generator step on a real pipeline: one blob output
 * for the fragment to hang off, and the builtin plugin ref that says what kind
 * of node it is. */
async function seedGenerator(request: APIRequestContext): Promise<Seeded> {
  const seed = await createPipeline(request);
  let revision = await apply(request, seed.pipelineId, seed.revisionId, {
    addStep: {
      step: {
        id: generatorStep,
        outputs: [{ name: "shard", type: { blob: {} } }],
      },
    },
  });
  revision = await apply(request, seed.pipelineId, revision, {
    setProperty: {
      stepId: generatorStep,
      property: "plugin_ref",
      value: generatorPluginRef,
    },
  });
  return { pipelineId: seed.pipelineId, revisionId: revision };
}

/** openGenerator mounts the generator node's harness on the running app,
 * signed in with the plane's own bootstrap credential. */
async function openGenerator(page: Page, seed: Seeded): Promise<void> {
  await page.addInitScript((value: string) => {
    window.localStorage.setItem("dhole.token", value);
  }, bootstrapToken());
  await page.goto(
    `/?generator.pipeline=${encodeURIComponent(seed.pipelineId)}` +
      `&generator.revision=${encodeURIComponent(seed.revisionId)}` +
      `&generator.step=${encodeURIComponent(generatorStep)}` +
      `&generator.record=${encodeURIComponent(realisedRecord())}`,
  );
  await page.addScriptTag({
    url: "/src/canvas/GeneratorNode.harness.tsx",
    type: "module",
  });
}

test("the editor draws a generator as opaque and the run view draws what it realised", async ({
  page,
  request,
}) => {
  const seed = await seedGenerator(request);
  const realised = realisedStepIDs();
  expect(realised.length).toBeGreaterThan(1);

  await openGenerator(page, seed);

  // The editor. Opaque, and honest about why.
  const authored = page.getByTestId(`generator-node-${generatorStep}`);
  await expect(authored).toBeVisible();
  await expect(authored).toContainText("expands at runtime");
  await expect(authored).toHaveAttribute("data-opaque", "true");
  // The authored node draws the step's declared ports — it is a step like any
  // other — and NOTHING the generator will emit.
  await expect(
    page.getByTestId(`generator-port-out-${generatorStep}-shard`),
  ).toBeVisible();
  for (const id of realised) {
    await expect(authored).not.toContainText(id);
  }

  // The run view. The realised steps, by name, out of the recorded fragment.
  const run = page.getByTestId(`realised-generator-${generatorStep}`);
  await expect(run).toBeVisible();
  await expect(run).not.toContainText("expands at runtime");
  const steps = page.getByTestId("realised-step");
  await expect(steps).toHaveCount(realised.length);
  for (const id of realised) {
    await expect(
      page.locator(`[data-testid="realised-step"][data-step-id="${id}"]`),
    ).toBeVisible();
  }
});
