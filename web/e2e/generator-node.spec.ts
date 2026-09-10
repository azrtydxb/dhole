/**
 * The generator, drawn twice by the real application, and the two drawings
 * deliberately disagree about what is on screen.
 *
 *  1. In the EDITOR a generator is opaque. The authored graph cannot know what
 *     the generator will emit — that is decided at runtime, from a directory
 *     listing or an API — so a canvas that drew any steps inside it would be
 *     drawing a guess, and the first time the generator emitted a different
 *     number the picture would be a lie nobody could see. It says "expands at
 *     runtime", draws the ports the step actually declares, and stops.
 *  2. In the RUN VIEW the realised steps are there, by name, because the run
 *     log holds the fragment that was realised (ADR 0003). Nothing is
 *     recomputed from the definition, which never contained these steps.
 *
 * Both halves are the shipping screens: `?pipeline=…&revision=…` is the
 * canvas, `#/runs/…` is the run view. This used to mount a harness module
 * instead, because neither screen knew what a generator was; they do now.
 *
 * The authored half is end to end through the real ApplyOperation. The
 * realised half is a run SEEDED with a fragment, because no generator step is
 * dispatched by the plane yet — the scheduler splices what is in the log, and
 * the step type that writes it there is not wired to an executor. The record
 * is not invented by the seeder either: internal/dynamic writes it, against
 * this pipeline, and refuses a fragment that does not fit where it is spliced.
 */
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
  seedUrl,
  type Seeded,
} from "./plane.js";

/** The plugin ref a generator node carries — internal/dynamic.PluginRef. */
const generatorPluginRef = "builtin:generator";
const generatorStep = "fan-out";

/** The steps the seeded generator emits. They are named here and asserted
 * everywhere else off what came back from the plane, so a fragment that
 * realised something else fails rather than passing quietly. */
const shards = ["shard-a", "shard-b", "shard-c"];

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

/** realiseFragment seeds a run of this pipeline whose log holds the fragment
 * the generator emitted, and answers with the run to open. */
async function realiseFragment(
  request: APIRequestContext,
  seed: Seeded,
): Promise<{ runId: string; steps: string[] }> {
  const query = new URLSearchParams({
    pipeline: seed.pipelineId,
    revision: seed.revisionId,
    step: generatorStep,
  });
  for (const id of shards) {
    query.append("shard", id);
  }
  const response = await request.post(`${seedUrl}/generator-run?${query}`);
  expect(
    response.ok(),
    `seeding a realised fragment: ${response.status()} ${await response.text()}`,
  ).toBe(true);
  return (await response.json()) as { runId: string; steps: string[] };
}

/** signIn stores the plane's own bootstrap credential, which is what the app
 * reads. A spec that invented a token would be testing its own fake. */
async function signIn(page: Page): Promise<void> {
  await page.addInitScript((value: string) => {
    window.localStorage.setItem("dhole.token", value);
  }, bootstrapToken());
}

test("the editor draws a generator as opaque and the run view draws what it realised", async ({
  page,
  request,
}) => {
  const seed = await seedGenerator(request);
  const realised = await realiseFragment(request, seed);
  expect(realised.steps).toEqual(shards);

  await signIn(page);

  // The editor: the canvas of the real application, on the real revision.
  await page.goto(
    `/?pipeline=${encodeURIComponent(seed.pipelineId)}` +
      `&revision=${encodeURIComponent(seed.revisionId)}`,
  );
  const authored = page.getByTestId(`generator-node-${generatorStep}`);
  await expect(authored).toBeVisible();
  await expect(authored).toContainText("expands at runtime");
  await expect(authored).toHaveAttribute("data-opaque", "true");
  // The authored node draws the step's declared ports — it is a step like any
  // other — and NOTHING the generator will emit.
  await expect(
    page.getByTestId(`generator-port-out-${generatorStep}-shard`),
  ).toBeVisible();
  for (const id of realised.steps) {
    await expect(authored).not.toContainText(id);
    // Nor anywhere else on the canvas: the realised steps are not in this
    // definition and there is nothing for them to be drawn as.
    await expect(page.getByTestId(`step-node-${id}`)).toHaveCount(0);
  }
  // And it is NOT drawn as an ordinary step: the whole point is that the two
  // are told apart on sight.
  await expect(page.getByTestId(`step-node-${generatorStep}`)).toHaveCount(0);

  // The run view: the same generator, in a run that expanded it.
  await page.goto(`/#/runs/${encodeURIComponent(realised.runId)}`);
  const run = page.getByTestId(`realised-generator-${generatorStep}`);
  await expect(run).toBeVisible();
  await expect(run).not.toContainText("expands at runtime");
  const steps = page.getByTestId("realised-step");
  await expect(steps).toHaveCount(realised.steps.length);
  for (const id of realised.steps) {
    await expect(
      page.locator(`[data-testid="realised-step"][data-step-id="${id}"]`),
    ).toBeVisible();
  }
});
