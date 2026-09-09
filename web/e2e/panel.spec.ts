/**
 * The property panel, against a real control plane.
 *
 * The property under test is not "the form works". It is that the FORM IS THE
 * PLUGIN'S SCHEMA. A plugin declares what it takes; the editor renders that
 * declaration. The moment a field exists because a React file names it, the
 * schema has stopped being the truth and every new plugin needs a web release
 * — so the first test publishes a plugin with a field no web file has ever
 * heard of and requires it on screen.
 *
 * Where the schema comes from: the CATALOG, through GetPlugin. The step names
 * a published plugin and carries no inline schema of its own, so a panel that
 * still read the definition would render nothing at all here. Until that RPC
 * existed the browser could not reach catalog.Manifest.InputSchema and had to
 * render whatever copy the definition happened to carry.
 */
import {
  expect,
  test,
  type APIRequestContext,
  type Page,
} from "@playwright/test";

import {
  bootstrapToken,
  createPipeline,
  rpc,
  seedUrl,
  type Seeded,
} from "./plane.js";

/** A plugin's input schema, as the plugin author wrote it. Nothing in web/
 * knows these field names; they arrive from the control plane. */
const deployV1 = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  $id: "https://example.test/plugins/deploy/1.json",
  type: "object",
  title: "deploy",
  properties: {
    plugin_ref: {
      type: "string",
      title: "plugin",
      pattern: "^acme/",
    },
    command: { type: "string", title: "command", default: "deploy" },
  },
  required: ["plugin_ref"],
};

/** Version two of the same plugin: one added integer field, one field whose
 * schema this editor cannot render, and nothing else. */
const deployV2 = {
  ...deployV1,
  $id: "https://example.test/plugins/deploy/2.json",
  properties: {
    ...deployV1.properties,
    retries: { type: "integer", title: "retries", minimum: 0, default: 0 },
    targets: { type: "array", title: "targets", items: { type: "string" } },
  },
};

/**
 * publishPlugin puts a manifest in the plane's catalog and returns its ref.
 *
 * Through the seeder, and this one is NOT the hole Task 27b closed: nothing
 * anywhere in Dhole publishes to the catalog — not the API, not the CLI, not
 * the git mirror — so there is no contract call to prefer here. See
 * e2e/seed/main.go.
 */
async function publishPlugin(
  request: APIRequestContext,
  version: string,
  schema: unknown,
): Promise<string> {
  const response = await request.post(`${seedUrl}/plugin`, {
    data: {
      namespace: "acme",
      name: "deploy",
      version,
      effectClass: "EFFECT_CLASS_AT_MOST_ONCE",
      inputSchema: schema,
    },
  });
  expect(response.ok(), await response.text()).toBe(true);
  return ((await response.json()) as { ref: string }).ref;
}

/** addStep adds a step naming a published plugin — and carrying no schema of
 * its own, so what the panel renders can only have come from the catalog. */
async function addStep(
  request: APIRequestContext,
  seed: Seeded,
  base: string,
  stepId: string,
  pluginRef: string,
): Promise<string> {
  const response = await rpc(request, "ApplyOperation", {
    pipelineId: seed.pipelineId,
    baseRevision: base,
    operation: {
      addStep: {
        step: {
          id: stepId,
          pluginRef,
          effectClass: "EFFECT_CLASS_AT_MOST_ONCE",
        },
      },
    },
  });
  const revision = (response as { revision?: { id?: string } }).revision;
  expect(revision?.id).toBeTruthy();
  return revision?.id ?? "";
}

/** openPanel opens the panel on one step of one revision.
 *
 * The panel has no route in App yet — the screen that places it beside the
 * canvas belongs to a later task — so the suite loads the real application
 * page from the dev server and adds src/panel/harness.tsx to it. The page has
 * to be the dev server's own document rather than one this suite fulfils:
 * Chromium's local-network-access rules refuse a synthesised document's
 * requests to a loopback control plane, which would make every assertion here
 * about the browser's address-space policy instead of about the panel.
 */
async function openPanel(
  page: Page,
  seed: Seeded,
  revision: string,
  stepId: string,
): Promise<void> {
  await page.addInitScript((value: string) => {
    window.localStorage.setItem("dhole.token", value);
  }, bootstrapToken());
  await page.goto(
    `/?panel.pipeline=${encodeURIComponent(seed.pipelineId)}` +
      `&panel.revision=${encodeURIComponent(revision)}` +
      `&panel.step=${encodeURIComponent(stepId)}`,
  );
  await page.addScriptTag({ url: "/src/panel/harness.tsx", type: "module" });
}

test("the panel is rendered from the plugin schema, not hardcoded", async ({
  page,
  request,
}) => {
  const seed = await createPipeline(request);
  const refV1 = await publishPlugin(request, "1.0.0", deployV1);
  const one = await addStep(request, seed, seed.revisionId, "deploy", refV1);

  await openPanel(page, seed, one, "deploy");
  await expect(page.getByTestId("property-panel")).toBeVisible();
  // What version one declares, and nothing more.
  await expect(page.locator("[name=command]")).toBeVisible();
  await expect(page.locator("[name=retries]")).toHaveCount(0);

  // Version two of the plugin is published. No web file changed between these
  // two assertions; the only thing that changed is the declaration.
  const refV2 = await publishPlugin(request, "2.0.0", deployV2);
  const two = await addStep(request, seed, one, "deploy-next", refV2);
  await openPanel(page, seed, two, "deploy-next");
  await expect(page.locator("[name=retries]")).toBeVisible();

  // And the field whose schema this editor cannot render is still ON SCREEN,
  // named, with the reason. A dropped field is a value the user can neither
  // set nor see is missing.
  await expect(page.getByTestId("field-unsupported-targets")).toBeVisible();
  await expect(page.getByTestId("field-unsupported-targets")).toContainText(
    "array",
  );
});

/** calls records every PipelineService request this page sends.
 *
 * The recorder is the point of every test below. A panel that shows a
 * validation error and sends the operation anyway satisfies each assertion
 * about the DOM, so the assertions here are about what left the browser —
 * exactly the trap that caught a real mutation on the canvas in Task 46.
 */
interface Sent {
  method: string;
  body: unknown;
}

function calls(page: Page): Sent[] {
  const sent: Sent[] = [];
  page.on("request", (request) => {
    const match = /\/dhole\.v1\.PipelineService\/(\w+)$/.exec(request.url());
    if (match === null || request.method() !== "POST") {
      return;
    }
    const body = request.postData();
    sent.push({
      method: match[1] ?? "",
      body: body === null ? null : (JSON.parse(body) as unknown),
    });
  });
  return sent;
}

/** reads are the RPCs the panel makes to show itself. What matters below is
 * what it WRITES, so these are filtered out by name rather than by counting. */
const reads = ["GetPipeline", "GetPlugin"];

function writes(sent: readonly Sent[]): string[] {
  return sent
    .map((call) => call.method)
    .filter((method) => !reads.includes(method));
}

test("a value the schema forbids is refused in the form, before any request", async ({
  page,
  request,
}) => {
  const seed = await createPipeline(request);
  const refV1 = await publishPlugin(request, "1.0.0", deployV1);
  const one = await addStep(request, seed, seed.revisionId, "deploy", refV1);
  const sent = calls(page);

  await openPanel(page, seed, one, "deploy");
  await expect(page.getByTestId("property-panel")).toBeVisible();

  await page.locator("[name=plugin_ref]").fill("ftp://nope");
  await page.getByTestId("panel-save").click();

  // The schema's own words, not a sentence a web file wrote about this
  // plugin: the pattern is quoted back from the declaration.
  const error = page.getByTestId("field-error-plugin_ref");
  await expect(error).toBeVisible();
  await expect(error).toContainText('must match pattern "^acme/"');
  // No review opened either: there is nothing to review.
  await expect(page.getByTestId("diff-view")).toHaveCount(0);

  // Nothing left the browser. Not the edit, and not the review's own
  // read-only Validate: "before any request is sent" is the requirement.
  expect(writes(sent)).toEqual([]);

  // The barrier: a value the schema ALLOWS goes all the way through, which
  // proves the recorder sees these calls at all.
  await publishPlugin(request, "2.0.0", deployV2);
  await page.locator("[name=plugin_ref]").fill("acme/deploy@2.0.0");
  await page.getByTestId("panel-save").click();
  await expect(page.getByTestId("diff-view")).toBeVisible();
  await page.getByTestId("diff-confirm").click();
  await expect(page.getByTestId("diff-applied")).toBeVisible();

  expect(writes(sent)).toEqual(["Validate", "ApplyOperation"]);
});

test("every save is reviewed, and cancelling applies nothing", async ({
  page,
  request,
}) => {
  const seed = await createPipeline(request);
  const refV1 = await publishPlugin(request, "1.0.0", deployV1);
  await publishPlugin(request, "2.0.0", deployV2);
  const one = await addStep(request, seed, seed.revisionId, "deploy", refV1);
  const sent = calls(page);

  await openPanel(page, seed, one, "deploy");
  await expect(page.getByTestId("property-panel")).toBeVisible();
  await page.locator("[name=plugin_ref]").fill("acme/deploy@2.0.0");

  // Save does not save. It opens the review, which names the change in the
  // step's own terms.
  await page.getByTestId("panel-save").click();
  const review = page.getByTestId("diff-view");
  await expect(review).toBeVisible();
  await expect(page.getByTestId("diff-change")).toContainText("plugin_ref");

  await page.getByTestId("diff-cancel").click();
  await expect(review).toHaveCount(0);
  expect(writes(sent)).not.toContain("ApplyOperation");

  // And the definition the API holds is untouched by a cancelled review.
  const after = (await rpc(request, "GetPipeline", {
    pipelineId: seed.pipelineId,
    revisionId: one,
  })) as { pipeline?: { steps?: { id?: string; pluginRef?: string }[] } };
  expect(after.pipeline?.steps?.find((s) => s.id === "deploy")?.pluginRef).toBe(
    refV1,
  );

  // Barrier again: confirming the same edit does apply it, once.
  await page.getByTestId("panel-save").click();
  await page.getByTestId("diff-confirm").click();
  await expect(page.getByTestId("diff-applied")).toBeVisible();
  expect(writes(sent).filter((m) => m === "ApplyOperation")).toEqual([
    "ApplyOperation",
  ]);
});

/** The sentence internal/catalog.widenWarning writes, verbatim. It is the
 * CONTROL PLANE's: internal/catalog computes it in ResolveStep
 * (Entry.OverrideWarnings) and internal/api/validate.go hands it out as a
 * warning diagnostic. The browser must not recompute it, so the suite drives
 * the panel by what the server says.
 *
 * It is provoked rather than stubbed. It used to be stubbed because the
 * fixture control plane was built without a catalog and could produce no
 * plugin diagnostic at all; `dhole serve` carries one, and the plugin below is
 * really published, so the real warning is what arrives. */
const widening =
  'step "deploy" widens effect class to EFFECT_CLASS_PURE over ' +
  "EFFECT_CLASS_AT_MOST_ONCE declared by acme/deploy@1.0.0: " +
  "the step becomes cacheable and automatically retried";

test("a widening effect-class override is a policy decision point in the diff", async ({
  page,
  request,
}) => {
  const seed = await createPipeline(request);
  const refV1 = await publishPlugin(request, "1.0.0", deployV1);
  await publishPlugin(request, "2.0.0", deployV2);
  const one = await addStep(request, seed, seed.revisionId, "deploy", refV1);
  const sent = calls(page);

  await openPanel(page, seed, one, "deploy");
  await expect(page.getByTestId("property-panel")).toBeVisible();

  // First, the control: an edit the control plane has nothing to say about.
  // A panel that decided this for itself would highlight here too, and the
  // property under test is that it highlights only what it was told.
  await page.locator("[name=plugin_ref]").fill("acme/deploy@2.0.0");
  await page.getByTestId("panel-save").click();
  await expect(page.getByTestId("diff-view")).toBeVisible();
  await expect(page.getByTestId("policy-decision")).toHaveCount(0);
  await page.getByTestId("diff-cancel").click();

  // The review asked the control plane about the pipeline it is PROPOSING,
  // not about the saved one: a warning about the definition as it stands
  // would never mention the override being reviewed.
  await page.locator("[name=plugin_ref]").fill(refV1);
  await page.locator("[name=effect_class]").selectOption("EFFECT_CLASS_PURE");
  await page.getByTestId("panel-save").click();

  const decision = page.getByTestId("policy-decision");
  await expect(decision).toBeVisible();
  // The control plane's sentence, not a paraphrase of it.
  await expect(decision).toContainText(widening);
  await expect(decision).toContainText("policy");

  const validate = sent.find((call) => call.method === "Validate");
  expect(validate).toBeDefined();
  const proposed = (
    validate?.body as {
      pipeline?: { steps?: { id?: string; effectClass?: string }[] };
    }
  ).pipeline;
  expect(proposed?.steps?.find((s) => s.id === "deploy")).toBeDefined();
});
