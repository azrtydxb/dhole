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
 * Where the schema comes from: the plugin's declaration reaches the browser
 * inline on the step's structured input port (`StructType.schema`), fetched
 * with GetPipeline. There is no catalog RPC on the contract yet — see the
 * report in the task and the note in PropertyPanel.tsx — so "publishing a
 * plugin" here means seeding a step whose declared input port carries that
 * plugin's input schema, done through the real ApplyOperation.
 */

import {
  expect,
  test,
  type APIRequestContext,
  type Page,
} from "@playwright/test";

import { apiUrl, bootstrapToken, seedPipeline, type Seeded } from "./plane.js";

/** rpc calls one RPC of the real contract with the fixture's real token. */
async function rpc(
  request: APIRequestContext,
  method: string,
  body: unknown,
): Promise<Record<string, unknown>> {
  const token = bootstrapToken();
  const response = await request.post(
    `${apiUrl}/dhole.v1.PipelineService/${method}`,
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      data: body,
    },
  );
  expect(response.ok(), await response.text()).toBe(true);
  return (await response.json()) as Record<string, unknown>;
}

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
      pattern: "^oci://",
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

/** publishStep adds a step whose declared input port carries `schema` — the
 * plugin's declaration, arriving over the wire rather than out of a web file.
 * It returns the revision the edit produced. */
async function publishStep(
  request: APIRequestContext,
  seed: Seeded,
  base: string,
  stepId: string,
  schema: unknown,
): Promise<string> {
  const response = await rpc(request, "ApplyOperation", {
    pipelineId: seed.pipelineId,
    baseRevision: base,
    operation: {
      addStep: {
        step: {
          id: stepId,
          pluginRef: "",
          effectClass: "EFFECT_CLASS_AT_MOST_ONCE",
          inputs: [
            {
              name: "config",
              type: {
                structured: {
                  schemaId: (schema as { $id: string }).$id,
                  schema: JSON.stringify(schema),
                },
              },
            },
          ],
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
 *
 * The query keys are the harness's own, so opening the panel does not also
 * open a canvas on the same pipeline.
 */
async function openPanel(
  page: Page,
  seed: Seeded,
  revision: string,
  stepId: string,
): Promise<void> {
  const token = bootstrapToken();
  await page.addInitScript((value: string) => {
    window.localStorage.setItem("dhole.token", value);
  }, token);
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
  const seed = await seedPipeline(request);
  const one = await publishStep(
    request,
    seed,
    seed.revisionId,
    "deploy",
    deployV1,
  );

  await openPanel(page, seed, one, "deploy");
  await expect(page.getByTestId("property-panel")).toBeVisible();
  // What version one declares, and nothing more.
  await expect(page.locator("[name=command]")).toBeVisible();
  await expect(page.locator("[name=retries]")).toHaveCount(0);

  // Version two of the plugin is published. No web file changed between these
  // two assertions; the only thing that changed is the declaration.
  const two = await publishStep(request, seed, one, "deploy-next", deployV2);
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

function methods(sent: readonly Sent[]): string[] {
  return sent.map((call) => call.method);
}

test("a value the schema forbids is refused in the form, before any request", async ({
  page,
  request,
}) => {
  const seed = await seedPipeline(request);
  const one = await publishStep(
    request,
    seed,
    seed.revisionId,
    "deploy",
    deployV1,
  );
  const sent = calls(page);

  await openPanel(page, seed, one, "deploy");
  await expect(page.getByTestId("property-panel")).toBeVisible();

  await page.locator("[name=plugin_ref]").fill("ftp://nope");
  await page.getByTestId("panel-save").click();

  // The schema's own words, not a sentence a web file wrote about this
  // plugin: the pattern is quoted back from the declaration.
  const error = page.getByTestId("field-error-plugin_ref");
  await expect(error).toBeVisible();
  await expect(error).toContainText('must match pattern "^oci://"');
  // No review opened either: there is nothing to review.
  await expect(page.getByTestId("diff-view")).toHaveCount(0);

  // Nothing left the browser. Not the edit, and not the review's own
  // read-only Validate: "before any request is sent" is the requirement.
  expect(methods(sent).filter((m) => m !== "GetPipeline")).toEqual([]);

  // The barrier: a value the schema ALLOWS goes all the way through, which
  // proves the recorder sees these calls at all.
  await page.locator("[name=plugin_ref]").fill("oci://example/deploy:1");
  await page.getByTestId("panel-save").click();
  await expect(page.getByTestId("diff-view")).toBeVisible();
  await page.getByTestId("diff-confirm").click();
  await expect(page.getByTestId("diff-applied")).toBeVisible();

  expect(methods(sent).filter((m) => m !== "GetPipeline")).toEqual([
    "Validate",
    "ApplyOperation",
  ]);
});

test("every save is reviewed, and cancelling applies nothing", async ({
  page,
  request,
}) => {
  const seed = await seedPipeline(request);
  const one = await publishStep(
    request,
    seed,
    seed.revisionId,
    "deploy",
    deployV1,
  );
  const sent = calls(page);

  await openPanel(page, seed, one, "deploy");
  await expect(page.getByTestId("property-panel")).toBeVisible();
  await page.locator("[name=plugin_ref]").fill("oci://example/deploy:1");

  // Save does not save. It opens the review, which names the change in the
  // step's own terms.
  await page.getByTestId("panel-save").click();
  const review = page.getByTestId("diff-view");
  await expect(review).toBeVisible();
  await expect(page.getByTestId("diff-change")).toContainText("plugin_ref");

  await page.getByTestId("diff-cancel").click();
  await expect(review).toHaveCount(0);
  expect(methods(sent)).not.toContain("ApplyOperation");

  // And the definition the API holds is untouched by a cancelled review.
  const after = (await rpc(request, "GetPipeline", {
    pipelineId: seed.pipelineId,
    revisionId: one,
  })) as { pipeline?: { steps?: { id?: string; pluginRef?: string }[] } };
  expect(
    after.pipeline?.steps?.find((s) => s.id === "deploy")?.pluginRef,
  ).toBeFalsy();

  // Barrier again: confirming the same edit does apply it, once.
  await page.getByTestId("panel-save").click();
  await page.getByTestId("diff-confirm").click();
  await expect(page.getByTestId("diff-applied")).toBeVisible();
  expect(methods(sent).filter((m) => m === "ApplyOperation")).toEqual([
    "ApplyOperation",
  ]);
});

/** stubValidate answers the review's Validate call with `diagnostics`.
 *
 * The widening warning is the CONTROL PLANE's: internal/catalog computes it in
 * ResolveStep (Entry.OverrideWarnings) and internal/api/validate.go hands it
 * out as a warning diagnostic. The browser must not recompute it, so the
 * suite drives the panel by what the server says rather than by what the edit
 * was — including the case where the server says nothing.
 *
 * It is stubbed rather than provoked because the e2e control plane is
 * constructed WITHOUT a catalog (api.Config.Catalog is nil in
 * web/e2e/fixture), so no plugin diagnostic can be produced there. The day
 * the served plane carries a catalog, this stub is deleted and the same
 * assertions run against the real warning.
 */
async function stubValidate(
  page: Page,
  diagnostics: readonly Record<string, string>[],
): Promise<void> {
  await page.route("**/dhole.v1.PipelineService/Validate", (route) => {
    const cors = {
      "access-control-allow-origin": "*",
      "access-control-allow-headers": "*",
      "access-control-allow-methods": "POST, OPTIONS",
      "access-control-expose-headers": "*",
    };
    if (route.request().method() === "OPTIONS") {
      return route.fulfill({ status: 204, headers: cors });
    }
    return route.fulfill({
      status: 200,
      headers: { ...cors, "content-type": "application/json" },
      body: JSON.stringify({ diagnostics }),
    });
  });
}

/** The sentence internal/catalog.widenWarning writes, verbatim. */
const widening =
  'step "deploy" widens effect class to EFFECT_CLASS_PURE over ' +
  "EFFECT_CLASS_AT_MOST_ONCE declared by acme/deploy@1.0.0: " +
  "the step becomes cacheable and automatically retried";

test("a widening effect-class override is a policy decision point in the diff", async ({
  page,
  request,
}) => {
  const seed = await seedPipeline(request);
  const one = await publishStep(
    request,
    seed,
    seed.revisionId,
    "deploy",
    deployV1,
  );
  const sent = calls(page);

  // First, the control: the SAME widening edit, with a control plane that
  // reports nothing. A panel that decided this for itself would highlight
  // here, and the property under test is that it does not.
  await stubValidate(page, []);
  await openPanel(page, seed, one, "deploy");
  await expect(page.getByTestId("property-panel")).toBeVisible();
  await page.locator("[name=plugin_ref]").fill("oci://example/deploy:1");
  await page.locator("[name=effect_class]").selectOption("EFFECT_CLASS_PURE");
  await page.getByTestId("panel-save").click();
  await expect(page.getByTestId("diff-view")).toBeVisible();
  await expect(page.getByTestId("policy-decision")).toHaveCount(0);
  await page.getByTestId("diff-cancel").click();

  // The review asked the control plane about the pipeline it is PROPOSING,
  // not about the saved one: a warning about the definition as it stands
  // would never mention the override being reviewed.
  const validate = sent.find((call) => call.method === "Validate");
  expect(validate).toBeDefined();
  const proposed = (
    validate?.body as {
      pipeline?: { steps?: { id?: string; effectClass?: string }[] };
    }
  ).pipeline;
  expect(proposed?.steps?.find((s) => s.id === "deploy")?.effectClass).toBe(
    "EFFECT_CLASS_PURE",
  );

  // Now the same edit against a plane that reports the widening.
  await stubValidate(page, [
    { severity: "warning", stepId: "deploy", message: widening },
  ]);
  await page.getByTestId("panel-save").click();
  const decision = page.getByTestId("policy-decision");
  await expect(decision).toBeVisible();
  // The control plane's sentence, not a paraphrase of it.
  await expect(decision).toContainText(widening);
  await expect(decision).toContainText("policy");
});
