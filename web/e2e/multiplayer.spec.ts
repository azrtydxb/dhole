/**
 * Two people editing one pipeline, in two real browsers.
 *
 * TWO BROWSER CONTEXTS, NOT TWO TABS OF ONE. A single context shares
 * localStorage, one service worker and one set of connections, so a "second
 * editor" living in it can see the first one's state without a byte crossing
 * the control plane — which is exactly the thing under test. Each editor here
 * gets its own context, and the only thing they share is the plane.
 *
 * The second test asserts on the NETWORK rather than the DOM. A canvas that
 * shows a rebase prompt and re-sends the operation against the new head has
 * silently overwritten the other person's work while looking, on screen,
 * exactly like one that did the right thing.
 */
import {
  expect,
  test,
  type APIRequestContext,
  type Browser,
  type Page,
} from "@playwright/test";

import { apiUrl, bootstrapToken, createPipeline, rpc } from "./plane.js";

/** A canvas open on one revision, in a context of its own. */
async function openEditor(
  browser: Browser,
  pipelineId: string,
  revisionId: string,
): Promise<Page> {
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.addInitScript((value: string) => {
    window.localStorage.setItem("dhole.token", value);
  }, bootstrapToken());
  await page.goto(
    `/?pipeline=${encodeURIComponent(pipelineId)}` +
      `&revision=${encodeURIComponent(revisionId)}`,
  );
  await expect(page.getByTestId("add-step")).toBeVisible();
  return page;
}

/** addStep writes one step through the API, so both editors can start from
 * the same revision rather than one of them having authored it. */
async function addStep(
  request: APIRequestContext,
  pipelineId: string,
  baseRevision: string,
  stepId: string,
): Promise<string> {
  const response = await rpc(request, "ApplyOperation", {
    pipelineId,
    baseRevision,
    operation: { addStep: { step: { id: stepId } } },
  });
  const revision = (response as { revision?: { id?: string } }).revision;
  expect(revision?.id, JSON.stringify(response)).toBeTruthy();
  return revision?.id ?? "";
}

/** pluginRefOf reads one step's plugin_ref back from the plane — the only
 * judge of what was actually written. */
async function pluginRefOf(
  request: APIRequestContext,
  pipelineId: string,
  stepId: string,
): Promise<string> {
  const response = await request.post(
    `${apiUrl}/dhole.v1.PipelineService/GetPipeline`,
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${bootstrapToken()}`,
      },
      data: { pipelineId },
    },
  );
  expect(response.ok(), await response.text()).toBe(true);
  const body = (await response.json()) as {
    pipeline?: { steps?: { id?: string; pluginRef?: string }[] };
  };
  const step = body.pipeline?.steps?.find((s) => s.id === stepId);
  return step?.pluginRef ?? "";
}

/** select drives the canvas's own step selector, which is what an editor
 * announces as its selection. */
async function select(page: Page, stepId: string): Promise<void> {
  await page.getByTestId("property-step").selectOption(stepId);
}

/** applyCount records how many ApplyOperation requests this page has sent. */
function applyCount(page: Page): () => number {
  let sent = 0;
  page.on("request", (request) => {
    if (request.url().endsWith("/dhole.v1.PipelineService/ApplyOperation")) {
      sent += 1;
    }
  });
  return () => sent;
}

test("two editors see each other's selections", async ({
  browser,
  request,
}) => {
  const seed = await createPipeline(request);
  const withA = await addStep(request, seed.pipelineId, seed.revisionId, "a");
  const revision = await addStep(request, seed.pipelineId, withA, "b");

  const one = await openEditor(browser, seed.pipelineId, revision);
  const two = await openEditor(browser, seed.pipelineId, revision);

  await select(one, "a");
  await select(two, "b");

  // Each editor learns what the other has selected, and nothing about it came
  // from the browser they share: they share none.
  await expect(one.getByTestId("presence-selection-b")).toBeVisible();
  await expect(two.getByTestId("presence-selection-a")).toBeVisible();

  // And presence is only ever about who is here now: an editor that closes
  // stops being drawn by the one still open.
  await two.context().close();
  await expect(one.getByTestId("presence-selection-b")).toHaveCount(0, {
    timeout: 15_000,
  });
});

test("a conflicting edit prompts a rebase instead of overwriting", async ({
  browser,
  request,
}) => {
  const seed = await createPipeline(request);
  const revision = await addStep(
    request,
    seed.pipelineId,
    seed.revisionId,
    "a",
  );

  const one = await openEditor(browser, seed.pipelineId, revision);
  const two = await openEditor(browser, seed.pipelineId, revision);
  const sentByTwo = applyCount(two);

  // The first editor changes step a. The second editor has not seen it.
  await select(one, "a");
  await one.getByTestId("property-name").selectOption("plugin_ref");
  await one.getByTestId("property-value").fill("oci://one/first:v1");
  await one.getByTestId("set-property").click();
  await expect(one.getByTestId("revision-id")).not.toHaveText(revision);

  await select(two, "a");
  await two.getByTestId("property-name").selectOption("plugin_ref");
  await two.getByTestId("property-value").fill("oci://two/second:v1");
  await two.getByTestId("set-property").click();

  // The refusal is visible, and it names the revision to rebase onto rather
  // than only saying that something went wrong.
  const prompt = two.getByTestId("rebase-prompt");
  await expect(prompt).toBeVisible();
  await expect(prompt).toContainText(
    (await one.getByTestId("revision-id").textContent()) ?? "",
  );

  // The network, which is where the real failure hides: the losing editor
  // sent its operation ONCE and did not resend it against the new head. A
  // canvas that showed this prompt and retried would look identical here.
  const afterConflict = sentByTwo();
  expect(afterConflict).toBe(1);
  await two.waitForTimeout(1_000);
  expect(sentByTwo()).toBe(1);

  // The plane agrees: the first editor's value is what stands.
  expect(await pluginRefOf(request, seed.pipelineId, "a")).toBe(
    "oci://one/first:v1",
  );

  // Rebasing is a READ. It moves this editor onto the revision it was told
  // about; it does not replay the refused edit.
  await two.getByTestId("rebase").click();
  await expect(two.getByTestId("revision-id")).toHaveText(
    (await one.getByTestId("revision-id").textContent()) ?? "",
  );
  await expect(two.getByTestId("rebase-prompt")).toHaveCount(0);
  expect(sentByTwo()).toBe(1);
  expect(await pluginRefOf(request, seed.pipelineId, "a")).toBe(
    "oci://one/first:v1",
  );
});
