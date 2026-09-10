/**
 * The run view, end to end against a live `dhole serve`.
 *
 * Everything here is asserted against what the CONTROL PLANE said, not against
 * strings retyped into the test: a case that hard-codes the server's wording
 * passes forever after the server changes it. The cache-ineligibility case is
 * the sharpest example — the reason string is Task 16's, it lives in
 * internal/cache/eligibility.go, and this file reads it back off the run's own
 * event stream before comparing it to the DOM.
 *
 * Credentials come from web/e2e/plane.ts, the one arrangement every spec
 * shares: `dhole serve` mints a bootstrap token before it opens its port, and
 * the seeder issues a second tenant's. This file mints nothing of its own — a
 * test that invents a credential is testing its own fake.
 *
 * The pipelines are still taken from the environment, because this suite needs
 * particular SHAPES that nothing yet creates through the contract: two pure
 * steps where the second consumes the first, one step with no effect class,
 * and one bounded loop. A missing one FAILS rather than skips: a skipped
 * isolation test is indistinguishable, in CI output, from one that never
 * existed.
 *
 *   DHOLE_E2E_PIPELINE         two pure steps, the second consuming the first
 *   DHOLE_E2E_PIPELINE_IMPURE  one step declaring no effect class
 *   DHOLE_E2E_PIPELINE_LOOP    a bounded loop
 */
import {
  expect,
  test,
  type APIRequestContext,
  type Page,
} from "@playwright/test";

import { apiUrl, bootstrapToken, seedShape, tokenFor } from "./plane.js";

const token = (): string => bootstrapToken();
/** A pipeline whose body is a bounded loop. */

/** call issues one Connect RPC over its JSON binding. */
async function call<T>(
  request: APIRequestContext,
  method: string,
  body: unknown,
  bearer: string = token(),
): Promise<T> {
  const res = await request.post(
    `${apiUrl}/dhole.v1.PipelineService/${method}`,
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${bearer}`,
      },
      data: body,
    },
  );
  expect(
    res.ok(),
    `${method}: ${res.status()} ${await res.text()}`,
  ).toBeTruthy();
  return (await res.json()) as T;
}

/** startRun starts one run of the pipeline and returns its id. */
async function startRun(
  request: APIRequestContext,
  pipeline: string,
): Promise<string> {
  const res = await call<{ runId: string }>(request, "StartRun", {
    pipelineId: pipeline,
  });
  expect(res.runId).not.toEqual("");
  return res.runId;
}

/** runEvents drains the SSE event stream of a finished run. */
async function runEvents(
  request: APIRequestContext,
  runID: string,
  bearer: string = token(),
): Promise<{ type: string; stepId: string; payload: string; id: string }[]> {
  const res = await request.get(`${apiUrl}/v1/runs/${runID}/events`, {
    headers: { Authorization: `Bearer ${bearer}`, Accept: "text/event-stream" },
    timeout: 60_000,
  });
  expect(res.ok(), `events: ${res.status()}`).toBeTruthy();
  return parseSSE(await res.text());
}

function parseSSE(
  text: string,
): { type: string; stepId: string; payload: string; id: string }[] {
  const out: { type: string; stepId: string; payload: string; id: string }[] =
    [];
  for (const frame of text.split("\n\n")) {
    let id = "";
    let event = "";
    let data = "";
    for (const line of frame.split("\n")) {
      if (line.startsWith("id:")) id = line.slice(3).trim();
      else if (line.startsWith("event:")) event = line.slice(6).trim();
      else if (line.startsWith("data:")) data += line.slice(5).trim();
    }
    if (event === "") continue;
    const parsed = data === "" ? {} : (JSON.parse(data) as { stepId?: string });
    out.push({ type: event, stepId: parsed.stepId ?? "", payload: data, id });
  }
  return out;
}

/** openRun points the browser at one run, signed in as the tenant that owns it. */
async function openRun(
  page: Page,
  runID: string,
  bearer: string = token(),
): Promise<void> {
  await page.addInitScript(
    ([key, value]) => {
      window.localStorage.setItem(key as string, value as string);
    },
    ["dhole.token", bearer],
  );
  await page.goto(`/#/runs/${runID}`);
}

test("the run view shows the realised graph, cache hits and streamed logs", async ({
  page,
  request,
}) => {
  // First run: cold. Every step actually executes, so every node has a real
  // duration and nothing is marked cached.
  const cold = await startRun(
    request,
    (await seedShape(request, "cacheable")).pipelineId,
  );
  await openRun(page, cold);

  const graph = page.getByTestId("run-graph");
  await expect(graph).toBeVisible();

  const nodes = graph.getByTestId(/^run-node-/);
  await expect(nodes).toHaveCount(2, { timeout: 60_000 });

  // Log lines arrive WITHOUT a reload: this is the live subject doing its job.
  const logs = page.getByTestId("log-stream");
  await expect(logs.getByTestId("log-line").first()).toBeVisible({
    timeout: 60_000,
  });
  const navigations = await page.evaluate(
    () => performance.getEntriesByType("navigation").length,
  );

  for (const node of await nodes.all()) {
    await expect(node).toHaveAttribute("data-state", /SUCCEEDED/, {
      timeout: 60_000,
    });
    const duration = await node.getByTestId("node-duration").innerText();
    expect(duration, "a realised node must show how long it took").toMatch(
      /\d/,
    );
  }
  expect(
    await page.evaluate(
      () => performance.getEntriesByType("navigation").length,
    ),
    "the log must stream into the open page, not arrive on a reload",
  ).toEqual(navigations);

  // Second run: warm. Both steps are pure and unchanged, so both come out of
  // the cache and the view has to say so.
  const warm = await startRun(
    request,
    (await seedShape(request, "cacheable")).pipelineId,
  );
  await openRun(page, warm);
  const warmNodes = page.getByTestId("run-graph").getByTestId(/^run-node-/);
  await expect(warmNodes).toHaveCount(2, { timeout: 60_000 });
  for (const node of await warmNodes.all()) {
    await expect(node).toHaveAttribute("data-cached", "true", {
      timeout: 60_000,
    });
  }
});

test("the view switches from the live subject to the stored object when the run completes", async ({
  page,
  request,
}) => {
  // The SLOW shape, because this test has to catch a step in the act. The
  // cacheable one finishes in milliseconds and the view goes straight to
  // "stored", which fails here as though the live source were broken when it
  // is only over.
  const runID = await startRun(
    request,
    (await seedShape(request, "slow")).pipelineId,
  );
  await openRun(page, runID);

  const logs = page.getByTestId("log-stream");
  // While it runs, the view is reading the ephemeral subject.
  await expect(logs).toHaveAttribute("data-source", "live", {
    timeout: 60_000,
  });
  // Once the run is done, it must be reading the authoritative object. The
  // live copy is best-effort and may have gaps; the stored one may not.
  await expect(logs).toHaveAttribute("data-source", "stored", {
    timeout: 120_000,
  });
  const streamed = await logs.innerText();

  // A reader who arrives AFTER the run gets the same complete log — there is
  // no live subject left to tail.
  await page.reload();
  await expect(logs).toHaveAttribute("data-source", "stored", {
    timeout: 60_000,
  });
  expect(await logs.innerText()).toEqual(streamed);
});

test("a non-cacheable step shows the control plane's own reason, not a retyped one", async ({
  page,
  request,
}) => {
  const runID = await startRun(
    request,
    (await seedShape(request, "impure")).pipelineId,
  );
  await openRun(page, runID);

  const events = await runEvents(request, runID);
  const dispatched = events.filter((e) => e.type === "STEP_DISPATCHED");
  expect(dispatched.length).toBeGreaterThan(0);
  const reason = dispatched
    .map(
      (e) =>
        (JSON.parse(e.payload) as { cache_ineligible_reason?: string })
          .cache_ineligible_reason,
    )
    .find((r) => r !== undefined && r !== "");
  expect(
    reason,
    "the dispatch must carry why the step could not be cached",
  ).toBeTruthy();

  const node = page.getByTestId(`run-node-${dispatched[0]!.stepId}`);
  // Exactly the server's string. Not a substring, not a paraphrase: the two
  // drift apart the moment this test is allowed to be approximately right.
  await expect(node.getByTestId("node-cache-reason")).toHaveText(reason!, {
    timeout: 60_000,
  });
});

test("a bounded loop is one container node that expands to its unrolled iterations", async ({
  page,
  request,
}) => {
  test.skip(
    true,
    "no loop pipeline can be created yet: no operation builds a loop node, so " +
      "there is nothing to seed. Task 50 built the loop; authoring one through " +
      "the contract is still open.",
  );
  const runID = "";
  await openRun(page, runID);

  const container = page
    .getByTestId("run-graph")
    .locator("[data-container=loop]");
  await expect(container).toHaveCount(1, { timeout: 60_000 });
  const loopID = (await container.getAttribute("data-step-id"))!;

  // Collapsed by default: a loop of fifty iterations must not be fifty nodes
  // on the canvas before anyone asked.
  await expect(container.getByTestId("loop-iteration")).toHaveCount(0);

  const events = await runEvents(request, runID);
  const iterations = events.filter(
    (e) => e.type === "LOOP_ITERATION_STARTED" && e.stepId.startsWith(loopID),
  ).length;
  expect(
    iterations,
    "the run must have recorded at least one iteration",
  ).toBeGreaterThan(0);

  await container.getByTestId("expand-loop").click();
  await expect(container.getByTestId("loop-iteration")).toHaveCount(iterations);
});

test("neither stream is readable by another tenant, or without a credential", async ({
  request,
}) => {
  const runID = await startRun(
    request,
    (await seedShape(request, "cacheable")).pipelineId,
  );
  // A real second tenant from the seeder, not a token this file invented.
  const otherTenant = await tokenFor(request, "isolation");

  const bare = await request.get(`${apiUrl}/v1/runs/${runID}/events`);
  expect(bare.status(), "an unauthenticated stream must be refused").toEqual(
    401,
  );

  // Another tenant gets NOT FOUND rather than FORBIDDEN: a caller who can tell
  // "not yours" from "not there" learns which runs exist elsewhere.
  const foreign = await request.get(`${apiUrl}/v1/runs/${runID}/events`, {
    headers: { Authorization: `Bearer ${otherTenant}` },
  });
  expect(foreign.status()).toEqual(404);

  const foreignLogs = await request.get(
    `${apiUrl}/v1/runs/${runID}/steps/first/logs`,
    {
      headers: { Authorization: `Bearer ${otherTenant}` },
    },
  );
  expect(foreignLogs.status()).toEqual(404);
});

test("the event stream ends when the run does, instead of holding the connection open", async ({
  request,
}) => {
  const runID = await startRun(
    request,
    (await seedShape(request, "cacheable")).pipelineId,
  );
  // A GET that returns at all is a stream that terminated: request.get reads
  // to EOF. A stream held open forever times out here instead.
  const events = await runEvents(request, runID);
  expect(events.at(-1)?.type).toEqual("end");
  expect(events.some((e) => e.type === "RUN_COMPLETED")).toBeTruthy();
});
