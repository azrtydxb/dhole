/**
 * The canvas, end to end, against a real control plane.
 *
 * Three properties are under test here, and only the first is about drawing:
 *
 *  1. The canvas authors a pipeline. Every edit is an ApplyOperation, and the
 *     revision the API holds afterwards is what the assertions read — not the
 *     DOM, which would only prove the canvas believes itself.
 *  2. A badly typed edge is refused AT DROP TIME. The assertion is on the
 *     network: a canvas that shows an error and sends the operation anyway
 *     passes a DOM-only check and fails this one.
 *  3. Node positions never reach the document. Layout is derived from the DAG
 *     every time it is drawn; the moment it is stored, the definition stops
 *     being a pipeline and becomes a drawing of one.
 */
import type { DescMessage } from "@bufbuild/protobuf";
import {
  expect,
  test,
  type APIRequestContext,
  type Page,
} from "@playwright/test";

import { ApplyOperationRequestSchema } from "../src/gen/dhole/v1/api_pb.js";
import { apiUrl, bootstrapToken, seedPipeline, type Seeded } from "./plane.js";

interface WirePort {
  name?: string;
}

interface WireStep {
  id?: string;
  pluginRef?: string;
  inputs?: WirePort[];
  outputs?: WirePort[];
}

interface WireEdge {
  fromStep?: string;
  fromPort?: string;
  toStep?: string;
  toPort?: string;
}

interface WirePipeline {
  id?: string;
  steps?: WireStep[];
  edges?: WireEdge[];
}

/** getPipeline reads a revision back from the API — the source of truth for
 * every assertion about what was actually authored. */
async function getPipeline(
  request: APIRequestContext,
  pipelineId: string,
  revisionId: string,
): Promise<WirePipeline> {
  const response = await request.post(
    `${apiUrl}/dhole.v1.PipelineService/GetPipeline`,
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${bootstrapToken()}`,
      },
      data: { pipelineId, revisionId },
    },
  );
  expect(response.ok(), await response.text()).toBe(true);
  const body = (await response.json()) as { pipeline?: WirePipeline };
  return body.pipeline ?? {};
}

/** openCanvas signs the browser in with the plane's real bootstrap token and
 * opens the canvas on one revision of one pipeline. */
async function openCanvas(page: Page, seed: Seeded): Promise<void> {
  await page.addInitScript((value: string) => {
    window.localStorage.setItem("dhole.token", value);
  }, bootstrapToken());
  await page.goto(
    `/?pipeline=${encodeURIComponent(seed.pipelineId)}` +
      `&revision=${encodeURIComponent(seed.revisionId)}`,
  );
  await expect(page.getByTestId("add-step")).toBeVisible();
}

/** addStep drives the canvas's own controls rather than the API. */
async function addStep(page: Page, id: string, kind: string): Promise<void> {
  await page.getByTestId("step-id").fill(id);
  await page.getByTestId("step-kind").selectOption(kind);
  await page.getByTestId("add-step").click();
  await expect(page.getByTestId(`step-node-${id}`)).toBeVisible();
}

/** dragPort is a real pointer drag from one port handle to another: the
 * connection has to be made the way a user makes it, or the type check at
 * drop time is not the thing being tested. */
async function dragPort(page: Page, from: string, to: string): Promise<void> {
  const source = page.getByTestId(from);
  const target = page.getByTestId(to);
  await expect(source).toBeVisible();
  await expect(target).toBeVisible();

  const a = await source.boundingBox();
  const b = await target.boundingBox();
  if (a === null || b === null) {
    throw new Error(`no bounding box for ${from} or ${to}`);
  }
  await page.mouse.move(a.x + a.width / 2, a.y + a.height / 2);
  await page.mouse.down();
  // Several moves, not one: a connection line follows pointer movement, and
  // a single jump can land before the drag has been recognised.
  for (let step = 1; step <= 8; step++) {
    await page.mouse.move(
      a.x + a.width / 2 + ((b.x - a.x) * step) / 8,
      a.y + a.height / 2 + ((b.y - a.y) * step) / 8,
    );
  }
  await page.mouse.move(b.x + b.width / 2, b.y + b.height / 2);
  await page.mouse.up();
}

/** setProperty sets one scalar property of one step through the canvas. */
async function setProperty(
  page: Page,
  stepId: string,
  property: string,
  value: string,
): Promise<void> {
  // Every operation produces a revision, so the revision changing is how the
  // canvas says the edit has landed. Reading the pipeline back before it does
  // would assert against the definition as it was one operation ago.
  const before = await revisionId(page);
  await page.getByTestId("property-step").selectOption(stepId);
  await page.getByTestId("property-name").selectOption(property);
  await page.getByTestId("property-value").fill(value);
  await page.getByTestId("set-property").click();
  await expect(page.getByTestId("revision-id")).not.toHaveText(before);
}

/** revisionId is the revision the canvas last saved — every operation saves
 * one, because there is no document-level write to "save" (ADR 0013). */
async function revisionId(page: Page): Promise<string> {
  const shown = await page.getByTestId("revision-id").textContent();
  expect(shown).toBeTruthy();
  return shown ?? "";
}

test("the canvas authors a pipeline end to end", async ({ page, request }) => {
  const seed = await seedPipeline(request);
  await openCanvas(page, seed);

  await addStep(page, "a", "blob-source");
  await addStep(page, "b", "blob-sink");

  await dragPort(page, "port-out-a-out", "port-in-b-in");
  await expect(page.getByTestId("edge-a-out-b-in")).toBeVisible();

  await setProperty(page, "a", "plugin_ref", "oci://example/producer:v1");

  // The API is the judge. The canvas can draw whatever it likes; what was
  // authored is what the control plane will hand back.
  const saved = await getPipeline(
    request,
    seed.pipelineId,
    await revisionId(page),
  );
  expect(saved.edges).toEqual([
    { fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" },
  ]);
  expect(saved.steps?.find((s) => s.id === "a")?.pluginRef).toBe(
    "oci://example/producer:v1",
  );
});

/** applyOperations records the body of every ApplyOperation this page sends.
 *
 * The recorder is the point of the next two tests. A canvas that shows a
 * refusal and sends the operation anyway satisfies every assertion about the
 * DOM, so the assertions here are about what left the browser. */
function applyOperations(page: Page): unknown[] {
  const sent: unknown[] = [];
  page.on("request", (request) => {
    if (!request.url().endsWith("/dhole.v1.PipelineService/ApplyOperation")) {
      return;
    }
    const body = request.postData();
    sent.push(body === null ? null : (JSON.parse(body) as unknown));
  });
  return sent;
}

/** operationKinds names the operation in each recorded body, in order. */
function operationKinds(sent: unknown[]): string[] {
  return sent.map((body) => {
    const operation = (body as { operation?: Record<string, unknown> })
      .operation;
    return Object.keys(operation ?? {}).join("+");
  });
}

test("a blob output refuses a structured input at drop time", async ({
  page,
  request,
}) => {
  const seed = await seedPipeline(request);
  const sent = applyOperations(page);
  await openCanvas(page, seed);

  await addStep(page, "a", "blob-source");
  await addStep(page, "c", "structured-sink");
  await addStep(page, "b", "blob-sink");

  await dragPort(page, "port-out-a-out", "port-in-c-in");

  // Visible, and it says what is wrong in the type system's own words.
  const refusal = page.getByTestId("edge-error");
  await expect(refusal).toBeVisible();
  await expect(refusal).toContainText("blob is not structured");
  await expect(page.getByTestId("edge-a-out-c-in")).toHaveCount(0);

  // The barrier: a connection that IS well typed. Requests arrive in order,
  // so once this one has been recorded, a request the refused drag might have
  // sent would already be in the list too. That makes the assertion below a
  // statement about the whole exchange rather than about a moment in it — and
  // it proves the recorder sees connects at all, which a bare "nothing was
  // sent" could not.
  await dragPort(page, "port-out-a-out", "port-in-b-in");
  await expect(page.getByTestId("edge-a-out-b-in")).toBeVisible();

  expect(operationKinds(sent)).toEqual([
    "addStep",
    "addStep",
    "addStep",
    "connect",
  ]);

  // And the definition the API holds has exactly the one edge.
  const saved = await getPipeline(
    request,
    seed.pipelineId,
    await revisionId(page),
  );
  expect(saved.edges).toEqual([
    { fromStep: "a", fromPort: "out", toStep: "b", toPort: "in" },
  ]);
});

/** contractKeys is every field name the ApplyOperation contract declares,
 * gathered from the GENERATED descriptor rather than typed out: a field added
 * to the proto is allowed the moment it is generated, and nothing else ever
 * is. */
function contractKeys(
  schema: DescMessage,
  seen = new Set<DescMessage>(),
  keys = new Set<string>(),
): Set<string> {
  if (seen.has(schema)) {
    return keys;
  }
  seen.add(schema);
  for (const field of schema.fields) {
    keys.add(field.name);
    keys.add(field.jsonName);
    if (field.message !== undefined) {
      contractKeys(field.message, seen, keys);
    }
  }
  return keys;
}

/** stringsIn collects every string value anywhere in a JSON value. */
function stringsIn(value: unknown, found: string[] = []): string[] {
  if (Array.isArray(value)) {
    for (const item of value) {
      stringsIn(item, found);
    }
    return found;
  }
  if (typeof value === "object" && value !== null) {
    for (const nested of Object.values(value)) {
      stringsIn(nested, found);
    }
    return found;
  }
  if (typeof value === "string") {
    found.push(value);
  }
  return found;
}

/** keysIn collects every object key anywhere in a JSON value. */
function keysIn(value: unknown, found = new Set<string>()): Set<string> {
  if (Array.isArray(value)) {
    for (const item of value) {
      keysIn(item, found);
    }
    return found;
  }
  if (typeof value === "object" && value !== null) {
    for (const [key, nested] of Object.entries(value)) {
      found.add(key);
      keysIn(nested, found);
    }
  }
  return found;
}

test("node positions are in no operation and in no document", async ({
  page,
  request,
}) => {
  const seed = await seedPipeline(request);
  const sent = applyOperations(page);
  await openCanvas(page, seed);

  await addStep(page, "a", "blob-source");
  await addStep(page, "b", "blob-sink");
  await dragPort(page, "port-out-a-out", "port-in-b-in");
  await expect(page.getByTestId("edge-a-out-b-in")).toBeVisible();
  await setProperty(page, "a", "plugin_ref", "oci://example/producer:v1");

  const saved = await getPipeline(
    request,
    seed.pipelineId,
    await revisionId(page),
  );

  // Exactly the four edits that were made, and no fifth operation quietly
  // recording where the nodes ended up.
  expect(operationKinds(sent)).toEqual([
    "addStep",
    "addStep",
    "connect",
    "setProperty",
  ]);

  // The canvas IS laying out: a downstream step sits in a later column, and
  // those coordinates exist only in the browser.
  const upstream = await page.getByTestId("step-node-a").boundingBox();
  const downstream = await page.getByTestId("step-node-b").boundingBox();
  expect(upstream).not.toBeNull();
  expect(downstream).not.toBeNull();
  expect(upstream?.x ?? 0).toBeLessThan(downstream?.x ?? 0);

  const allowed = contractKeys(ApplyOperationRequestSchema);
  for (const body of sent) {
    const unknownKeys = [...keysIn(body)].filter((key) => !allowed.has(key));
    expect(
      unknownKeys,
      `ApplyOperation body carried ${JSON.stringify(body)}`,
    ).toEqual([]);
  }

  // A coordinate pair smuggled into a declared field - set_property
  // "position" = "0,140" is the obvious way - would pass every check above,
  // because every key it uses is in the contract.
  const coordinates = /^-?\d+(\.\d+)?\s*,\s*-?\d+(\.\d+)?$/;
  for (const value of [...stringsIn(sent), ...stringsIn(saved)]) {
    expect(value, "a coordinate pair reached the document").not.toMatch(
      coordinates,
    );
  }

  // Named explicitly, because this is the property and not a side effect of
  // the allowlist: nothing that could hold a coordinate is in the document
  // either, however it travelled there.
  const everyKey = new Set([
    ...sent.flatMap((b) => [...keysIn(b)]),
    ...keysIn(saved),
  ]);
  for (const forbidden of ["position", "positions", "layout", "x", "y"]) {
    expect([...everyKey]).not.toContain(forbidden);
  }
});
