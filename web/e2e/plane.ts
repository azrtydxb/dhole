/**
 * How the Playwright suite reaches the control plane.
 *
 * There is one arrangement and every spec shares it, because two specs that
 * each invent their own way to get a credential drift apart and then disagree
 * about which one is the real plane.
 *
 * The plane is `dhole serve` — the shipping binary, serving the one contract
 * the GUI, the CLI and agents share (ADR 0013). It mints a bootstrap
 * credential at start-up and writes it, mode 0600, beside its database, before
 * it opens the port Playwright waits for; that file is the suite's credential
 * for the default tenant. Anything else — a pipeline to edit, a second
 * tenant's token — comes from e2e/seed, which is what is left of the fixture
 * binary that used to stand in for the whole plane.
 */
import { readFileSync } from "node:fs";

import { expect, type APIRequestContext } from "@playwright/test";

/** Where the plane and the seeder are, matching playwright.config.ts. */
export const apiUrl = process.env.VITE_DHOLE_API_URL ?? "http://127.0.0.1:8080";
export const seedUrl = process.env.DHOLE_SEED_URL ?? "http://127.0.0.1:8081";

/** The tenant `dhole serve` bootstraps. */
export const defaultTenant = "default";

/** One empty pipeline and the revision to base the first edit on. */
export interface Seeded {
  pipelineId: string;
  revisionId: string;
}

/**
 * The credential `dhole serve` minted at start-up. There is no unauthenticated
 * call on this API, which is the point: a suite that can talk to the plane is
 * a suite carrying a real token, and a spec that forgets one sees the same
 * refusal a production caller would.
 */
export function bootstrapToken(): string {
  const path = new URL("../.playwright/bootstrap.token", import.meta.url);
  return readFileSync(path, "utf8").trim();
}

/**
 * A pipeline of the caller's own. Heads are per pipeline, so two tests sharing
 * one would conflict with each other for a reason neither is about.
 *
 * It goes through the seeder rather than the contract because the contract
 * cannot create a pipeline: ApplyOperation requires a base_revision, and no
 * RPC writes a first one. That is a real hole in ADR 0013 — the GUI cannot
 * create a pipeline either — and this helper is where it shows.
 */
export async function seedPipeline(
  request: APIRequestContext,
): Promise<Seeded> {
  const response = await request.post(`${seedUrl}/pipeline`);
  expect(response.ok(), await response.text()).toBe(true);
  return (await response.json()) as Seeded;
}

/**
 * A credential for a tenant other than the bootstrapped one — what a test
 * asserting that one tenant cannot see another's work needs. `dhole token
 * issue` is the command an operator uses; this is the same call, so the
 * browser side of the suite does not have to shell out.
 */
export async function tokenFor(
  request: APIRequestContext,
  tenant: string,
  subject = "e2e",
): Promise<string> {
  const response = await request.post(
    `${seedUrl}/token?tenant=${encodeURIComponent(tenant)}` +
      `&subject=${encodeURIComponent(subject)}`,
  );
  expect(response.ok(), await response.text()).toBe(true);
  return ((await response.json()) as { token: string }).token;
}

/**
 * A pipeline of a named shape, seeded because nothing creates one through the
 * contract: ApplyOperation needs a base revision, and no operation sets a
 * step's command. The shapes live in Go, beside the types they must satisfy.
 */
export async function seedShape(
  request: APIRequestContext,
  shape: "cacheable" | "impure",
): Promise<Seeded> {
  const response = await request.post(`${seedUrl}/shape/${shape}`);
  expect(
    response.ok(),
    `seeding the ${shape} pipeline: ${response.status()} ${await response.text()}`,
  ).toBeTruthy();
  return (await response.json()) as Seeded;
}
