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
 * for the default tenant, and a pipeline to edit is created through the
 * contract like any other client would.
 *
 * The one thing e2e/seed is still for is a SECOND TENANT's credential. The
 * plane mints a bootstrap token for the default tenant only, and a suite
 * asserting that one tenant cannot see another's work needs a token for the
 * other one; `dhole token issue` is the operator command, and the seeder is
 * that call over HTTP so the browser side does not shell out.
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

/** rpc calls one RPC of the contract over Connect's JSON protocol. */
export async function rpc(
  request: APIRequestContext,
  method: string,
  body: unknown,
  token = bootstrapToken(),
): Promise<Record<string, unknown>> {
  const response = await request.post(
    `${apiUrl}/dhole.v1.PipelineService/${method}`,
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      data: body as Record<string, unknown>,
    },
  );
  expect(response.ok(), await response.text()).toBe(true);
  return (await response.json()) as Record<string, unknown>;
}

/** A counter, so each test's pipeline id is its own. Heads are per pipeline,
 * and two tests sharing one would conflict for a reason neither is about. */
let created = 0;

/**
 * A pipeline of the caller's own, created THROUGH THE CONTRACT.
 *
 * It used to go through e2e/seed, which wrote a first revision straight into
 * the plane's database, because ApplyOperation requires a base_revision and no
 * RPC wrote one — so the GUI could not create a pipeline either, and the suite
 * had to do something the product cannot. CreatePipeline closed that, and this
 * helper is now exactly what the canvas itself would call.
 */
export async function createPipeline(
  request: APIRequestContext,
): Promise<Seeded> {
  created += 1;
  const pipelineId = `canvas-e2e-${process.pid}-${created}`;
  const response = await rpc(request, "CreatePipeline", { pipelineId });
  const revision = (response as { revision?: { id?: string } }).revision;
  expect(revision?.id, JSON.stringify(response)).toBeTruthy();
  return { pipelineId, revisionId: revision?.id ?? "" };
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
