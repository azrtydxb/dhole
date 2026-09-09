import { defineConfig, devices } from "@playwright/test";

/**
 * End-to-end configuration. This is NOT part of `make check`: it needs a real
 * control plane and a real browser, which is minutes rather than seconds, so
 * it lives on the integration path — `make web-e2e`, beside
 * `make test-integration` — and CI runs it there.
 *
 * The control plane is real: internal/api serving the generated
 * PipelineService, internal/identity authenticating a real service token,
 * internal/defstore over a real SQLite database. It is started by
 * `e2e/fixture` rather than by `dhole serve` because `dhole serve` does not
 * yet mount the API — internal/server assembles the bus, the scheduler and an
 * engine, and nothing calls api.Server.Handler. See e2e/fixture/main.go; that
 * program goes away the day the binary serves its own contract.
 */
const apiUrl = process.env.VITE_DHOLE_API_URL ?? "http://127.0.0.1:8080";
// localhost, not 127.0.0.1: `vite` binds to localhost, which on a
// dual-stack machine is ::1, and a readiness probe on the IPv4 literal
// then waits for a socket that was never opened.
const webUrl = process.env.DHOLE_WEB_URL ?? "http://localhost:5173";

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? "github" : "list",
  use: {
    baseURL: webUrl,
    trace: "on-first-retry",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: [
    {
      command: `go run ./e2e/fixture --addr ${new URL(apiUrl).host} --state .playwright`,
      // A fresh database on every start, so a run never depends on which
      // tests ran before it — which is also why this one is not reused.
      reuseExistingServer: false,
      // The port rather than a URL: every RPC on this plane is
      // authenticated, so an HTTP readiness probe would have to carry a
      // credential to get anything but a refusal.
      port: Number(new URL(apiUrl).port),
      timeout: 180_000,
    },
    {
      command: "npm run dev",
      url: webUrl,
      reuseExistingServer: !process.env.CI,
      timeout: 60_000,
      env: { VITE_DHOLE_API_URL: apiUrl },
    },
  ],
});
