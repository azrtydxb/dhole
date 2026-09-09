import { defineConfig, devices } from "@playwright/test";

/**
 * End-to-end configuration. This is NOT part of `make check`: it needs a real
 * control plane and a real browser, which is minutes rather than seconds, so
 * it lives on the integration path — `make web-e2e`, beside
 * `make test-integration` — and CI runs it there.
 *
 * The control plane is `dhole serve` itself. It used to be a fixture binary of
 * the suite's own, because the binary assembled a bus, a scheduler and an
 * engine and mounted no API; it now serves the one contract the GUI, the CLI
 * and agents share (ADR 0013), so the canvas is tested against the thing that
 * ships. The credential is the bootstrap token the plane mints and writes to
 * its state directory, and the only thing left beside it is e2e/seed, which
 * writes each test its own first revision — see that file for why the contract
 * cannot do it yet.
 */
const apiUrl = process.env.VITE_DHOLE_API_URL ?? "http://127.0.0.1:8080";
const seedUrl = process.env.DHOLE_SEED_URL ?? "http://127.0.0.1:8081";
// localhost, not 127.0.0.1: `vite` binds to localhost, which on a
// dual-stack machine is ::1, and a readiness probe on the IPv4 literal
// then waits for a socket that was never opened.
const webUrl = process.env.DHOLE_WEB_URL ?? "http://localhost:5173";
// The plane's state directory. It holds the SQLite database, the object
// stores, the embedded NATS data and — mode 0600 — the bootstrap credential
// the suite authenticates with. Gitignored.
const stateDir = ".playwright";

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
      // A scratch state directory per start: a plane that accumulated
      // yesterday's pipelines would make a passing test depend on which tests
      // ran before it — which is also why this one is not reused.
      //
      // --api-allowed-origin is what lets the page Vite serves on another
      // port talk to this one. The plane allows NO cross-origin call by
      // default, so this names the one origin the suite runs from rather
      // than opening the plane to any page on any site.
      command:
        `rm -rf ${stateDir} && go run ../cmd/dhole serve` +
        ` --api-addr ${new URL(apiUrl).host}` +
        ` --store-dsn ${stateDir}/dhole.db --blob-root ${stateDir}` +
        ` --api-allowed-origin ${webUrl}`,
      reuseExistingServer: false,
      // The port rather than a URL: every RPC on this plane is
      // authenticated, so an HTTP readiness probe would have to carry a
      // credential to get anything but a refusal.
      port: Number(new URL(apiUrl).port),
      timeout: 180_000,
    },
    {
      // Waits for the plane's port before it opens the database, so the
      // migrations are applied by the plane that owns it rather than raced
      // with it.
      command:
        `go run ./e2e/seed --addr ${new URL(seedUrl).host}` +
        ` --store-dsn ${stateDir}/dhole.db --wait-for ${new URL(apiUrl).host}`,
      reuseExistingServer: false,
      port: Number(new URL(seedUrl).port),
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
