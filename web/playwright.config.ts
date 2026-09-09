import { defineConfig, devices } from "@playwright/test";

/**
 * End-to-end configuration. This is NOT part of `make check`: it needs a real
 * control plane and a real browser, which is minutes rather than seconds, so
 * it lives on the integration path — `make web-e2e`, beside
 * `make test-integration` — and CI runs it there.
 *
 * The fixture is the product as a user gets it: `dhole serve` with no flags is
 * already the embedded mode (in-process bus, SQLite, filesystem blobs, a
 * hosted engine), started on a scratch state directory so a run never touches
 * the developer's own.
 */
const apiUrl = process.env.VITE_DHOLE_API_URL ?? "http://127.0.0.1:8080";
const webUrl = process.env.DHOLE_WEB_URL ?? "http://127.0.0.1:5173";

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
      command:
        "go run ./cmd/dhole serve --mode embedded " +
        "--store-dsn .playwright/dhole.db --blob-root .playwright",
      cwd: "..",
      url: apiUrl,
      reuseExistingServer: !process.env.CI,
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
