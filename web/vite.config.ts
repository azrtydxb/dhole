/// <reference types="vitest/config" />
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    // `dhole serve` in the Playwright fixture; override with VITE_DHOLE_API_URL
    // when the control plane is elsewhere.
    proxy: {
      "/dhole.v1.": {
        target: process.env.VITE_DHOLE_API_URL ?? "http://127.0.0.1:8080",
        changeOrigin: true,
      },
    },
  },
  optimizeDeps: {
    // Vite pre-bundles the dependencies it finds by crawling from index.html.
    // The panel harness is not reachable from there — the e2e suite injects it
    // into a page that has already loaded — so the first request for it found
    // ajv un-bundled, re-optimized, and answered with a full page reload. That
    // reload is what destroyed the execution context `page.addScriptTag` was
    // running in, and it happened only on a cold dependency cache: exactly the
    // state CI starts every run in. Naming the harness as an entry puts its
    // imports in the first bundling pass, so no module this app serves can
    // discover a dependency late.
    entries: ["index.html", "src/panel/harness.tsx"],
  },
  test: {
    environment: "node",
    // Playwright owns e2e/; vitest must not try to run those specs.
    include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
  },
});
