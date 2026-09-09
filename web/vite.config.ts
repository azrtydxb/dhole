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
  test: {
    environment: "node",
    // Playwright owns e2e/; vitest must not try to run those specs.
    include: ["src/**/*.test.ts", "src/**/*.test.tsx"],
  },
});
