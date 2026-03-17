import { defineConfig } from "vitest/config";
import path from "node:path";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["tests/conformance/**/*.test.ts"],
    exclude: ["node_modules", "dist"],
    testTimeout: 60000,
    hookTimeout: 60000,
    fileParallelism: false,
    maxWorkers: 1,
    minWorkers: 1,
    // Vitest JSON reporter supplements the per-scenario JSON written by the harness.
    // Run with: vitest run --reporter=verbose --config vitest.conformance.config.ts
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
});
