import { defineConfig } from "vite-plus/test/config";

export default defineConfig({
  test: {
    globals: true,
    environment: "node",
    include: ["tests/integration/**/*.test.ts"],
    exclude: ["node_modules", "dist"],
    testTimeout: 30000,
    hookTimeout: 60000,
    fileParallelism: false,
    maxWorkers: 1,
  },
});
