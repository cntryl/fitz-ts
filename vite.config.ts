import { defineConfig } from "vite-plus";

export default defineConfig({
  staged: {
    "*": "vp check --fix",
  },
  fmt: {
    ignorePatterns: ["dist/**", "node_modules/**", "coverage/**", ".vitest/**", "artifacts/**"],
  },
  lint: {
    ignorePatterns: ["dist/**", "node_modules/**", "coverage/**", ".vitest/**", "artifacts/**"],
    env: {
      builtin: true,
    },
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  pack: {
    entry: "src/index.ts",
    format: ["esm", "cjs"],
    platform: "node",
    deps: {
      neverBundle: ["ws"],
    },
    sourcemap: true,
    dts: false,
  },
  test: {
    globals: true,
    environment: "node",
    coverage: {
      provider: "v8",
      reporter: ["text", "json", "html"],
      exclude: ["node_modules/", "dist/", "tests/", "benches/"],
    },
    projects: [
      {
        extends: true,
        test: {
          name: "unit",
          include: ["tests/unit/**/*.test.ts", "src/**/*.test.ts"],
          exclude: ["node_modules", "dist", "tests/integration", "benches"],
        },
      },
      {
        extends: true,
        test: {
          name: "integration",
          include: ["tests/integration/**/*.test.ts"],
          exclude: ["node_modules", "dist"],
          testTimeout: 30000,
          hookTimeout: 60000,
          fileParallelism: false,
          maxWorkers: 1,
        },
      },
      {
        extends: true,
        test: {
          name: "conformance",
          include: ["tests/conformance/**/*.test.ts"],
          exclude: ["node_modules", "dist"],
          testTimeout: 60000,
          hookTimeout: 60000,
          fileParallelism: false,
          maxWorkers: 1,
        },
      },
    ],
  },
});
