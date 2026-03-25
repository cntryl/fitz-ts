import { defineConfig } from "vite-plus";

export default defineConfig({
  staged: {
    "*": "vp check --fix",
  },
  fmt: {
    printWidth: 80,
    sortPackageJson: false,
  },
  pack: {
    entry: ["src/index.ts"],
  },
});
