import { defineConfig } from "vite-plus";

export default defineConfig({
  fmt: {
    printWidth: 80,
    sortPackageJson: false,
  },
  pack: {
    entry: ["src/index.ts"],
  },
});
