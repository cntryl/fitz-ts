#!/usr/bin/env node

import { execFileSync, spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const repoRoot = path.resolve(path.dirname(__filename), "../..");
const artifactsDir = path.join(repoRoot, "artifacts");
const smokeDir = path.join(artifactsDir, "smoke");
const vpBin = path.join(
  repoRoot,
  "node_modules",
  ".bin",
  process.platform === "win32" ? "vp.cmd" : "vp",
);
const viteBin = path.join(
  repoRoot,
  "node_modules",
  ".bin",
  process.platform === "win32" ? "vite.cmd" : "vite",
);
const tscBin = path.join(
  repoRoot,
  "node_modules",
  ".bin",
  process.platform === "win32" ? "tsc.cmd" : "tsc",
);

function run(command, args, options = {}) {
  const result = spawnSync(command, args, {
    cwd: repoRoot,
    stdio: "inherit",
    ...options,
  });

  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed with status ${result.status}`);
  }
}

function writeSmokeFile(relativePath, content) {
  fs.writeFileSync(path.join(smokeDir, relativePath), content.trimStart());
}

fs.mkdirSync(artifactsDir, { recursive: true });
fs.rmSync(smokeDir, { recursive: true, force: true });
fs.mkdirSync(smokeDir, { recursive: true });

const packOutput = execFileSync("npm", ["pack", "--pack-destination", artifactsDir], {
  cwd: repoRoot,
  encoding: "utf8",
  stdio: ["ignore", "pipe", "inherit"],
})
  .trim()
  .split(/\r?\n/);
const tarballName = packOutput.at(-1);

if (!tarballName) {
  throw new Error("npm pack did not report a tarball");
}

const tarballPath = path.join(artifactsDir, tarballName);

run("npm", ["init", "-y"], { cwd: smokeDir, stdio: "ignore" });
run("npm", ["install", tarballPath], { cwd: smokeDir });

run(
  "node",
  [
    "--input-type=module",
    "--eval",
    `
      const root = await import("@cntryl/fitz");
      const node = await import("@cntryl/fitz/node");
      if (typeof root.createClient !== "function") throw new Error("root createClient missing");
      if (typeof node.createClient !== "function") throw new Error("node createClient missing");
      const client = root.createClient({ url: "tcp://localhost:4090", transport: "tcp" });
      const nodeClient = node.createClient({ url: "tcp://localhost:4090", transport: "tcp" });
      if (typeof client.connect !== "function") throw new Error("root client invalid");
      if (typeof nodeClient.connect !== "function") throw new Error("node client invalid");
    `,
  ],
  { cwd: smokeDir },
);

run(
  "node",
  [
    "--eval",
    `
      const root = require("@cntryl/fitz");
      const node = require("@cntryl/fitz/node");
      if (typeof root.createClient !== "function") throw new Error("CJS root createClient missing");
      if (typeof node.createClient !== "function") throw new Error("CJS node createClient missing");
    `,
  ],
  { cwd: smokeDir },
);

writeSmokeFile(
  "typecheck-node-esm.mts",
  `
    import { createClient, type LeaseAuthority } from "@cntryl/fitz";

    const client = createClient({ url: "tcp://localhost:4090", transport: "tcp" });
    client.lease.withLease("lease://realm/area/resource", 30, async signal => {
      signal.aborted satisfies boolean;
    });
    client.lease.withLease("lease://realm/area/resource", 30, async (_signal, authority) => {
      const snapshot: LeaseAuthority = authority;
      const token: bigint = snapshot.fencingToken;
      // @ts-expect-error Admission fencing tokens are bigint, never number.
      const invalid: number = snapshot.fencingToken;
      void token;
      void invalid;
    });
  `,
);

writeSmokeFile(
  "typecheck-node-cjs.cts",
  `
    import fitz = require("@cntryl/fitz/node");

    const client = fitz.createClient({ url: "tcp://localhost:4090", transport: "tcp" });
    client.lease.withLease("lease://realm/area/resource", 30, async signal => {
      signal.aborted satisfies boolean;
    });
    client.lease.withLease("lease://realm/area/resource", 30, async (_signal, authority) => {
      const token: bigint = authority.fencingToken;
      // @ts-expect-error Admission fencing tokens are bigint, never number.
      const invalid: number = authority.fencingToken;
      void token;
      void invalid;
    });
  `,
);

writeSmokeFile(
  "tsconfig.typecheck-node.json",
  `
    {
      "compilerOptions": {
        "module": "preserve",
        "moduleResolution": "bundler",
        "target": "es2022",
        "strict": true,
        "noEmit": true,
        "lib": ["es2022", "dom"]
      },
      "include": ["typecheck-node-esm.mts", "typecheck-node-cjs.cts"]
    }
  `,
);

writeSmokeFile(
  "browser-entry.ts",
  `
    import { createClient as createDefaultClient } from "@cntryl/fitz";
    import { createClient as createBrowserClient } from "@cntryl/fitz/browser";

    export const clients = [
      createDefaultClient({ url: "https://example.test/ws", transport: "auto" }),
      createBrowserClient({ url: "ws://example.test/ws", transport: "ws" }),
    ];
  `,
);

writeSmokeFile(
  "typecheck-root-browser.ts",
  `
    import { createClient, type LeaseAuthority } from "@cntryl/fitz";

    const client = createClient({ url: "ws://example.test/ws", transport: "ws" });

    client.config.transport satisfies "ws" | "auto";
    client.lease.withLease("lease://realm/area/resource", 30, async (_signal, authority) => {
      const snapshot: LeaseAuthority = authority;
      snapshot.fencingToken satisfies bigint;
    });

    // @ts-expect-error Browser-resolved root import must reject TCP transport.
    createClient({ url: "tcp://example.test:4090", transport: "tcp" });

    createClient({
      url: "ws://example.test/ws",
      transport: "ws",
      // @ts-expect-error Browser-resolved root import must reject WebSocket headers.
      webSocket: { headers: { Authorization: "x" } },
    });
  `,
);

writeSmokeFile(
  "typecheck-browser-subpath.ts",
  `
    import { createClient, type Client } from "@cntryl/fitz/browser";

    const client = createClient({ url: "ws://example.test/ws", transport: "ws" });
    const typedClient: Client = client;

    typedClient.config.transport satisfies "ws" | "auto";

    // @ts-expect-error Browser subpath must reject TCP transport.
    createClient({ url: "tcp://example.test:4090", transport: "tcp" });

    createClient({
      url: "ws://example.test/ws",
      transport: "ws",
      // @ts-expect-error Browser subpath must reject WebSocket headers.
      webSocket: { headers: { Authorization: "x" } },
    });
  `,
);

writeSmokeFile(
  "tsconfig.typecheck-root-browser.json",
  `
    {
      "compilerOptions": {
        "module": "esnext",
        "moduleResolution": "bundler",
        "customConditions": ["browser"],
        "target": "es2022",
        "strict": true,
        "noEmit": true,
        "lib": ["es2022", "dom"]
      },
      "include": ["typecheck-root-browser.ts"]
    }
  `,
);

writeSmokeFile(
  "tsconfig.typecheck-browser-subpath.json",
  `
    {
      "compilerOptions": {
        "module": "esnext",
        "moduleResolution": "bundler",
        "target": "es2022",
        "strict": true,
        "noEmit": true,
        "lib": ["es2022", "dom"]
      },
      "include": ["typecheck-browser-subpath.ts"]
    }
  `,
);

writeSmokeFile(
  "worker-entry.ts",
  `
    import { createClient } from "@cntryl/fitz/browser";

    const client = createClient({ url: "ws://example.test/ws", transport: "ws" });

    self.addEventListener("message", () => {
      self.postMessage(typeof client.connect);
    });
  `,
);

writeSmokeFile(
  "vite.browser.config.mjs",
  `
    import { defineConfig } from "vite";

    export default defineConfig({
      build: {
        emptyOutDir: true,
        outDir: "vite-browser",
        lib: {
          entry: "browser-entry.ts",
          formats: ["es"],
          fileName: "browser-entry",
        },
      },
    });
  `,
);

writeSmokeFile(
  "vite.worker.config.mjs",
  `
    import { defineConfig } from "vite";

    export default defineConfig({
      build: {
        emptyOutDir: true,
        outDir: "vite-worker",
        lib: {
          entry: "worker-entry.ts",
          formats: ["es"],
          fileName: "worker-entry",
        },
      },
    });
  `,
);

run(viteBin, ["build", "--config", "vite.browser.config.mjs"], { cwd: smokeDir });
run(viteBin, ["build", "--config", "vite.worker.config.mjs"], { cwd: smokeDir });
run(tscBin, ["--project", "tsconfig.typecheck-node.json"], { cwd: smokeDir });
run(tscBin, ["--project", "tsconfig.typecheck-root-browser.json"], { cwd: smokeDir });
run(tscBin, ["--project", "tsconfig.typecheck-browser-subpath.json"], { cwd: smokeDir });
run(
  vpBin,
  [
    "pack",
    "worker-entry.ts",
    "--platform",
    "browser",
    "--out-dir",
    "vp-worker",
    "--clean",
    "--dts=false",
  ],
  {
    cwd: smokeDir,
  },
);
