import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = dirname(scriptDir);
const artifactsDir = join(repoRoot, "artifacts");

function run(command, args, cwd = repoRoot, capture = false) {
  const result =
    process.platform === "win32" && command === "npm"
      ? spawnSync("cmd.exe", ["/d", "/s", "/c", command, ...args], {
          cwd,
          stdio: capture ? ["ignore", "pipe", "inherit"] : "inherit",
          encoding: capture ? "utf8" : undefined,
        })
      : spawnSync(command, args, {
          cwd,
          stdio: capture ? ["ignore", "pipe", "inherit"] : "inherit",
          encoding: capture ? "utf8" : undefined,
        });

  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }

  return capture ? result.stdout.trim() : "";
}

mkdirSync(artifactsDir, { recursive: true });
const tarballStdout = run("npm", ["pack", "--pack-destination", "./artifacts"], repoRoot, true);
const tarballName = tarballStdout
  .split(/\r?\n/)
  .map((line) => line.trim())
  .filter(Boolean)
  .at(-1);
if (!tarballName) {
  console.error("npm pack did not report a tarball filename");
  process.exit(1);
}

const tarballPath = join(artifactsDir, tarballName);
const tempRoot = mkdtempSync(join(tmpdir(), "fitz-ts-pack-smoke-"));
const esmProject = join(tempRoot, "esm-consumer");
const cjsProject = join(tempRoot, "cjs-consumer");

for (const project of [esmProject, cjsProject]) {
  mkdirSync(project, { recursive: true });
}

writeFileSync(
  join(esmProject, "package.json"),
  JSON.stringify(
    {
      name: "fitz-ts-esm-consumer",
      private: true,
      type: "module",
    },
    null,
    2,
  ),
);
writeFileSync(
  join(esmProject, "tsconfig.json"),
  JSON.stringify(
    {
      compilerOptions: {
        module: "NodeNext",
        moduleResolution: "NodeNext",
        target: "ES2022",
        strict: true,
        noEmit: true,
      },
      include: ["consumer.ts"],
    },
    null,
    2,
  ),
);
writeFileSync(
  join(esmProject, "consumer.ts"),
  [
    'import { Client, ConnectionState, type ClientConfig } from "@cntryl/fitz";',
    'const config: ClientConfig = { url: "ws://localhost:4090/ws" };',
    "const client = new Client(config);",
    "void client;",
    "const state: ConnectionState = ConnectionState.Disconnected;",
    "void state;",
  ].join("\n"),
);

writeFileSync(
  join(cjsProject, "package.json"),
  JSON.stringify(
    {
      name: "fitz-ts-cjs-consumer",
      private: true,
    },
    null,
    2,
  ),
);
writeFileSync(
  join(cjsProject, "consumer.cjs"),
  [
    'const fitz = require("@cntryl/fitz");',
    'if (typeof fitz.Client !== "function") throw new Error("Client export missing from CommonJS bundle");',
    'if (!fitz.ConnectionState || fitz.ConnectionState.Disconnected !== "DISCONNECTED") throw new Error("ConnectionState export missing from CommonJS bundle");',
  ].join("\n"),
);

for (const project of [esmProject, cjsProject]) {
  run("npm", ["install", "--no-package-lock", "--no-save", tarballPath], project);
}

const tscPath = resolve(repoRoot, "node_modules", "typescript", "bin", "tsc");
if (!existsSync(tscPath)) {
  console.error(`TypeScript compiler not found at ${tscPath}`);
  process.exit(1);
}

run(process.execPath, [tscPath, "--project", "tsconfig.json"], esmProject);
run("node", ["consumer.cjs"], cjsProject);
run(
  "node",
  [
    "--input-type=module",
    "--eval",
    'const mod = await import("@cntryl/fitz"); if (typeof mod.Client !== "function") throw new Error("Client export missing from ESM bundle");',
  ],
  esmProject,
);

rmSync(tempRoot, { recursive: true, force: true });
