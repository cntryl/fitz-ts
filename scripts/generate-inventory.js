#!/usr/bin/env node
/*
 * generate-inventory.js
 *
 * Generate a repository inventory for fitz-ts.
 * Scans src/, benches/, tests/, docs/, scripts/, and top-level config files.
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const repoRoot = path.resolve(__dirname, "..");
const inventoryPath = path.join(repoRoot, "inventory.md");

const ignoredDirs = new Set([
  ".git",
  ".vite-hooks",
  ".vitest",
  ".turbo",
  "artifacts",
  "build",
  "coverage",
  "dist",
  "node_modules",
]);

const rootMarkdownFiles = new Set([
  "AGENTS.md",
  "CHANGELOG.md",
  "CLIENT_SPEC.md",
  "PERF_RESULTS.md",
  "README.md",
  "REBUILD_SUMMARY.md",
]);

const rootConfigFiles = new Set([
  ".gitattributes",
  ".gitignore",
  "compose.yml",
  "package-lock.json",
  "package.json",
  "tsconfig.json",
  "vite.config.ts",
  "vitest.conformance.config.ts",
  "vitest.integration.config.ts",
  "vitest.unit.config.ts",
]);

function unique(values) {
  return Array.from(new Set(values));
}

function isTypeScriptFile(filePath) {
  return /\.(ts|tsx|mts|cts)$/.test(filePath);
}

function isMarkdownFile(filePath) {
  return /\.(md|mdx)$/.test(filePath);
}

function isYamlFile(filePath) {
  return /\.(yml|yaml)$/.test(filePath);
}

function readText(filePath) {
  return fs.readFileSync(filePath, "utf8");
}

function walkFiles(startDir, visitor) {
  const entries = fs.readdirSync(startDir, { withFileTypes: true });
  entries.sort((left, right) => left.name.localeCompare(right.name));

  for (const entry of entries) {
    if (ignoredDirs.has(entry.name)) {
      continue;
    }

    const fullPath = path.join(startDir, entry.name);
    if (entry.isDirectory()) {
      walkFiles(fullPath, visitor);
      continue;
    }

    if (entry.isFile()) {
      visitor(fullPath);
    }
  }
}

function extractTypeScriptSymbols(content) {
  const symbols = {
    functions: [],
    classes: [],
    interfaces: [],
    types: [],
    enums: [],
    constants: [],
  };

  let match;

  const functionPattern = /(?:export\s+)?(?:async\s+)?function\s+(\w+)\s*\(/g;
  while ((match = functionPattern.exec(content))) {
    symbols.functions.push(match[1]);
  }

  const classPattern = /(?:export\s+)?class\s+(\w+)/g;
  while ((match = classPattern.exec(content))) {
    symbols.classes.push(match[1]);
  }

  const interfacePattern = /(?:export\s+)?interface\s+(\w+)/g;
  while ((match = interfacePattern.exec(content))) {
    symbols.interfaces.push(match[1]);
  }

  const typePattern = /(?:export\s+)?type\s+(\w+)\s*=/g;
  while ((match = typePattern.exec(content))) {
    symbols.types.push(match[1]);
  }

  const enumPattern = /(?:export\s+)?enum\s+(\w+)/g;
  while ((match = enumPattern.exec(content))) {
    symbols.enums.push(match[1]);
  }

  const constPattern = /(?:export\s+)?const\s+(\w+)\s*(?::|=)/g;
  while ((match = constPattern.exec(content))) {
    const name = match[1];
    const functionLikeAssignment = new RegExp(
      `(?:export\\s+)?const\\s+${name}\\s*=\\s*(?:async\\s*)?\\(`,
    );
    if (!functionLikeAssignment.test(content)) {
      symbols.constants.push(name);
    }
  }

  for (const key of Object.keys(symbols)) {
    symbols[key] = unique(symbols[key]).filter((symbol) => symbol.length > 1);
  }

  return symbols;
}

function extractBenchmarkNames(content) {
  const names = [];
  const benchPattern =
    /\bbench(?:\.(?:skip|only|todo|fails|concurrent|each))?\s*\(\s*(['"`])([^'"`]+)\1/g;
  const describePattern =
    /\bdescribe(?:\.(?:skip|only|todo|concurrent|each))?\s*\(\s*(['"`])([^'"`]+)\1/g;

  let match;
  while ((match = benchPattern.exec(content))) {
    names.push(match[2]);
  }
  while ((match = describePattern.exec(content))) {
    names.push(match[2]);
  }

  return unique(names);
}

function extractTestBehaviors(content) {
  const names = [];
  const testPattern =
    /\b(?:it|test)(?:\.(?:skip|only|todo|fails|concurrent|each))?\s*\(\s*(['"`])([^'"`]+)\1/g;

  let match;
  while ((match = testPattern.exec(content))) {
    names.push(match[2]);
  }

  return unique(names);
}

function collectInventory() {
  const sourceFiles = [];
  const benchFiles = [];
  const testFiles = [];
  const docsFiles = [];
  const scriptFiles = [];
  const configFiles = [];

  walkFiles(repoRoot, (fullPath) => {
    const relativePath = path.relative(repoRoot, fullPath).replace(/\\/g, "/");
    const baseName = path.basename(relativePath);

    if (relativePath === "inventory.md") {
      return;
    }

    if (relativePath.startsWith("src/") && isTypeScriptFile(fullPath)) {
      const content = readText(fullPath);
      sourceFiles.push({
        path: relativePath,
        lineCount: content.split(/\r?\n/).length,
        size: content.length,
        symbols: extractTypeScriptSymbols(content),
      });
      return;
    }

    if (relativePath.startsWith("benches/") && isTypeScriptFile(fullPath)) {
      const content = readText(fullPath);
      benchFiles.push({
        path: relativePath,
        lineCount: content.split(/\r?\n/).length,
        size: content.length,
        benchmarks: extractBenchmarkNames(content),
      });
      return;
    }

    if (relativePath.startsWith("tests/") && isTypeScriptFile(fullPath)) {
      const content = readText(fullPath);
      testFiles.push({
        path: relativePath,
        lineCount: content.split(/\r?\n/).length,
        size: content.length,
        behaviors: extractTestBehaviors(content),
      });
      return;
    }

    if (relativePath.startsWith("docs/") && isMarkdownFile(fullPath)) {
      docsFiles.push(relativePath);
      return;
    }

    if (relativePath.startsWith("scripts/") && !isTypeScriptFile(fullPath)) {
      scriptFiles.push(relativePath);
      return;
    }

    if (rootMarkdownFiles.has(baseName)) {
      docsFiles.push(relativePath);
      return;
    }

    if (
      rootConfigFiles.has(baseName) ||
      (relativePath.startsWith(".github/workflows/") && isYamlFile(fullPath))
    ) {
      configFiles.push(relativePath);
    }
  });

  sourceFiles.sort((left, right) => left.path.localeCompare(right.path));
  benchFiles.sort((left, right) => left.path.localeCompare(right.path));
  testFiles.sort((left, right) => left.path.localeCompare(right.path));
  docsFiles.sort((left, right) => left.localeCompare(right));
  scriptFiles.sort((left, right) => left.localeCompare(right));
  configFiles.sort((left, right) => left.localeCompare(right));

  return {
    sourceFiles,
    benchFiles,
    testFiles,
    docsFiles,
    scriptFiles,
    configFiles,
  };
}

function formatNames(values) {
  return values.length === 0
    ? "none"
    : values.map((value) => `\`${value}\``).join(", ");
}

function generateMarkdownInventory(inventory, packageJson) {
  const lines = [];

  lines.push("# fitz-ts Repository Inventory");
  lines.push("");
  lines.push(`Generated on: ${new Date().toISOString().slice(0, 10)}`);
  lines.push(`Package: ${packageJson.name} ${packageJson.version}`);
  lines.push(`Description: ${packageJson.description}`);
  lines.push("");

  lines.push("## Summary");
  lines.push("");
  lines.push(`- Source files: ${inventory.sourceFiles.length}`);
  lines.push(`- Benchmark files: ${inventory.benchFiles.length}`);
  lines.push(`- Test files: ${inventory.testFiles.length}`);
  lines.push(`- Docs files: ${inventory.docsFiles.length}`);
  lines.push(`- Script files: ${inventory.scriptFiles.length}`);
  lines.push(`- Config files: ${inventory.configFiles.length}`);
  lines.push("");

  const totalSourceSymbols = inventory.sourceFiles.reduce((total, file) => {
    return (
      total +
      file.symbols.functions.length +
      file.symbols.classes.length +
      file.symbols.interfaces.length +
      file.symbols.types.length +
      file.symbols.enums.length
    );
  }, 0);

  const totalBenchmarks = inventory.benchFiles.reduce(
    (total, file) => total + file.benchmarks.length,
    0,
  );
  const totalBehaviors = inventory.testFiles.reduce(
    (total, file) => total + file.behaviors.length,
    0,
  );

  lines.push(`- Source symbols discovered: ${totalSourceSymbols}`);
  lines.push(`- Benchmark cases discovered: ${totalBenchmarks}`);
  lines.push(`- Test cases discovered: ${totalBehaviors}`);
  lines.push("");

  lines.push("## Source Files");
  lines.push("");
  for (const file of inventory.sourceFiles) {
    const symbols = file.symbols;
    const summary = [];
    if (symbols.classes.length)
      summary.push(`classes: ${formatNames(symbols.classes)}`);
    if (symbols.interfaces.length)
      summary.push(`interfaces: ${formatNames(symbols.interfaces)}`);
    if (symbols.functions.length)
      summary.push(`functions: ${formatNames(symbols.functions)}`);
    if (symbols.types.length)
      summary.push(`types: ${formatNames(symbols.types)}`);
    if (symbols.enums.length)
      summary.push(`enums: ${formatNames(symbols.enums)}`);
    if (symbols.constants.length)
      summary.push(`constants: ${formatNames(symbols.constants)}`);

    lines.push(
      `- [${file.path}](${file.path}) - ${summary.length ? summary.join("; ") : "No top-level symbols found"}`,
    );
  }

  lines.push("");
  lines.push("## Benchmarks");
  lines.push("");
  for (const file of inventory.benchFiles) {
    lines.push(
      `- [${file.path}](${file.path}) - ${file.benchmarks.length} benchmark${file.benchmarks.length === 1 ? "" : "s"}`,
    );
    for (const benchmarkName of file.benchmarks.slice().sort()) {
      lines.push(`  - ${benchmarkName}`);
    }
  }

  lines.push("");
  lines.push("## Tests");
  lines.push("");
  for (const file of inventory.testFiles) {
    lines.push(
      `- [${file.path}](${file.path}) - ${file.behaviors.length} test case${file.behaviors.length === 1 ? "" : "s"}`,
    );
    for (const behavior of file.behaviors.slice().sort()) {
      lines.push(`  - ${behavior}`);
    }
  }

  lines.push("");
  lines.push("## Docs");
  lines.push("");
  for (const filePath of inventory.docsFiles) {
    lines.push(`- [${filePath}](${filePath})`);
  }

  lines.push("");
  lines.push("## Scripts");
  lines.push("");
  for (const filePath of inventory.scriptFiles) {
    lines.push(`- [${filePath}](${filePath})`);
  }

  lines.push("");
  lines.push("## Config");
  lines.push("");
  for (const filePath of inventory.configFiles) {
    lines.push(`- [${filePath}](${filePath})`);
  }

  return lines.join("\n");
}

function main() {
  const packageJson = JSON.parse(
    fs.readFileSync(path.join(repoRoot, "package.json"), "utf8"),
  );
  const inventory = collectInventory();
  const markdown = generateMarkdownInventory(inventory, packageJson);

  fs.writeFileSync(inventoryPath, markdown, "utf8");
  console.log(`Inventory generated: ${inventoryPath}`);
  console.log(`Source files: ${inventory.sourceFiles.length}`);
  console.log(`Benchmark files: ${inventory.benchFiles.length}`);
  console.log(`Test files: ${inventory.testFiles.length}`);
  console.log(`Docs files: ${inventory.docsFiles.length}`);
  console.log(`Script files: ${inventory.scriptFiles.length}`);
  console.log(`Config files: ${inventory.configFiles.length}`);
}

if (process.argv[1] && process.argv[1].endsWith("generate-inventory.js")) {
  main();
}
