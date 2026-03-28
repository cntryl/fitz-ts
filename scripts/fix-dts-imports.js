import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptPath = fileURLToPath(import.meta.url);
const repoRoot = resolve(scriptPath, "..", "..");
const typesRoot = join(repoRoot, "dist", "types");

const importLikePattern =
  /((?:from|import)\s*["'])(\.[^"']*)(["'])|((?:import|require)\(\s*["'])(\.[^"']*)(["']\s*\))/g;

function hasExplicitExtension(specifier) {
  return /\.[a-z0-9]+$/i.test(specifier);
}

function normalizeSpecifier(specifier, filePath) {
  if (!specifier.startsWith(".")) {
    return specifier;
  }
  if (hasExplicitExtension(specifier)) {
    return specifier;
  }

  const resolved = resolve(filePath, "..", specifier);
  const asFile = `${resolved}.d.ts`;
  const asDirectory = join(resolved, "index.d.ts");

  if (statSafe(asFile)?.isFile()) {
    return `${specifier}.js`;
  }
  if (statSafe(asDirectory)?.isFile()) {
    return `${specifier}/index.js`;
  }

  throw new Error(
    `Unable to resolve declaration import '${specifier}' from ${relative(repoRoot, filePath)}`,
  );
}

function statSafe(path) {
  try {
    return statSync(path);
  } catch {
    return null;
  }
}

function rewriteFile(filePath) {
  const original = readFileSync(filePath, "utf8");
  const rewritten = original.replace(
    importLikePattern,
    (match, prefixA, specifierA, suffixA, prefixB, specifierB, suffixB) => {
      const prefix = prefixA ?? prefixB;
      const specifier = specifierA ?? specifierB;
      const suffix = suffixA ?? suffixB;
      const normalized = normalizeSpecifier(specifier, filePath);
      return `${prefix}${normalized}${suffix}`;
    },
  );

  if (rewritten !== original) {
    writeFileSync(filePath, rewritten);
  }
}

function walk(dirPath) {
  for (const entry of readdirSync(dirPath, { withFileTypes: true })) {
    const fullPath = join(dirPath, entry.name);
    if (entry.isDirectory()) {
      walk(fullPath);
      continue;
    }
    if (entry.isFile() && (fullPath.endsWith(".d.ts") || fullPath.endsWith(".d.cts"))) {
      rewriteFile(fullPath);
    }
  }
}

walk(typesRoot);
