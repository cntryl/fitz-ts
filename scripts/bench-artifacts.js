#!/usr/bin/env node

import { spawn } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const DEFAULT_TIERS = ["tier1", "tier2", "tier3", "tier4"];
const DEFAULT_THRESHOLDS = {
  rmePercent: 5,
  cvPercent: 5,
  minChangePercent: 3,
};
const schemaVersion = 1;
const generatedBy = "fitz-ts bench-artifacts";

const __filename = fileURLToPath(import.meta.url);
const repoRoot = path.resolve(path.dirname(__filename), "..");

function normalizePath(value) {
  return value.replaceAll("\\", "/");
}

function isFiniteNumber(value) {
  return typeof value === "number" && Number.isFinite(value);
}

function toFiniteNumber(value) {
  const numberValue = Number(value);
  return Number.isFinite(numberValue) ? numberValue : undefined;
}

function median(values) {
  if (values.length === 0) return undefined;
  const sorted = [...values].sort((left, right) => left - right);
  const midpoint = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[midpoint];
  return (sorted[midpoint - 1] + sorted[midpoint]) / 2;
}

function coefficientOfVariation(values) {
  if (values.length < 2) return 0;
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  if (mean === 0) return 0;
  const variance = values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;
  return (Math.sqrt(variance) / mean) * 100;
}

function minValidRunsFor(expectedRuns) {
  return expectedRuns >= 5 ? 4 : expectedRuns;
}

function formatNumber(value, digits = 2) {
  if (!isFiniteNumber(value)) return "n/a";
  return value.toLocaleString("en-US", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

function formatInteger(value) {
  if (!isFiniteNumber(value)) return "n/a";
  return Math.round(value).toLocaleString("en-US");
}

function escapeMarkdown(value) {
  return String(value).replaceAll("|", "\\|");
}

function formatTimestamp(date) {
  return date
    .toISOString()
    .replaceAll(":", "-")
    .replace(/\.\d{3}Z$/, "Z");
}

function inferTier(filePath) {
  const match = normalizePath(filePath).match(/(?:^|\/)benches\/(tier\d)\//);
  return match?.[1] ?? "unknown";
}

function relativeFilePath(filePath, root) {
  const normalized = normalizePath(filePath);
  if (!path.isAbsolute(filePath)) return normalized;
  return normalizePath(path.relative(root, filePath));
}

function groupNameFromFullName(fullName, filePath) {
  if (typeof fullName !== "string" || fullName.length === 0) return "default";
  const parts = fullName.split(" > ");
  if (parts.length <= 1) return fullName;
  const normalizedFile = normalizePath(filePath);
  if (
    normalizePath(parts[0]) === normalizedFile ||
    normalizedFile.endsWith(normalizePath(parts[0]))
  ) {
    return parts.slice(1).join(" > ");
  }
  return parts.slice(1).join(" > ") || fullName;
}

export function flattenVitestBenchJson(json, options = {}) {
  const root = options.repoRoot ?? process.cwd();
  const runFilePath = options.runFilePath;
  const entries = [];
  const files = Array.isArray(json?.files) ? json.files : [];

  for (const file of files) {
    const rawFilePath = typeof file?.filepath === "string" ? file.filepath : "unknown";
    const filePath = relativeFilePath(rawFilePath, root);
    const tier = options.tier ?? inferTier(filePath);
    const groups = Array.isArray(file?.groups) ? file.groups : [];

    for (const group of groups) {
      const groupName = groupNameFromFullName(group?.fullName, filePath);
      const benchmarks = Array.isArray(group?.benchmarks) ? group.benchmarks : [];

      for (const benchmark of benchmarks) {
        const name = String(benchmark?.name ?? "unknown");
        const hz = toFiniteNumber(benchmark?.hz);
        const period = toFiniteNumber(benchmark?.period);
        const rme = toFiniteNumber(benchmark?.rme);
        const sampleCount = toFiniteNumber(benchmark?.sampleCount);
        const id = `${tier} / ${filePath} / ${groupName} / ${name}`;

        entries.push({
          id,
          tier,
          filepath: filePath,
          group: groupName,
          name,
          hz,
          period,
          rme,
          sampleCount,
          runFilePath,
          valid:
            hz !== undefined &&
            hz > 0 &&
            period !== undefined &&
            period > 0 &&
            rme !== undefined &&
            sampleCount !== undefined &&
            sampleCount > 0,
        });
      }
    }
  }

  return entries;
}

export function aggregateBenchmarks(entries, options = {}) {
  const expectedRuns = options.expectedRuns ?? 5;
  const minValidRuns = options.minValidRuns ?? minValidRunsFor(expectedRuns);
  const rmeThreshold = options.rmePercent ?? DEFAULT_THRESHOLDS.rmePercent;
  const cvThreshold = options.cvPercent ?? DEFAULT_THRESHOLDS.cvPercent;
  const byId = new Map();

  for (const entry of entries) {
    const grouped = byId.get(entry.id) ?? [];
    grouped.push(entry);
    byId.set(entry.id, grouped);
  }

  const benchmarks = [];
  const failures = [];

  for (const [id, groupedEntries] of [...byId.entries()].sort(([left], [right]) =>
    left.localeCompare(right),
  )) {
    const validEntries = groupedEntries.filter((entry) => entry.valid);
    const hzValues = validEntries.map((entry) => entry.hz);
    const periodValues = validEntries.map((entry) => entry.period);
    const rmeValues = validEntries.map((entry) => entry.rme);
    const sampleCounts = validEntries.map((entry) => entry.sampleCount);
    const rawRunFilePaths = validEntries
      .map((entry) => entry.runFilePath)
      .filter((value) => typeof value === "string");
    const maxRme = rmeValues.length === 0 ? undefined : Math.max(...rmeValues);
    const runCvPercent = coefficientOfVariation(hzValues);
    const benchmarkFailures = [];

    if (validEntries.length < minValidRuns) {
      benchmarkFailures.push(
        `valid runs ${validEntries.length}/${expectedRuns} below required ${minValidRuns}`,
      );
    }

    if (maxRme !== undefined && maxRme > rmeThreshold) {
      benchmarkFailures.push(`max RME ${formatNumber(maxRme)}% exceeds ${rmeThreshold}%`);
    }

    if (runCvPercent > cvThreshold) {
      benchmarkFailures.push(`run CV ${formatNumber(runCvPercent)}% exceeds ${cvThreshold}%`);
    }

    const first = groupedEntries[0];
    const benchmark = {
      id,
      tier: first.tier,
      filepath: first.filepath,
      group: first.group,
      name: first.name,
      validRuns: validEntries.length,
      expectedRuns,
      medianHz: median(hzValues),
      medianPeriod: median(periodValues),
      maxRme,
      runCvPercent,
      sampleCounts,
      rawRunFilePaths,
      usable: benchmarkFailures.length === 0,
      failures: benchmarkFailures,
    };

    benchmarks.push(benchmark);
    for (const failure of benchmarkFailures) {
      failures.push({ id, reason: failure });
    }
  }

  return {
    benchmarks,
    failures,
    ok: failures.length === 0,
  };
}

function flattenAttemptRecords(attemptRecords) {
  return attemptRecords.flatMap((attempt) => attempt.entries);
}

function collectAttemptRmeFailures(entries, rmeThreshold) {
  return entries
    .filter((entry) => entry.rme !== undefined && entry.rme > rmeThreshold)
    .map((entry) => ({
      id: entry.id,
      reason: `RME ${formatNumber(entry.rme)}% exceeds ${rmeThreshold}%`,
    }));
}

function collectCvFailures(attemptRecords, expectedRuns, thresholds) {
  const aggregated = aggregateBenchmarks(flattenAttemptRecords(attemptRecords), {
    expectedRuns,
    minValidRuns: minValidRunsFor(expectedRuns),
    rmePercent: thresholds.rmePercent,
    cvPercent: thresholds.cvPercent,
  });

  return aggregated.benchmarks
    .filter((benchmark) => benchmark.runCvPercent > thresholds.cvPercent)
    .map((benchmark) => ({
      id: benchmark.id,
      reason: `run CV ${formatNumber(benchmark.runCvPercent)}% exceeds ${thresholds.cvPercent}%`,
      benchmark,
    }));
}

function deviationScore(attemptRecord, cvFailures) {
  let score = 0;

  for (const failure of cvFailures) {
    const medianHz = failure.benchmark.medianHz;
    if (!isFiniteNumber(medianHz) || medianHz <= 0) continue;

    for (const entry of attemptRecord.entries) {
      if (entry.id !== failure.id || !entry.valid || !isFiniteNumber(entry.hz)) continue;
      score += (Math.abs(entry.hz - medianHz) / medianHz) * 100;
    }
  }

  return score;
}

function selectHighestDeviationAttempt(attemptRecords, cvFailures) {
  let selected = attemptRecords[0];
  let selectedScore = -1;

  for (const attemptRecord of attemptRecords) {
    const score = deviationScore(attemptRecord, cvFailures);
    if (score > selectedScore) {
      selected = attemptRecord;
      selectedScore = score;
    }
  }

  return selected;
}

function attemptSummary(attemptRecord, status, reason, failures = []) {
  return {
    attempt: attemptRecord.attempt,
    status,
    reason,
    runFiles: attemptRecord.runFiles,
    failures,
  };
}

function replaceAttemptSummary(summaries, attemptRecord, reason, failures) {
  const index = summaries.findIndex((summary) => summary.attempt === attemptRecord.attempt);
  if (index === -1) return;
  summaries[index] = attemptSummary(attemptRecord, "rejected", reason, failures);
}

function acceptedRunFiles(attemptRecords) {
  return attemptRecords.flatMap((attempt) => attempt.runFiles);
}

export function selectBenchmarkAttempts(attemptRecords, options = {}) {
  const runs = options.runs ?? 5;
  const signalMode = options.signalMode ?? runs >= 5;
  const targetRuns = signalMode ? 5 : runs;
  const maxAttempts = options.maxAttempts ?? (signalMode ? 10 : runs);
  const thresholds = {
    ...DEFAULT_THRESHOLDS,
    ...options.thresholds,
  };
  const summaries = [];
  const acceptedRecords = [];
  let done = false;

  for (const attemptRecord of attemptRecords.slice(0, maxAttempts)) {
    const rmeFailures = signalMode
      ? collectAttemptRmeFailures(attemptRecord.entries, thresholds.rmePercent)
      : [];

    if (rmeFailures.length > 0) {
      summaries.push(
        attemptSummary(
          attemptRecord,
          "rejected",
          `attempt RME exceeds ${thresholds.rmePercent}%`,
          rmeFailures,
        ),
      );
      continue;
    }

    acceptedRecords.push(attemptRecord);
    summaries.push(attemptSummary(attemptRecord, "accepted"));

    if (!signalMode) {
      done = acceptedRecords.length >= targetRuns;
      if (done) break;
      continue;
    }

    if (acceptedRecords.length < targetRuns) continue;

    const cvFailures = collectCvFailures(acceptedRecords, targetRuns, thresholds);
    if (cvFailures.length === 0) {
      done = true;
      break;
    }

    if (summaries.length >= maxAttempts) break;

    const rejectedAttempt = selectHighestDeviationAttempt(acceptedRecords, cvFailures);
    acceptedRecords.splice(acceptedRecords.indexOf(rejectedAttempt), 1);
    replaceAttemptSummary(
      summaries,
      rejectedAttempt,
      "largest aggregate deviation across CV-failing rows",
      cvFailures.map(({ id, reason }) => ({ id, reason })),
    );
  }

  const processedAttempts = summaries.length;
  const finalCvFailures =
    signalMode && acceptedRecords.length >= targetRuns
      ? collectCvFailures(acceptedRecords, targetRuns, thresholds)
      : [];
  const failures = [];

  if (signalMode && acceptedRecords.length < targetRuns && processedAttempts >= maxAttempts) {
    failures.push({
      id: "<runner>",
      reason: `accepted attempts ${acceptedRecords.length}/${targetRuns} below required ${targetRuns} after ${maxAttempts} attempts`,
    });
  }

  if (signalMode && finalCvFailures.length > 0 && processedAttempts >= maxAttempts) {
    failures.push({
      id: "<runner>",
      reason: `max attempts ${maxAttempts} exhausted before CV gates passed`,
    });
  }

  return {
    attempts: summaries,
    entries: flattenAttemptRecords(acceptedRecords),
    acceptedRunFiles: acceptedRunFiles(acceptedRecords),
    rejectedRunFiles: summaries
      .filter((summary) => summary.status === "rejected")
      .flatMap((summary) => summary.runFiles),
    targetRuns,
    maxAttempts,
    done,
    failures,
  };
}

export function shouldFailStableRun(summary) {
  return summary.signalMode !== false && !summary.ok;
}

export function buildSummary(options) {
  const thresholds = {
    ...DEFAULT_THRESHOLDS,
    ...options.thresholds,
  };
  const signalMode = options.signalMode ?? options.runs >= 5;
  const aggregated = aggregateBenchmarks(options.entries, {
    expectedRuns: options.runs,
    minValidRuns: minValidRunsFor(options.runs),
    rmePercent: thresholds.rmePercent,
    cvPercent: thresholds.cvPercent,
  });
  const failures = [...(options.runnerFailures ?? []), ...aggregated.failures];

  return {
    schemaVersion,
    generatedBy,
    createdAt: options.createdAt,
    artifactDir: options.artifactDir,
    repoRoot: options.repoRoot,
    tiers: options.tiers,
    runs: options.runs,
    signalMode,
    thresholds,
    runFiles: options.runFiles,
    acceptedRunFiles: options.acceptedRunFiles ?? options.runFiles,
    rejectedRunFiles: options.rejectedRunFiles ?? [],
    attempts: options.attempts ?? [],
    maxAttempts: options.maxAttempts ?? options.runs,
    benchmarks: aggregated.benchmarks,
    failures,
    ok: failures.length === 0,
  };
}

export function compareSummaries(baseline, current, options = {}) {
  const minChangePercent = options.minChangePercent ?? DEFAULT_THRESHOLDS.minChangePercent;
  const baselineBenchmarks = new Map((baseline.benchmarks ?? []).map((bench) => [bench.id, bench]));
  const currentBenchmarks = new Map((current.benchmarks ?? []).map((bench) => [bench.id, bench]));
  const ids = [...new Set([...baselineBenchmarks.keys(), ...currentBenchmarks.keys()])].sort(
    (left, right) => left.localeCompare(right),
  );
  const regressions = [];
  const improvements = [];
  const unchanged = [];
  const unusable = [];

  for (const id of ids) {
    const baselineBench = baselineBenchmarks.get(id);
    const currentBench = currentBenchmarks.get(id);

    if (!baselineBench || !currentBench) {
      unusable.push({
        id,
        reason: baselineBench ? "missing from current summary" : "missing from baseline summary",
      });
      continue;
    }

    if (
      !baselineBench.usable ||
      !currentBench.usable ||
      !isFiniteNumber(baselineBench.medianHz) ||
      !isFiniteNumber(currentBench.medianHz) ||
      baselineBench.medianHz <= 0
    ) {
      unusable.push({
        id,
        reason: "baseline or current benchmark failed noise gates",
      });
      continue;
    }

    const deltaPercent =
      ((currentBench.medianHz - baselineBench.medianHz) / baselineBench.medianHz) * 100;
    const noiseThreshold = Math.max(
      minChangePercent,
      (baselineBench.runCvPercent ?? 0) + (currentBench.runCvPercent ?? 0),
    );
    const comparison = {
      id,
      baselineHz: baselineBench.medianHz,
      currentHz: currentBench.medianHz,
      deltaPercent,
      thresholdPercent: noiseThreshold,
      baselineCvPercent: baselineBench.runCvPercent ?? 0,
      currentCvPercent: currentBench.runCvPercent ?? 0,
    };

    if (Math.abs(deltaPercent) < noiseThreshold) {
      unchanged.push(comparison);
    } else if (deltaPercent < 0) {
      regressions.push(comparison);
    } else {
      improvements.push(comparison);
    }
  }

  return {
    schemaVersion,
    generatedBy,
    baseline: baseline.artifactDir,
    current: current.artifactDir,
    regressions,
    improvements,
    unchanged,
    unusable,
    ok: regressions.length === 0,
  };
}

export function formatSummaryMarkdown(summary) {
  const status = summary.signalMode === false ? "SMOKE" : summary.ok ? "PASS" : "FAIL";
  const lines = [
    "# Benchmark Summary",
    "",
    `- Created: ${summary.createdAt}`,
    `- Tiers: ${summary.tiers.join(", ")}`,
    `- Runs: ${summary.runs}`,
    `- Max attempts: ${summary.maxAttempts ?? summary.runs}`,
    `- Accepted run files: ${(summary.acceptedRunFiles ?? summary.runFiles).length}`,
    `- Rejected run files: ${(summary.rejectedRunFiles ?? []).length}`,
    `- Signal mode: ${summary.signalMode === false ? "no" : "yes"}`,
    `- Noise gates: RME <= ${summary.thresholds.rmePercent}%, CV <= ${summary.thresholds.cvPercent}%`,
    `- Status: ${status}`,
    "",
    "## Benchmarks",
    "",
    "| Benchmark | Median hz | Median period ms | CV % | Max RME % | Valid runs | Status |",
    "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
  ];

  for (const bench of summary.benchmarks) {
    lines.push(
      `| ${escapeMarkdown(bench.id)} | ${formatInteger(bench.medianHz)} | ${formatNumber(
        bench.medianPeriod,
        6,
      )} | ${formatNumber(bench.runCvPercent)} | ${formatNumber(bench.maxRme)} | ${
        bench.validRuns
      }/${bench.expectedRuns} | ${bench.usable ? "ok" : "noisy"} |`,
    );
  }

  if (summary.failures.length > 0) {
    lines.push("", "## Failures", "");
    for (const failure of summary.failures) {
      lines.push(`- ${failure.id}: ${failure.reason}`);
    }
  }

  if ((summary.attempts ?? []).length > 0) {
    lines.push(
      "",
      "## Attempts",
      "",
      "| Attempt | Status | Files | Reason |",
      "| ---: | --- | ---: | --- |",
    );

    for (const attempt of summary.attempts) {
      lines.push(
        `| ${attempt.attempt} | ${attempt.status} | ${attempt.runFiles.length} | ${escapeMarkdown(
          attempt.reason ?? "",
        )} |`,
      );
    }
  }

  lines.push("");
  return lines.join("\n");
}

export function formatCompareMarkdown(report) {
  const lines = [
    "# Benchmark Compare",
    "",
    `- Baseline: ${report.baseline}`,
    `- Current: ${report.current}`,
    `- Status: ${report.ok ? "PASS" : "FAIL"}`,
    "",
  ];

  const appendSection = (title, rows) => {
    lines.push(`## ${title}`, "");
    if (rows.length === 0) {
      lines.push("None.", "");
      return;
    }
    lines.push("| Benchmark | Baseline hz | Current hz | Delta % | Threshold % |");
    lines.push("| --- | ---: | ---: | ---: | ---: |");
    for (const row of rows) {
      lines.push(
        `| ${escapeMarkdown(row.id)} | ${formatInteger(row.baselineHz)} | ${formatInteger(
          row.currentHz,
        )} | ${formatNumber(row.deltaPercent)} | ${formatNumber(row.thresholdPercent)} |`,
      );
    }
    lines.push("");
  };

  appendSection("Regressions", report.regressions);
  appendSection("Improvements", report.improvements);
  appendSection("Unchanged", report.unchanged);

  lines.push("## Noisy or Unusable", "");
  if (report.unusable.length === 0) {
    lines.push("None.", "");
  } else {
    for (const row of report.unusable) {
      lines.push(`- ${row.id}: ${row.reason}`);
    }
    lines.push("");
  }

  return lines.join("\n");
}

function parseArgs(argv) {
  const [command = "stable", ...rest] = argv;
  const options = { command, positionals: [] };

  for (let index = 0; index < rest.length; index += 1) {
    const arg = rest[index];
    if (!arg.startsWith("--")) {
      options.positionals.push(arg);
      continue;
    }

    const [rawName, inlineValue] = arg.slice(2).split("=", 2);
    const value = inlineValue ?? rest[index + 1];
    if (inlineValue === undefined) index += 1;

    options[rawName] = value;
  }

  return options;
}

function parseTiers(value) {
  if (!value || value === "all") return DEFAULT_TIERS;
  return String(value)
    .split(",")
    .map((tier) => tier.trim())
    .filter(Boolean);
}

function parseRuns(value) {
  const runs = Number(value ?? 5);
  if (!Number.isInteger(runs) || runs < 1) {
    throw new Error(`Invalid --runs value: ${value}`);
  }
  return runs;
}

function parseMaxAttempts(value, defaultValue) {
  const maxAttempts = Number(value ?? defaultValue);
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1) {
    throw new Error(`Invalid --maxAttempts value: ${value}`);
  }
  return maxAttempts;
}

function readJsonFile(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function resolveSummaryPath(value) {
  if (!value) throw new Error("Expected summary path");
  const resolved = path.resolve(repoRoot, value);
  const stat = fs.statSync(resolved);
  return stat.isDirectory() ? path.join(resolved, "summary.json") : resolved;
}

function assertGeneratedSummary(summary, filePath) {
  if (summary?.schemaVersion !== schemaVersion || summary?.generatedBy !== generatedBy) {
    throw new Error(`${filePath} is not a generated fitz-ts benchmark summary`);
  }
}

async function runCommand(command, args, cwd) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      env: { ...process.env, NODE_ENV: process.env.NODE_ENV ?? "production" },
      stdio: "inherit",
    });

    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
        return;
      }
      reject(new Error(`${command} ${args.join(" ")} exited with code ${code}`));
    });
  });
}

function listBenchFiles(tier) {
  const tierDir = path.join(repoRoot, "benches", tier);
  return fs
    .readdirSync(tierDir)
    .filter((fileName) => fileName.endsWith(".bench.ts"))
    .sort((left, right) => left.localeCompare(right))
    .map((fileName) => normalizePath(path.join("benches", tier, fileName)));
}

async function runVitestBench(tier, outputJson) {
  const vitestBin = path.join(
    repoRoot,
    "node_modules",
    ".bin",
    process.platform === "win32" ? "vitest.cmd" : "vitest",
  );
  const benchFiles = listBenchFiles(tier);
  if (benchFiles.length === 0) {
    throw new Error(`No benchmark files found for ${tier}`);
  }
  const args = [
    "bench",
    "--run",
    "--project",
    tier,
    "--reporter=default",
    "--outputJson",
    outputJson,
    "--no-file-parallelism",
    "--maxWorkers=1",
    ...benchFiles,
  ];
  await runCommand(vitestBin, args, repoRoot);
}

async function stableCommand(options) {
  const runs = parseRuns(options.runs);
  const signalMode = runs >= 5;
  const maxAttempts = parseMaxAttempts(options.maxAttempts, signalMode ? 10 : runs);
  const tiers = parseTiers(options.tiers);
  const createdAt = new Date().toISOString();
  const timestamp = options.timestamp ?? formatTimestamp(new Date(createdAt));
  const artifactDir = path.resolve(repoRoot, options.outputDir ?? `artifacts/bench/${timestamp}`);
  const attemptRecords = [];

  fs.mkdirSync(artifactDir, { recursive: true });

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const runFiles = [];
    const entries = [];

    for (const tier of tiers) {
      const outputJson = path.join(
        artifactDir,
        `run-${String(attempt).padStart(2, "0")}-${tier}.json`,
      );
      await runVitestBench(tier, outputJson);
      const json = readJsonFile(outputJson);
      runFiles.push(normalizePath(path.relative(repoRoot, outputJson)));
      entries.push(
        ...flattenVitestBenchJson(json, {
          repoRoot,
          tier,
          runFilePath: normalizePath(path.relative(repoRoot, outputJson)),
        }),
      );
    }

    attemptRecords.push({ attempt, runFiles, entries });

    const selection = selectBenchmarkAttempts(attemptRecords, {
      runs,
      maxAttempts,
      signalMode,
    });
    if (selection.done) break;
  }

  const selection = selectBenchmarkAttempts(attemptRecords, {
    runs,
    maxAttempts,
    signalMode,
  });
  const allRunFiles = attemptRecords.flatMap((attempt) => attempt.runFiles);
  const summary = buildSummary({
    entries: selection.entries,
    runs: selection.targetRuns,
    tiers,
    runFiles: allRunFiles,
    acceptedRunFiles: selection.acceptedRunFiles,
    rejectedRunFiles: selection.rejectedRunFiles,
    attempts: selection.attempts,
    maxAttempts,
    runnerFailures: selection.failures,
    repoRoot,
    artifactDir: normalizePath(path.relative(repoRoot, artifactDir)),
    createdAt,
    signalMode,
  });
  const summaryJsonPath = path.join(artifactDir, "summary.json");
  const summaryMarkdownPath = path.join(artifactDir, "summary.md");
  fs.writeFileSync(summaryJsonPath, `${JSON.stringify(summary, null, 2)}\n`);
  fs.writeFileSync(summaryMarkdownPath, formatSummaryMarkdown(summary));
  console.log(formatSummaryMarkdown(summary));

  if (shouldFailStableRun(summary)) {
    process.exitCode = 1;
  }
}

function compareCommand(options) {
  const baselinePath = resolveSummaryPath(options.baseline);
  const currentPath = resolveSummaryPath(options.current);
  const baseline = readJsonFile(baselinePath);
  const current = readJsonFile(currentPath);
  assertGeneratedSummary(baseline, baselinePath);
  assertGeneratedSummary(current, currentPath);

  const report = compareSummaries(baseline, current);
  console.log(formatCompareMarkdown(report));
  if (!report.ok) {
    process.exitCode = 1;
  }
}

async function main(argv) {
  const options = parseArgs(argv);
  if (options.command === "stable") {
    await stableCommand(options);
    return;
  }
  if (options.command === "compare") {
    compareCommand(options);
    return;
  }

  throw new Error(`Unknown bench-artifacts command: ${options.command}`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === __filename) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
