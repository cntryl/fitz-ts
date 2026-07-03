/// <reference types="node" />

import { describe, expect, it } from "vite-plus/test";

import {
  aggregateBenchmarks,
  buildSummary,
  compareSummaries,
  flattenVitestBenchJson,
  selectBenchmarkAttempts,
  shouldFailStableRun,
  type BenchmarkSummary,
  type BenchmarkAttemptRecord,
  type FlattenedBenchmark,
} from "../../../scripts/bench-artifacts.js";

function entry(id: string, run: number, hz: number, rme = 1): FlattenedBenchmark {
  return {
    id,
    tier: "tier1",
    filepath: "benches/tier1/hotpath.bench.ts",
    group: "group",
    name: id,
    hz,
    period: 1_000 / hz,
    rme,
    sampleCount: 100,
    runFilePath: `run-${run}.json`,
    valid: true,
  };
}

function attempt(run: number, entries: FlattenedBenchmark[]): BenchmarkAttemptRecord {
  return {
    attempt: run,
    runFiles: [`run-${run}.json`],
    entries,
  };
}

function summary(benchmarks: BenchmarkSummary["benchmarks"]): BenchmarkSummary {
  return {
    schemaVersion: 1,
    generatedBy: "fitz-ts bench-artifacts",
    createdAt: "2026-07-03T00:00:00.000Z",
    artifactDir: "artifacts/bench/test",
    repoRoot: "/repo",
    tiers: ["tier1"],
    runs: 5,
    signalMode: true,
    thresholds: {
      rmePercent: 5,
      cvPercent: 5,
      minChangePercent: 3,
    },
    runFiles: [],
    acceptedRunFiles: [],
    rejectedRunFiles: [],
    attempts: [],
    maxAttempts: 5,
    benchmarks,
    failures: [],
    ok: benchmarks.every((benchmark) => benchmark.usable),
  };
}

function benchmark(id: string, medianHz: number, runCvPercent = 1, usable = true) {
  return {
    id,
    tier: "tier1",
    filepath: "benches/tier1/hotpath.bench.ts",
    group: "group",
    name: id,
    validRuns: 5,
    expectedRuns: 5,
    medianHz,
    medianPeriod: 1_000 / medianHz,
    maxRme: 1,
    runCvPercent,
    sampleCounts: [100, 100, 100, 100, 100],
    rawRunFilePaths: [],
    usable,
    failures: usable ? [] : ["run CV 8.00% exceeds 5%"],
  };
}

describe("bench artifact utilities", () => {
  it("flattens Vitest benchmark JSON with a stable benchmark identity", () => {
    const flattened = flattenVitestBenchJson(
      {
        files: [
          {
            filepath: "/repo/benches/tier1/hotpath.bench.ts",
            groups: [
              {
                fullName: "benches/tier1/hotpath.bench.ts > fitz-ts hotpath benchmarks",
                benchmarks: [
                  {
                    name: "frame encode",
                    hz: 1_000,
                    period: 0.001,
                    rme: 1.2,
                    sampleCount: 500,
                  },
                ],
              },
            ],
          },
        ],
      },
      { repoRoot: "/repo", runFilePath: "run-01-tier1.json" },
    );

    expect(flattened).toHaveLength(1);
    expect(flattened[0]).toMatchObject({
      id: "tier1 / benches/tier1/hotpath.bench.ts / fitz-ts hotpath benchmarks / frame encode",
      tier: "tier1",
      filepath: "benches/tier1/hotpath.bench.ts",
      group: "fitz-ts hotpath benchmarks",
      name: "frame encode",
      hz: 1_000,
      period: 0.001,
      rme: 1.2,
      sampleCount: 500,
      runFilePath: "run-01-tier1.json",
      valid: true,
    });
  });

  it("aggregates median throughput, median period, max RME, CV, and sample counts", () => {
    const aggregated = aggregateBenchmarks(
      [
        entry("bench", 1, 100),
        entry("bench", 2, 105, 2),
        entry("bench", 3, 95),
        entry("bench", 4, 100),
        entry("bench", 5, 100),
      ],
      { expectedRuns: 5 },
    );

    expect(aggregated.ok).toBe(true);
    expect(aggregated.benchmarks[0]).toMatchObject({
      id: "bench",
      medianHz: 100,
      medianPeriod: 10,
      maxRme: 2,
      sampleCounts: [100, 100, 100, 100, 100],
      validRuns: 5,
      usable: true,
    });
    expect(aggregated.benchmarks[0].runCvPercent).toBeCloseTo(3.16, 2);
  });

  it("fails benchmarks that do not satisfy the stable-run noise gates", () => {
    const aggregated = aggregateBenchmarks(
      [
        entry("too few", 1, 100),
        entry("too few", 2, 100),
        entry("too few", 3, 100),
        entry("high rme", 1, 100, 6),
        entry("high rme", 2, 100),
        entry("high rme", 3, 100),
        entry("high rme", 4, 100),
        entry("high rme", 5, 100),
        entry("high cv", 1, 100),
        entry("high cv", 2, 120),
        entry("high cv", 3, 80),
        entry("high cv", 4, 100),
        entry("high cv", 5, 100),
      ],
      { expectedRuns: 5 },
    );

    expect(aggregated.ok).toBe(false);
    expect(aggregated.failures.map((failure) => failure.id)).toEqual([
      "high cv",
      "high rme",
      "too few",
    ]);
    expect(aggregated.failures.map((failure) => failure.reason).join("\n")).toContain(
      "below required 4",
    );
    expect(aggregated.failures.map((failure) => failure.reason).join("\n")).toContain("exceeds 5%");
  });

  it("rejects signal attempts with any benchmark RME above the gate", () => {
    const selection = selectBenchmarkAttempts(
      [
        attempt(1, [entry("bench", 1, 100, 6)]),
        attempt(2, [entry("bench", 2, 100)]),
        attempt(3, [entry("bench", 3, 100)]),
        attempt(4, [entry("bench", 4, 100)]),
        attempt(5, [entry("bench", 5, 100)]),
      ],
      { runs: 5, maxAttempts: 5 },
    );

    expect(selection.attempts[0]).toMatchObject({
      attempt: 1,
      status: "rejected",
      reason: "attempt RME exceeds 5%",
    });
    expect(selection.acceptedRunFiles).toEqual([
      "run-2.json",
      "run-3.json",
      "run-4.json",
      "run-5.json",
    ]);
    expect(selection.failures[0].reason).toContain("accepted attempts 4/5");
  });

  it("replaces the highest-deviation accepted attempt when final CV fails", () => {
    const selection = selectBenchmarkAttempts(
      [
        attempt(1, [entry("bench", 1, 100)]),
        attempt(2, [entry("bench", 2, 100)]),
        attempt(3, [entry("bench", 3, 100)]),
        attempt(4, [entry("bench", 4, 100)]),
        attempt(5, [entry("bench", 5, 130)]),
        attempt(6, [entry("bench", 6, 100)]),
      ],
      { runs: 5, maxAttempts: 6 },
    );

    expect(selection.done).toBe(true);
    expect(selection.attempts.find((item) => item.attempt === 5)).toMatchObject({
      status: "rejected",
      reason: "largest aggregate deviation across CV-failing rows",
    });
    expect(selection.acceptedRunFiles).toEqual([
      "run-1.json",
      "run-2.json",
      "run-3.json",
      "run-4.json",
      "run-6.json",
    ]);
  });

  it("fails signal mode when max attempts are exhausted before CV passes", () => {
    const selection = selectBenchmarkAttempts(
      [
        attempt(1, [entry("bench", 1, 100)]),
        attempt(2, [entry("bench", 2, 100)]),
        attempt(3, [entry("bench", 3, 100)]),
        attempt(4, [entry("bench", 4, 100)]),
        attempt(5, [entry("bench", 5, 130)]),
      ],
      { runs: 5, maxAttempts: 5 },
    );

    const generatedSummary = buildSummary({
      entries: selection.entries,
      runs: selection.targetRuns,
      tiers: ["tier1"],
      runFiles: selection.acceptedRunFiles,
      acceptedRunFiles: selection.acceptedRunFiles,
      rejectedRunFiles: selection.rejectedRunFiles,
      attempts: selection.attempts,
      maxAttempts: selection.maxAttempts,
      runnerFailures: selection.failures,
      repoRoot: "/repo",
      artifactDir: "artifacts/bench/test",
      createdAt: "2026-07-03T00:00:00.000Z",
      signalMode: true,
    });

    expect(selection.done).toBe(false);
    expect(generatedSummary.failures.map((failure) => failure.reason).join("\n")).toContain(
      "max attempts 5 exhausted",
    );
    expect(shouldFailStableRun(generatedSummary)).toBe(true);
  });

  it("keeps smoke mode non-failing even when noise is reported", () => {
    const selection = selectBenchmarkAttempts(
      [attempt(1, [entry("bench", 1, 100, 7)]), attempt(2, [entry("bench", 2, 140)])],
      { runs: 2 },
    );
    const generatedSummary = buildSummary({
      entries: selection.entries,
      runs: selection.targetRuns,
      tiers: ["tier1"],
      runFiles: selection.acceptedRunFiles,
      acceptedRunFiles: selection.acceptedRunFiles,
      rejectedRunFiles: selection.rejectedRunFiles,
      attempts: selection.attempts,
      maxAttempts: selection.maxAttempts,
      repoRoot: "/repo",
      artifactDir: "artifacts/bench/test",
      createdAt: "2026-07-03T00:00:00.000Z",
      signalMode: false,
    });

    expect(generatedSummary.ok).toBe(false);
    expect(shouldFailStableRun(generatedSummary)).toBe(false);
  });

  it("classifies improvements, regressions, unchanged, and unusable comparisons", () => {
    const baseline = summary([
      benchmark("improved", 100),
      benchmark("regressed", 100),
      benchmark("unchanged", 100),
      benchmark("unusable", 100, 6, false),
    ]);
    const current = summary([
      benchmark("improved", 110),
      benchmark("regressed", 90),
      benchmark("unchanged", 102),
      benchmark("unusable", 120),
    ]);

    const report = compareSummaries(baseline, current);

    expect(report.ok).toBe(false);
    expect(report.improvements.map((row) => row.id)).toEqual(["improved"]);
    expect(report.regressions.map((row) => row.id)).toEqual(["regressed"]);
    expect(report.unchanged.map((row) => row.id)).toEqual(["unchanged"]);
    expect(report.unusable.map((row) => row.id)).toEqual(["unusable"]);
  });
});
