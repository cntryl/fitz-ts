export interface FlattenedBenchmark {
  id: string;
  tier: string;
  filepath: string;
  group: string;
  name: string;
  hz: number | undefined;
  period: number | undefined;
  rme: number | undefined;
  sampleCount: number | undefined;
  runFilePath: string | undefined;
  valid: boolean;
}

export interface BenchmarkAttemptRecord {
  attempt: number;
  runFiles: string[];
  entries: FlattenedBenchmark[];
}

export interface BenchmarkAttemptSummary {
  attempt: number;
  status: "accepted" | "rejected";
  reason?: string;
  runFiles: string[];
  failures: Array<{ id: string; reason: string }>;
}

export interface AggregatedBenchmark {
  id: string;
  tier: string;
  filepath: string;
  group: string;
  name: string;
  validRuns: number;
  expectedRuns: number;
  medianHz: number | undefined;
  medianPeriod: number | undefined;
  maxRme: number | undefined;
  runCvPercent: number;
  sampleCounts: number[];
  rawRunFilePaths: string[];
  usable: boolean;
  failures: string[];
}

export interface BenchmarkSummary {
  schemaVersion: number;
  generatedBy: string;
  createdAt: string;
  artifactDir: string;
  repoRoot: string;
  tiers: string[];
  runs: number;
  signalMode: boolean;
  thresholds: {
    rmePercent: number;
    cvPercent: number;
    minChangePercent: number;
  };
  runFiles: string[];
  acceptedRunFiles: string[];
  rejectedRunFiles: string[];
  attempts: BenchmarkAttemptSummary[];
  maxAttempts: number;
  benchmarks: AggregatedBenchmark[];
  failures: Array<{ id: string; reason: string }>;
  ok: boolean;
}

export interface BenchmarkCompare {
  schemaVersion: number;
  generatedBy: string;
  baseline: string;
  current: string;
  regressions: BenchmarkComparison[];
  improvements: BenchmarkComparison[];
  unchanged: BenchmarkComparison[];
  unusable: Array<{ id: string; reason: string }>;
  ok: boolean;
}

export interface BenchmarkComparison {
  id: string;
  baselineHz: number;
  currentHz: number;
  deltaPercent: number;
  thresholdPercent: number;
  baselineCvPercent: number;
  currentCvPercent: number;
}

export function flattenVitestBenchJson(
  json: unknown,
  options?: {
    repoRoot?: string;
    tier?: string;
    runFilePath?: string;
  },
): FlattenedBenchmark[];

export function aggregateBenchmarks(
  entries: FlattenedBenchmark[],
  options?: {
    expectedRuns?: number;
    minValidRuns?: number;
    rmePercent?: number;
    cvPercent?: number;
  },
): {
  benchmarks: AggregatedBenchmark[];
  failures: Array<{ id: string; reason: string }>;
  ok: boolean;
};

export function selectBenchmarkAttempts(
  attemptRecords: BenchmarkAttemptRecord[],
  options?: {
    runs?: number;
    signalMode?: boolean;
    maxAttempts?: number;
    thresholds?: {
      rmePercent?: number;
      cvPercent?: number;
      minChangePercent?: number;
    };
  },
): {
  attempts: BenchmarkAttemptSummary[];
  entries: FlattenedBenchmark[];
  acceptedRunFiles: string[];
  rejectedRunFiles: string[];
  targetRuns: number;
  maxAttempts: number;
  done: boolean;
  failures: Array<{ id: string; reason: string }>;
};

export function shouldFailStableRun(summary: BenchmarkSummary): boolean;

export function buildSummary(options: {
  entries: FlattenedBenchmark[];
  runs: number;
  tiers: string[];
  runFiles: string[];
  acceptedRunFiles?: string[];
  rejectedRunFiles?: string[];
  attempts?: BenchmarkAttemptSummary[];
  maxAttempts?: number;
  runnerFailures?: Array<{ id: string; reason: string }>;
  repoRoot: string;
  artifactDir: string;
  createdAt: string;
  signalMode?: boolean;
  thresholds?: {
    rmePercent?: number;
    cvPercent?: number;
    minChangePercent?: number;
  };
}): BenchmarkSummary;

export function compareSummaries(
  baseline: BenchmarkSummary,
  current: BenchmarkSummary,
  options?: {
    minChangePercent?: number;
  },
): BenchmarkCompare;

export function formatSummaryMarkdown(summary: BenchmarkSummary): string;

export function formatCompareMarkdown(report: BenchmarkCompare): string;
