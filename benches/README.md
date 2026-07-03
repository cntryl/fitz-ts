# Benchmark Suite

This repository uses a tiered benchmark suite to separate microbenchmarks from broader subsystem and integration-style workloads.

## Tier definitions

- `tier1`: hot-path microbenchmarks for core frame and domain codec operations, request correlation, multiplexer performance, and parser behavior.
- `tier2`: subsystem benchmarks for domain-level workload patterns across `Kv`, `Notice`, `Lease`, `Queue`, `Schedule`, `Stream`, and `Rpc`.
- `tier3`: system benchmarks that compose multiple protocol and client-side payload flows into broader message batches.
- `tier4`: integration-style benchmarks that reflect realistic multi-message encode and frame assembly flows.

## One-pass Commands

Single tier commands are smoke/canary runs. They are useful for checking that a benchmark still executes, but they are not stable enough to use as before/after optimization evidence.

Run a specific tier:

```bash
npm run bench:tier1
npm run bench:tier2
npm run bench:tier3
npm run bench:tier4
```

Run every tier once:

```bash
npm run bench
```

All one-pass tier commands run serialized with `--no-file-parallelism --maxWorkers=1`.

## Stable Optimization Signal

Use `bench:stable` for optimization decisions. It runs the selected tier set five times, writes Vitest JSON output under `artifacts/bench/<timestamp>/`, and generates:

- `summary.json`: machine-readable aggregate data.
- `summary.md`: human-readable benchmark table and noise-gate failures.
- `run-XX-tierY.json`: raw Vitest output for each tier/run pair.

Run all tiers:

```bash
npm run bench:stable
```

Run one tier:

```bash
npm run bench:stable:tier1
npm run bench:stable:tier2
npm run bench:stable:tier3
npm run bench:stable:tier4
```

For runner smoke only, reduce the run count:

```bash
npm run bench:stable -- --runs 2
```

Runs below five are smoke mode: they write the same artifacts and report noisy rows, but do not fail the command on noise gates. Stable signal runs fail when a benchmark has too few valid runs, any required run exceeds `5%` RME, or run-level coefficient of variation exceeds `5%`. With the default five-run workflow, at least four valid runs are required.

## Compare Workflow

Collect an explicit baseline and current run, then compare their generated summaries:

```bash
npm run bench:stable -- --outputDir artifacts/bench/baseline
# make the candidate change
npm run bench:stable -- --outputDir artifacts/bench/current
npm run bench:compare -- --baseline artifacts/bench/baseline --current artifacts/bench/current
```

Compare uses generated `summary.json` artifacts only. A change is meaningful when the absolute throughput delta is at least `max(3%, currentCv + baselineCv)`. Significant regressions fail the command; significant improvements, unchanged rows, and noisy/unusable rows are reported separately.

## Benchmark Guidance

- Treat one-pass `npm run bench` output as smoke evidence only.
- Use `bench:stable` and `bench:compare` for before/after optimization claims.
- `tier4` is intentionally broader and more integration-style than `tier1`/`tier2`/`tier3`, so expect greater variance.
- Keep tier4 composition evidence separate from the tighter microbenchmark thresholds used for tier1.

## Notes

Benchmarks are primarily intended as performance evidence and should be kept stable. Prefer synthetic, deterministic workloads that exercise the same code paths as real client flows. Do not commit machine-specific baseline JSON; generated benchmark artifacts belong under ignored `artifacts/`.
