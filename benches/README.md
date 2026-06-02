# Benchmark Suite

This repository uses a tiered benchmark suite to separate microbenchmarks from broader subsystem and integration-style workloads.

## Tier definitions

- `tier1`: hot-path microbenchmarks for core frame and domain codec operations, request correlation, multiplexer performance, and parser behavior.
- `tier2`: subsystem benchmarks for domain-level workload patterns across `Kv`, `Notice`, `Lease`, `Queue`, `Schedule`, `Stream`, and `Rpc`.
- `tier3`: system benchmarks that compose multiple protocol and client-side payload flows into broader message batches.
- `tier4`: integration-style benchmarks that reflect realistic multi-message encode and frame assembly flows.

## Run commands

Run a specific tier:

```bash
npm run bench:tier1
npm run bench:tier2
npm run bench:tier3
npm run bench:tier4
```

Run the full suite:

```bash
npm run bench
```

## Execution guidance

- Run the full suite multiple times and compare results before treating any single run as the baseline.
- `tier4` is intentionally broader and more integration-style than `tier1`/`tier2`/`tier3`, so expect greater variance.
- Use the repeated `npm run bench` output to identify stable trends for tier4 workloads before promoting thresholds to CI.
- Keep tier4 composition evidence separate from the tighter microbenchmark thresholds used for tier1.

## Notes

Benchmarks are primarily intended as performance evidence and should be kept stable. Prefer synthetic, deterministic workloads that exercise the same code paths as real client flows.
