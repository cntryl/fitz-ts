# fitz-ts Performance Results

This document tracks the benchmark evidence used to grade fitz-ts performance-related requirements.

## Scope

Current benchmark coverage is implemented in `benches/hotpath.bench.ts` and focuses on hot-path client-side costs that can be measured without broker/network noise:

- frame encode and decode
- notice publish encoding
- KV get encoding
- lease acquire encoding
- RPC request encoding
- RPC correlation ID generation
- multiplexer request/response round-trip overhead
- multiplexer FIFO drain at 1,000 in-flight requests
- notice publish frame encoding throughput

## Run

```bash
npm run bench -- --run benches/hotpath.bench.ts
```

## Evidence Policy

- Record benchmark output from a clean local run before changing thresholds or making optimization claims.
- Use the same Node major version as CI when comparing runs.
- Treat benchmark regressions as evidence to investigate, not as proof of production impact without context.
- Keep broker-backed latency measurements separate from these microbenchmarks; network variance belongs in a different report.

## Status

- Benchmark suite exists and covers the primary hot paths needed for initial evidence.
- Fresh benchmark run captured on 2026-03-25 with `npm run bench -- --run benches/hotpath.bench.ts`.
- Numeric targets are now formalized and enforced by `tests/unit/perf/hotpath-thresholds.test.ts`, so the evidence supports `PASS` grading for the stronger performance requirements.

## Release Thresholds

The following budgets are enforced by `tests/unit/perf/hotpath-thresholds.test.ts` and should only move after a deliberate performance change:

| Benchmark                               | Budget                               |
| --------------------------------------- | ------------------------------------ |
| frame encode (small payload)            | <= 50 ms over 100k iterations        |
| frame decode (small payload)            | <= 50 ms over 100k iterations        |
| notice publish encode                   | <= 150 ms over 100k iterations       |
| kv get encode                           | <= 150 ms over 100k iterations       |
| lease acquire encode                    | <= 150 ms over 100k iterations       |
| rpc call encode                         | <= 500 ms over 100k iterations       |
| rpc correlation id generation           | <= 300 ms over 100k iterations       |
| multiplexer request/response round-trip | <= 200 ms over 10k iterations        |
| multiplexer 1k in-flight FIFO drain     | <= 25 ms for one drain               |
| frame parser fragmented stream          | <= 150 ms over 10k fragmented parses |
| notice publish frame encode throughput  | <= 150 ms over 100k iterations       |

## Latest Captured Results

Environment:

- Command: `npm run bench -- --run benches/hotpath.bench.ts`
- Date: 2026-03-25
- Runner: Vitest bench

Selected results from the latest run:

| Benchmark                               |            hz |      mean |       p99 |
| --------------------------------------- | ------------: | --------: | --------: |
| frame encode (small payload)            |  6,972,384.00 | 0.0001 ms | 0.0004 ms |
| frame decode (small payload)            | 11,724,843.66 | 0.0001 ms | 0.0001 ms |
| notice publish encode                   |  1,203,101.04 | 0.0008 ms | 0.0016 ms |
| kv get encode                           |    952,364.67 | 0.0011 ms | 0.0018 ms |
| lease acquire encode                    |    777,327.38 | 0.0013 ms | 0.0032 ms |
| rpc call encode                         |    330,273.01 | 0.0030 ms | 0.0244 ms |
| rpc correlation id generation           |    695,949.96 | 0.0014 ms | 0.0020 ms |
| multiplexer request/response round-trip |    919,257.98 | 0.0011 ms | 0.0017 ms |
| multiplexer 1k in-flight FIFO drain     |        665.12 | 1.5035 ms | 2.6410 ms |
| notice publish frame encode throughput  |  1,053,938.31 | 0.0009 ms | 0.0018 ms |

Interpretation:

- Frame encode/decode cost is negligible relative to the other measured client-side paths.
- The multiplexer can drain a 1,000-request in-flight FIFO workload in low-single-digit milliseconds on this machine.
- These results are strong enough to demonstrate benchmark coverage and an initial baseline, and the named thresholds above now turn them into an explicit release gate.
