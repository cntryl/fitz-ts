# fitz-ts Performance Results

This document tracks the benchmark evidence used to grade fitz-ts performance-related requirements.

## Scope

Current benchmark coverage is implemented in `tests/bench/hotpath.bench.ts` and focuses on hot-path client-side costs that can be measured without broker/network noise:

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
npm run bench -- --run tests/bench/hotpath.bench.ts
```

## Evidence Policy

- Record benchmark output from a clean local run before changing thresholds or making optimization claims.
- Use the same Node major version as CI when comparing runs.
- Treat benchmark regressions as evidence to investigate, not as proof of production impact without context.
- Keep broker-backed latency measurements separate from these microbenchmarks; network variance belongs in a different report.

## Status

- Benchmark suite exists and covers the primary hot paths needed for initial evidence.
- Fresh benchmark run captured on 2026-03-25 with `npm run bench -- --run tests/bench/hotpath.bench.ts`.
- Numeric targets are still not formalized, so the evidence supports `PARTIAL` grading rather than `PASS` for the stronger performance requirements.

## Latest Captured Results

Environment:

- Command: `npm run bench -- --run tests/bench/hotpath.bench.ts`
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
- These results are strong enough to demonstrate benchmark coverage and an initial baseline, but not yet enough to claim fixed release thresholds across environments.
