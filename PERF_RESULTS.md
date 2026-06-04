# fitz-ts Performance Results

This document tracks the benchmark evidence used to grade fitz-ts performance-related requirements.

## Scope

The benchmark suite is organized into four tiers:

- `tier1`: hot-path microbenchmarks for core frame encoding/decoding, domain codec payload encoding, request correlation, and multiplexer/parser runtime costs.
- `tier2`: subsystem benchmarks for domain-level payload and client primitive workloads across KV, Notice, Lease, Queue, Schedule, Stream, and RPC.
- `tier3`: system benchmarks for combined protocol and client flow payloads that represent multi-message or multi-domain internal paths.
- `tier4`: integration benchmarks for realistic multi-message encode and frame assembly scenarios intended to capture broader client payload composition.

Current coverage includes hot-path client-side costs that can be measured without broker/network noise:

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
npm run bench
```

## Evidence Policy

- Record benchmark output from a clean local run before changing thresholds or making optimization claims.
- Use the same Node major version as CI when comparing runs.
- Treat benchmark regressions as evidence to investigate, not as proof of production impact without context.
- Keep broker-backed latency measurements separate from these microbenchmarks; network variance belongs in a different report.
- Run the full suite multiple times, especially `tier4`, before promoting any bench results or threshold behavior into CI.

## Status

- Benchmark suite exists and covers the primary hot paths needed for initial evidence.
- Fresh benchmark run captured on 2026-06-04 with `npm run bench:tier1`,
  `npm run bench:tier2`, and `npm run bench:tier3`.
- Numeric targets are now formalized and enforced by `tests/unit/perf/hotpath-thresholds.test.ts`, `tests/unit/perf/subsystem-thresholds.test.ts`, and `tests/unit/perf/system-thresholds.test.ts`.
- The 2026-06-04 optimization pass added shared exact-buffer helpers for direct
  RPC encode/key paths and replaced worker subscribe/unsubscribe `BufferWriter`
  allocation with exact route payload copies.

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
| queue enqueue encode                    | <= 200 ms over 100k iterations       |
| queue reserve encode                    | <= 150 ms over 100k iterations       |
| stream append encode                    | <= 200 ms over 100k iterations       |
| schedule create encode                  | <= 200 ms over 100k iterations       |
| rpc request encode                      | <= 400 ms over 100k iterations       |
| rpc decode inbound request              | <= 250 ms over 100k iterations       |
| frame batch encode + parse              | <= 250 ms over 10k iterations        |
| kv begin + frame encode                 | <= 150 ms over 100k iterations       |
| mixed payload encode batch              | <= 200 ms over 50k iterations        |

## Latest Captured Results

Environment:

- Command: `npm run bench:tier1`, `npm run bench:tier2`, `npm run bench:tier3`
- Date: 2026-06-04
- Runner: Vitest bench

Selected results from the latest run:

| Benchmark                               |            hz |      mean |       p99 |
| --------------------------------------- | ------------: | --------: | --------: |
| frame encode (small payload)            | 23,618,309.20 | 0.0000 ms | 0.0001 ms |
| frame decode (small payload)            | 25,428,289.95 | 0.0000 ms | 0.0000 ms |
| notice publish encode                   |  4,236,625.20 | 0.0002 ms | 0.0006 ms |
| kv get encode                           |  2,912,292.62 | 0.0003 ms | 0.0006 ms |
| lease acquire encode                    |  2,730,458.97 | 0.0004 ms | 0.0008 ms |
| rpc call encode                         |  1,220,336.59 | 0.0008 ms | 0.0013 ms |
| rpc correlation id generation           |  1,657,048.68 | 0.0006 ms | 0.0008 ms |
| multiplexer request/response round-trip |    233,147.04 | 0.0043 ms | 0.0045 ms |
| multiplexer 1k in-flight FIFO drain     |      1,677.30 | 0.5962 ms | 1.2636 ms |
| notice publish frame encode throughput  |  3,671,701.37 | 0.0003 ms | 0.0005 ms |
| rpc subscribe worker encode             | 16,805,740.55 | 0.0001 ms | 0.0001 ms |
| frame batch encode + parse              |    759,413.45 | 0.0013 ms | 0.0028 ms |
| mixed payload encode batch              |  1,338,980.15 | 0.0007 ms | 0.0014 ms |

Interpretation:

- Frame encode/decode cost is negligible relative to the other measured client-side paths.
- The multiplexer can drain a 1,000-request in-flight FIFO workload in low-single-digit milliseconds on this machine.
- RPC worker subscribe/unsubscribe encode now returns exact route payload copies
  instead of allocating a `BufferWriter`; the tier2 unit benchmark measured
  `rpc subscribe worker encode` at 16.8M hz after the change versus roughly
  4.1M hz in the pre-optimization baseline.
- Direct numeric write/read helpers are used only by direct RPC buffer paths;
  `BufferWriter` kept inline writes after benchmarking showed helper indirection
  regressed broader composed paths.
- These results demonstrate current benchmark coverage and keep the named
  thresholds above as an explicit release gate.
