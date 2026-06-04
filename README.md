# fitz-ts

TypeScript client SDK for [Fitz](https://github.com/cntryl/fitz).

## Install

Requires Node.js 20.19 or later.

```bash
npm install @cntryl/fitz
```

## Quick Start

```typescript
import { Client } from "@cntryl/fitz";

const client = Client({
  url: "ws://localhost:4090/ws",
  tokenProvider: async () => "your-jwt-token",
  reconnect: { enabled: true },
  asyncHandlers: {
    maxConcurrency: 32,
    timeoutMs: 5000,
  },
});

await client.connect();

const tx = await client.kv().begin("kv://realm/area/users", "ReadWrite");
await tx.put(new TextEncoder().encode("user-1"), new TextEncoder().encode('{"name":"Alice"}'));
await tx.commit();

await client.close();
```

## Observability

`fitz-ts` now supports additive observability hooks through `ClientConfig.observability`.

```typescript
import { Client } from "@cntryl/fitz";

const client = Client({
  url: "ws://localhost:4090/ws",
  reconnect: { enabled: true },
  observability: {
    logger: {
      log(level, event, fields) {
        console.log(level, event, fields);
      },
    },
    onLifecycleEvent(event) {
      console.log(event.event, event.state);
    },
  },
});
```

See `docs/OPERATIONS.md` for lifecycle events, metric names, and production guidance.

## Stream Replay

```typescript
import type { StreamFilterSet } from "@cntryl/fitz";

const filter: StreamFilterSet = {
  clauses: [{ kind: "Equals", value: "proj.alpha" }],
};

const records = await client.stream().read("stream://realm/app/events", 0n, 100, {
  filter,
  maxBytes: 64_000n,
});

const page = await client.stream().readPage("stream://realm/app/events", 0n, 100, {
  filter,
  maxBytes: 64_000n,
});

// read() keeps the compatibility projection and returns event records only.
// readPage() exposes synthetic filtered markers and cursor metadata.
void records;
void page.cursor.lastResourceOffset;
```

## Concurrency Notes

- Different domains can operate concurrently on one client connection.
- Multiple independent KV transactions and stream sessions can also be active concurrently.
- Do not issue overlapping operations against the same KV transaction or the same stream session. Those stateful handles are intended to be used sequentially.
- Notification and RPC worker handlers run through a shared async dispatcher. Use `asyncHandlers.maxConcurrency` and `asyncHandlers.timeoutMs` to bound handler fan-out in production.

## Transport Support

- WebSocket: browser and Node.js
- TCP: Node.js only
- Auto transport detection: defaults to WebSocket when the URL omits a scheme

## Verification

Fast local checks:

```bash
npm ci
npm run verify:fast
```

Broker-backed verification:

```bash
docker compose -f ../fitz-go/compose.yml up -d
npm run verify
docker compose -f ../fitz-go/compose.yml down --volumes
```

Package smoke verification:

```bash
npm run pack:smoke
```

Suggested release checklist:

```bash
npm ci
npm run verify:fast
docker compose -f ../fitz-go/compose.yml up -d
npm run verify
npm run bench
docker compose -f ../fitz-go/compose.yml down --volumes
```

Tiered benchmark commands:

```bash
npm run bench:tier1
npm run bench:tier2
npm run bench:tier3
npm run bench:tier4
npm run bench
```

> Run the full suite repeatedly before relying on benchmark automation. `tier4` is broader and more integration-style, so it should be stabilized over multiple executions before adding CI thresholds.

Benchmark tiers:

- `tier1`: hot-path microbenchmarks for core frame, codec, and multiplexer/parse runtime costs.
- `tier2`: subsystem benchmarks for domain-level payload and request-encoding workloads.
- `tier3`: system benchmarks for combined protocol and client flow payloads.
- `tier4`: integration benchmarks for realistic multi-message encode and frame assembly scenarios.

The conformance harness writes JSON results to `artifacts/conformance-results.json` by default.

Tooling is direct:

- `vp check` for combined format, lint, and type checks
- `vp fmt` for formatting
- `vp lint` for linting
- `vp test` for unit, integration, and conformance tests
- `vp pack` for JS bundle output
- `tsc` for declaration emit
- `tsc` for typechecking and declaration emit

Published artifacts are smoke-tested from the packed tarball in both ESM and
CommonJS consumer fixtures before release.

## Repository Layout

- `src/client`: public client facade and connection management
- `src/transport`: WebSocket and TCP transports
- `src/domains`: domain clients and codecs
- `tests/unit`: fast unit coverage
- `tests/integration`: broker-backed integration coverage
- `tests/conformance`: release-gate conformance suite

Broker-backed connection hardening coverage now includes automatic reconnect subscription replay and token-provider replay checks in `tests/integration/connection.test.ts`.

## Canonical Docs

`fitz-ts` follows the canonical Fitz client docs in the server repo:

- [`../fitz/docs/clients/CLIENT_SPEC.md`](../fitz/docs/clients/CLIENT_SPEC.md)
- [`../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md`](../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md)
- [`../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md`](../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md)
- [`../fitz/docs/clients/CONNECTION_FLOW.md`](../fitz/docs/clients/CONNECTION_FLOW.md)

## Documentation

- [`docs/README.md`](docs/README.md)
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md)
- [`docs/PUBLIC_CONTRACT.md`](docs/PUBLIC_CONTRACT.md)
- [`CLIENT_SPEC.md`](CLIENT_SPEC.md)
- [`CLIENT_ACCEPTANCE_CRITERIA.md`](CLIENT_ACCEPTANCE_CRITERIA.md)
- [`CHANGELOG.md`](CHANGELOG.md)
