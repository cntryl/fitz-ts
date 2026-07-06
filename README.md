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

## Startup Orchestration

`connect()` is one-shot for the initial session. Services that start alongside
Fitz can opt into startup waiting with `connectWhenReady()`:

```typescript
const controller = new AbortController();

await client.connectWhenReady({
  signal: controller.signal,
  timeoutMs: 30_000,
  backoffMs: 250,
  maxBackoffMs: 2_000,
});
```

The helper retries initial transport and connection-readiness failures until the
first successful session, the timeout expires, or the signal aborts. Authentication
failures are not retried.

## Observability

`fitz-ts` now supports additive observability hooks through `ClientConfig.observability`.

```typescript
import { Client } from "@cntryl/fitz";

const client = Client({
  url: "ws://localhost:4090/ws",
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

## Resilience Defaults

- After a client has connected successfully once, transport loss automatically triggers reconnect with bounded backoff unless `reconnect.enabled` is set to `false`.
- The initial `connect()` call is one-shot by default. Use `connectWhenReady()` when service startup should wait out broker availability.
- Idle subscription clients stay connected by default: `heartbeat.enabled` defaults to `true`, `heartbeat.intervalMs` defaults to `10000`, and `heartbeat.timeoutMs` defaults to `30000`.
- Heartbeats noop when the client was active within the current interval. Node WebSocket uses native ping/pong when available, TCP enables socket keepalive, and browser WebSocket relies on close/error plus receive-timeout suppression.
- Safe automatic retries are enabled by default through `ClientConfig.retry`:
  - idempotent reads: KV `get` / `scan`, Stream `read` / `readPage` / `peek` / `metadata`, Lease `query`
  - queue `enqueue()` only after Fitz explicitly rejects the write with a known transient commit failure or queue backpressure response
- The client does not automatically replay KV mutations, stream writes, queue reservations or acknowledgements, lease ownership changes, RPC calls, or notice publishes after an ambiguous post-send failure.
- `QueueItem`, `Lease`, `KvTransaction`, and `StreamSession` handles from the pre-disconnect session are stale after reconnect and must be reacquired.

## Wake Gates

`createWakeGate()` exposes the low-level wake primitive used by the client helpers. It is useful whenever a notification wakes the loop and the loop then performs the authoritative read or claim.

Subscription-driven helpers built on the gate:

- `queue.reserveWhenAvailable(route, { leaseSeconds, batchSize = 1, signal })`
- `stream.readWhenCommitted(route, { offset, batchSize = 100, maxBytes, filter, signal })`
- `schedule.waitForNotifications(route, { signal })`

Queue and stream subscriptions are wake signals, not work handlers. The authoritative work step remains `reserve()` or `read()`.

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
- Do not issue overlapping operations against the same KV transaction, stream session, queue item, or lease. Those stateful handles are intended to be used sequentially.
- Notification and RPC worker handlers run through a shared async dispatcher. Use `asyncHandlers.maxConcurrency` and `asyncHandlers.timeoutMs` to bound handler fan-out in production.

## Transport Support

- WebSocket: browser and Node.js
- TCP: Node.js only
- Auto transport detection: defaults to WebSocket when the URL omits a scheme

Node.js WebSocket clients send `User-Agent: @cntryl/fitz` and `Accept: */*`
on upgrade requests by default so common HTTP front doors treat the client like
a normal HTTP client. Add or override Node-only upgrade headers with
`ClientConfig.webSocket.headers`:

```typescript
const client = Client({
  url: "wss://fitz.example.com/ws",
  webSocket: {
    headers: {
      "User-Agent": "my-service",
      "X-Environment": "dev",
    },
  },
});
```

Browser WebSocket APIs do not allow custom upgrade headers, so
`webSocket.headers` is ignored in browsers.

## Verification

Fast local checks:

```bash
npm ci
npm run verify:fast
```

Broker-backed verification:

```bash
docker compose up -d
npm run verify
docker compose down --volumes
```

Package smoke verification:

```bash
npm run pack:smoke
```

Suggested release checklist:

```bash
npm ci
npm run verify:fast
docker compose up -d
npm run verify
npm run bench
docker compose down --volumes
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
