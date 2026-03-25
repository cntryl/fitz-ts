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

const client = new Client({
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
await tx.put(
  new TextEncoder().encode("user-1"),
  new TextEncoder().encode('{"name":"Alice"}'),
);
await tx.commit();

await client.close();
```

## Observability

`fitz-ts` now supports additive observability hooks through `ClientConfig.observability`.

```typescript
import { Client } from "@cntryl/fitz";

const client = new Client({
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
npm run typecheck
npm run lint
npm run fmt:check
npm run build
npm run test:unit
npm run verify
```

Broker-backed verification:

```bash
docker compose -f ../fitz-go/compose.yml up -d
npm run test:integration
CONFORMANCE_TRANSPORT=ws CONFORMANCE_AUTH_MODE=anonymous \
CONFORMANCE_OUTPUT=artifacts/conformance-results.json \
npm run test:spec
docker compose -f ../fitz-go/compose.yml down --volumes
```

Package smoke verification:

```bash
npm run pack:smoke
```

Suggested release checklist:

```bash
npm ci
npm run typecheck
npm run lint
npm run fmt:check
npm run build
npm run test:unit
docker compose -f ../fitz-go/compose.yml up -d
npm run test:integration
npm run test:conformance
npm run bench -- --run tests/bench/hotpath.bench.ts
docker compose -f ../fitz-go/compose.yml down --volumes
npm run pack:smoke
```

The conformance harness writes JSON results to `artifacts/conformance-results.json` by default.

## Repository Layout

- `src/client`: public client facade and connection management
- `src/transport`: WebSocket and TCP transports
- `src/domains`: domain clients and codecs
- `tests/unit`: fast unit coverage
- `tests/integration`: broker-backed integration coverage
- `tests/conformance`: release-gate conformance suite

Broker-backed connection hardening coverage now includes automatic reconnect subscription replay and token-provider replay checks in `tests/integration/connection.test.ts`.

## Canonical Spec

`fitz-ts` follows the canonical Fitz client docs in the server repo:

- [`../fitz/docs/clients/CLIENT_SPEC.md`](../fitz/docs/clients/CLIENT_SPEC.md)
- [`../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md`](../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md)
- [`../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md`](../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md)

## Additional Documentation

- [`docs/OPERATIONS.md`](docs/OPERATIONS.md)
- [`CHANGELOG.md`](CHANGELOG.md)
