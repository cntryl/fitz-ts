# fitz-ts

TypeScript client SDK for [Fitz](https://github.com/cntryl/fitz).

## Install

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

## Transport Support

- WebSocket: browser and Node.js
- TCP: Node.js only
- Auto transport detection: defaults to WebSocket when the URL omits a scheme

## Verification

Fast local checks:

```bash
npm ci
npm run lint
npm run build
npm run test:unit
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

The conformance harness writes JSON results to `artifacts/conformance-results.json` by default.

## Repository Layout

- `src/client`: public client facade and connection management
- `src/transport`: WebSocket and TCP transports
- `src/domains`: domain clients and codecs
- `tests/unit`: fast unit coverage
- `tests/integration`: broker-backed integration coverage
- `tests/conformance`: release-gate conformance suite

## Canonical Spec

`fitz-ts` follows the canonical Fitz client docs in the server repo:

- [`../fitz/docs/clients/CLIENT_SPEC.md`](../fitz/docs/clients/CLIENT_SPEC.md)
- [`../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md`](../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md)
- [`../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md`](../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md)
