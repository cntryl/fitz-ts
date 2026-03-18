# fitz-ts

TypeScript client library for [Fitz](https://github.com/cntryl/fitz), a distributed system with domains for KV, Queue, RPC, Lease, Notice, Stream, and Schedule operations.

## Features

- WebSocket and TCP transport support
- Token-provider based authentication with anonymous mode support
- Reconnect-aware connection manager
- Domain clients for KV, Queue, RPC, Lease, Notice, Stream, and Schedule
- Promise-based TypeScript API with typed domain errors

## Installation

```bash
npm install @cntryl/fitz
```

## Quick Start

```typescript
import { Client } from "@cntryl/fitz";

// Create client
const client = new Client({
  url: "ws://localhost:4090/ws",
  tokenProvider: async () => "your-jwt-token",
  reconnect: { enabled: true },
});

// Connect
await client.connect();

// Use KV
const kv = client.kv();
const tx = await kv.begin("kv://realm/area/users", "ReadWrite");

await tx.put(
  new TextEncoder().encode("user-1"),
  new TextEncoder().encode('{"name": "Alice"}'),
);

const value = await tx.get(new TextEncoder().encode("user-1"));
if (value.type === "found") {
  console.log(value.value);
}
await tx.commit();

// Close
await client.close();
```

## Supported Transports

- **WebSocket**: `ws://` and `wss://` (browser and Node.js)
- **TCP**: `tcp://` (Node.js only, with length-prefixed framing)
- **Auto-detection**: URL automatically converted to appropriate transport

```typescript
// WebSocket
const client = new Client({
  url: "ws://localhost:4090/ws",
  tokenProvider: () => token,
});

// TCP
const client = new Client({
  url: "tcp://localhost:4090",
  tokenProvider: () => token,
});

// Auto-detect
const client = new Client({
  url: "localhost:4090", // Defaults to WebSocket
  tokenProvider: () => token,
});
```

## API

### Client Configuration

```typescript
interface ClientConfig {
  url: string;
  tokenProvider?: () => string | Promise<string>;
  timeout?: number; // Default: 30000ms
  transport?: "ws" | "tcp" | "auto"; // Default: 'auto'
  reconnect?: {
    enabled?: boolean;
    maxAttempts?: number;
    backoffMs?: number;
    maxBackoffMs?: number;
  };
  maxFrameSize?: number;
  authSettleDelayMs?: number;
}
```

### Connection Management

```typescript
const client = new Client(config);

await client.connect();
const state = client.getState();
await client.close();
```

### KV Domain

```typescript
const kv = client.kv();

// Begin transaction
const tx = await kv.begin("kv://realm/area/resources", "ReadWrite");

// Operations
await tx.put(key, value);
const value = await tx.get(key);
await tx.delete(key);
await tx.insert(key, valueBytes);
await tx.deleteRange(startKey, endKey);
const keys = await tx.scan({ startKey, endKey, limit: 100, reverse: false });

// Finalize
await tx.commit();
await tx.rollback();
```

### Other Domains

Queue, RPC, Lease, Notice, Stream, and Schedule clients are available:

```typescript
const queue = client.queue();
const rpc = client.rpc();
const lease = client.lease();
const notice = client.notice();
const stream = client.stream();
const schedule = client.schedule();

const messageId = await queue.enqueue("queue://realm/area/tasks", body);
const items = await queue.reserve("queue://realm/area/tasks", 30, 10);
const responses = await rpc.call("rpc://realm/area/worker", body);
const worker = await rpc.registerWorker(
  "rpc://realm/area/worker",
  async (request, writer) => {
    await writer.send(request.body, true);
  },
);
```

## Error Handling

All errors subclass `FitzError`:

```typescript
import {
  FitzError,
  TimeoutError,
  ConnectionError,
  KvError,
} from "@cntryl/fitz";

try {
  await client.connect();
} catch (err) {
  if (err instanceof TimeoutError) {
    console.log("Connection timeout");
  } else if (err instanceof ConnectionError) {
    console.log("Connection failed");
  } else if (err instanceof FitzError) {
    console.log("Fitz error:", err.code);
  }
}
```

## Building

```bash
npm run build
```

Outputs:

- `dist/index.js` (CommonJS)
- `dist/index.mjs` (ESM)
- `dist/index.d.ts` (TypeScript declarations)

## Testing

```bash
# Run unit + integration tests
npm test

# Run unit tests only
npm run test:unit

# Run broker-backed integration tests only
npm run test:integration

# Run the spec-compliance conformance suite
npm run test:spec

# Watch unit tests
npm run test:watch

# With coverage
npm run test:unit -- --coverage

# Run benchmarks
npm run bench
```

## Integration Testing

`fitz-ts` mirrors the real-broker integration coverage in `fitz-go/test/*.go`.
The integration suite does not start brokers for you; it expects the Fitz
brokers from `fitz-go/compose.yml` to already be running.

The required repo-local spec gate is the conformance suite:

```bash
npm run test:spec
```

Default broker addresses:

- Anonymous TCP: `localhost:4191`
- Anonymous WebSocket: `ws://localhost:4190/ws`
- Auth TCP: `localhost:4091`
- Auth WebSocket: `ws://localhost:4090/ws`

Supported environment variables:

- `FITZ_BROKER_TCP_ADDR`
- `FITZ_BROKER_WS_ADDR`
- `FITZ_BROKER_AUTH_TCP_ADDR`
- `FITZ_BROKER_AUTH_WS_ADDR`
- `FITZ_BROKER_ANON_TCP_ADDR`
- `FITZ_BROKER_ANON_WS_ADDR`
- `FITZ_BROKER_JWT_HMAC_SECRET`
- `FITZ_BROKER_JWT_AUDIENCE`

Use `npm run test:integration` to run only the broker-backed suite. `npm test`
now runs both the unit project and the full broker-backed integration project.

## Development

```bash
# Format code
npm run fmt

# Lint
npm run lint

# Type check
npx tsc --noEmit
```

## Canonical Spec

`fitz-ts` follows the canonical client docs in the server repo:

- [`../fitz/docs/clients/CLIENT_SPEC.md`](../fitz/docs/clients/CLIENT_SPEC.md)
- [`../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md`](../fitz/docs/clients/CLIENT_ACCEPTANCE_CRITERIA.md)
- [`../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md`](../fitz/docs/clients/CLIENT_IMPLEMENTATION_GUIDE.md)

## Architecture

### Layers

1. **API** (`src/client/`) - Main Client and Connection management
2. **Transport** (`src/transport/`) - WebSocket/TCP abstraction
3. **Frame** (`src/frame/`) - Wire protocol encoding/decoding
4. **Domains** (`src/domains/`) - Domain-specific clients and codecs
5. **Core** (`src/core/`) - Common utilities (buffer, types, errors)

### Frame Protocol

```
[MessageType (1-3 bytes)] [Length (u16 BE)] [Payload]
```

- MessageType 0-254: single byte
- MessageType 255+: escape byte `0xFF` + u16 BE
- Payload: domain-specific concatenated fields (no nesting)

## License

Apache-2.0
