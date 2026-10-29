/**
 * Fitz cross-language conformance harness — TypeScript / fitz-ts
 *
 * Runs the implemented shared scenarios and reports the remaining cases as missing:
 *   fitz/docs/clients/cross-language-conformance-suite.yaml
 *
 * Configuration via environment variables:
 *   CONFORMANCE_TRANSPORT   "ws" (default) | "tcp"
 *   CONFORMANCE_AUTH_MODE   "anonymous" (default) | "valid_jwt"
 *   CONFORMANCE_OUTPUT      path to write JSON results (default: ./artifacts/conformance-results.json)
 *
 * Broker address resolved via the same env vars as integration tests
 * (FITZ_BROKER_WS_ADDR / FITZ_BROKER_TCP_ADDR / FITZ_BROKER_ANON_* / FITZ_BROKER_AUTH_*).
 *
 * Run:
 *   npm run test:conformance
 *   CONFORMANCE_TRANSPORT=tcp CONFORMANCE_AUTH_MODE=valid_jwt npm run test:conformance
 */
import { afterAll, describe, expect, it } from "vite-plus/test";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

import { createClient, type Client } from "../../src/client/client.js";
import type { ClientConfig } from "../../src/core/types.js";
import {
  AuthenticationError,
  ConnectionError,
  ErrCodeRpcRouteNotRegistered,
  ErrCodeRpcTimeout,
  KvError,
  RpcError,
  ScheduleError,
  isRetryable,
} from "../../src/core/errors.js";
import type { InboundRequest, ResponseWriter } from "../../src/domains/rpc/types.js";
import type { StreamReadBatch, StreamRecord } from "../../src/domains/stream/types.js";

import {
  REQUIRED_SCENARIO_IDS,
  ResultCollector,
  type ScenarioResult,
  type Verdict,
} from "./result.js";
import { exerciseGlobalFilteredReplay } from "./global-replay.js";
import type { TransportType } from "../integration/fixture/transport.js";
import { brokerAddrFor, type AuthMode } from "../integration/fixture/fixture.js";
import { createTransportProxy } from "../integration/fixture/transport-proxy.js";
import { exerciseStreamBoundary } from "../integration/fixture/stream-boundary.js";
import { waitFor } from "../integration/helpers.js";
import {
  generateValidTestJwt,
  generateInvalidSignatureTestJwt,
} from "../integration/fixture/jwt.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const TRANSPORT = (process.env["CONFORMANCE_TRANSPORT"] ?? "ws") as TransportType;
const AUTH_MODE = (process.env["CONFORMANCE_AUTH_MODE"] ?? "anonymous") as AuthMode;
const OUTPUT_PATH = resolve(
  process.env["CONFORMANCE_OUTPUT"] ?? "./artifacts/conformance-results.json",
);
const CLIENT_NAME = "fitz-ts";
const SECRET = process.env["FITZ_BROKER_JWT_HMAC_SECRET"] ?? "dev-test-secret";
const AUDIENCE = process.env["FITZ_BROKER_JWT_AUDIENCE"] ?? "fitz";
const TENANT = process.env["FITZ_BROKER_JWT_TENANT"] ?? "dev";
const SHARED_SUITE_PATH = resolve(
  process.cwd(),
  "docs/clients/cross-language-conformance-suite.yaml",
);

const BROKER_ADDR = brokerAddrFor(TRANSPORT, AUTH_MODE);
// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const b = (value: string) => Buffer.from(value);

let _counter = 0;
function uniqueRoute(scheme: string): string {
  _counter += 1;
  const id = `${Date.now()}-${_counter}-${Math.floor(Math.random() * 1_000_000)}`;
  if (scheme === "schedule") {
    return `${scheme}://conformance-realm/${id}/res/run`;
  }
  return `${scheme}://conformance-realm/${id}/res`;
}

function tokenProvider(): () => string | Promise<string> {
  switch (AUTH_MODE) {
    case "anonymous":
      return () => "";
    case "valid_jwt":
      return () => generateValidTestJwt(SECRET, AUDIENCE, TENANT);
    default:
      return () => "";
  }
}

async function withClient<T>(
  overrides: Partial<ClientConfig>,
  fn: (client: Client) => Promise<T>,
): Promise<T> {
  const client = createClient({
    url: BROKER_ADDR,
    transport: TRANSPORT,
    tokenProvider: tokenProvider(),
    timeout: 10000,
    ...overrides,
  });
  try {
    await client.connect();
    return await fn(client);
  } finally {
    await client.close().catch(() => undefined);
  }
}

async function expectRpcIteratorFailure(
  iterator: AsyncIterableIterator<unknown>,
): Promise<unknown> {
  let caught: unknown;
  try {
    await iterator.next();
  } catch (err) {
    caught = err;
  }
  expect(caught).toBeTruthy();
  return caught;
}

async function collectStreamRecords(
  iterator: AsyncIterableIterator<StreamReadBatch>,
): Promise<readonly StreamRecord[]> {
  const records: StreamRecord[] = [];
  for await (const batch of iterator) records.push(...batch.records);
  return records;
}

async function runScenario(
  id: string,
  title: string,
  priority: "P0" | "P1",
  fn: () => Promise<{ verdict: Verdict; evidence: string[] }>,
): Promise<ScenarioResult> {
  const start = Date.now();
  try {
    const { verdict, evidence } = await fn();
    return {
      scenario_id: id,
      title,
      priority,
      client: CLIENT_NAME,
      transport: TRANSPORT,
      auth_mode: AUTH_MODE,
      verdict,
      evidence,
      latency_ms: Date.now() - start,
    };
  } catch (err) {
    return {
      scenario_id: id,
      title,
      priority,
      client: CLIENT_NAME,
      transport: TRANSPORT,
      auth_mode: AUTH_MODE,
      verdict: "fail",
      evidence: [],
      latency_ms: Date.now() - start,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

// ---------------------------------------------------------------------------
// Result collector (module-level so afterAll can read it)
// ---------------------------------------------------------------------------

const collector = new ResultCollector();
const IMPLEMENTED_SCENARIO_IDS = [
  "CS-001",
  "CS-002",
  "CS-003",
  "CS-004",
  "CS-005",
  "CS-006",
  "CS-007",
  "CS-008",
  "CS-009",
  "CS-010",
  "CS-011",
  "CS-012",
  "CS-013",
  "CS-014",
  "CS-015",
  "CS-016",
  "CS-017",
  "CS-019",
  "CS-024",
];

function loadSharedSuiteScenarioIds(): string[] {
  const source = readFileSync(SHARED_SUITE_PATH, "utf-8");
  return source
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter((line) => line.startsWith("- id:"))
    .map((line) => line.slice("- id:".length).trim().replace(/^"|"$/gu, ""));
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

describe(`Fitz conformance — fitz-ts [transport=${TRANSPORT}, auth=${AUTH_MODE}]`, () => {
  it("accounts for every shared conformance scenario id", () => {
    expect(loadSharedSuiteScenarioIds()).toEqual(REQUIRED_SCENARIO_IDS);
    expect(new Set(IMPLEMENTED_SCENARIO_IDS).size).toBe(IMPLEMENTED_SCENARIO_IDS.length);
    expect(REQUIRED_SCENARIO_IDS).toEqual(expect.arrayContaining(IMPLEMENTED_SCENARIO_IDS));
  });

  // CS-001 ─ connect success
  it("CS-001 connect success", async () => {
    const result = await runScenario("CS-001", "connect success", "P0", async () => {
      const evidence: string[] = [];

      const client = createClient({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });
      try {
        await client.connect();
        evidence.push(`connect returned successfully`);

        const connected = client.isConnected();
        evidence.push(`isConnected() = ${connected}`);
        expect(connected).toBe(true);

        const route = uniqueRoute("kv");
        const tx = await client.kv.begin(route, {});
        await tx.put({ key: b("cs001-key"), value: b("cs001-value") });
        await tx.commit({ durability: "Sync" });
        evidence.push("first domain request (kv) succeeded");

        return { verdict: "pass", evidence };
      } finally {
        await client.close().catch(() => undefined);
      }
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-002 ─ auth failure
  it("CS-002 auth failure", async () => {
    const result = await runScenario("CS-002", "auth failure", "P0", async () => {
      const evidence: string[] = [];

      if (TRANSPORT === "tcp") {
        // TCP transport with silent CONNECT: auth failure manifests as connection
        // close rather than a typed error on connect(). We accept either a throw
        // or a close that leaves isConnected() == false.
        const client = createClient({
          url: brokerAddrFor("tcp", "invalid_signature"),
          transport: "tcp",
          tokenProvider: () => generateInvalidSignatureTestJwt(SECRET, AUDIENCE, TENANT),
          timeout: 3000,
        });
        try {
          await client.connect().catch(() => undefined);
          const connected = client.isConnected();
          evidence.push(`post-connect isConnected() = ${connected}`);
          // The client must NOT be connected after a bad token on TCP
          if (!connected) {
            evidence.push("auth failure correctly set connected=false (TCP silent close)");
            return { verdict: "pass", evidence };
          }
          // If still connected but domain ops fail, that's partial
          try {
            await client.kv.begin(uniqueRoute("kv"), {});
            evidence.push("domain request unexpectedly succeeded — verdict partial");
            return { verdict: "partial", evidence };
          } catch {
            evidence.push("domain request failed after silent auth close — partial pass");
            return { verdict: "partial", evidence };
          }
        } finally {
          await client.close().catch(() => undefined);
        }
      }

      // For WebSocket, auth failure should surface as AuthenticationError or
      // generic error on connect()
      const client = createClient({
        url: brokerAddrFor("ws", "invalid_signature"),
        transport: "ws",
        tokenProvider: () => generateInvalidSignatureTestJwt(SECRET, AUDIENCE, TENANT),
        timeout: 3000,
      });
      try {
        let caught: unknown;
        try {
          await client.connect();
        } catch (err) {
          caught = err;
        }
        if (caught) {
          evidence.push(`connect threw: ${(caught as Error).constructor.name}`);
          evidence.push("auth failure surfaced as error (correct)");
          if (caught instanceof AuthenticationError) {
            evidence.push("error is typed AuthenticationError (ideal)");
          }
          return { verdict: "pass", evidence };
        }
        // Didn't throw — check that client is not usable
        const afterConnected = client.isConnected();
        evidence.push(`isConnected() after invalid auth = ${afterConnected}`);
        evidence.push("connect did not throw for invalid JWT");
        return { verdict: "partial", evidence };
      } finally {
        await client.close().catch(() => undefined);
      }
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-003 ─ request success (kv read-after-write)
  it("CS-003 request success", async () => {
    const result = await runScenario("CS-003", "request success", "P0", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        const route = uniqueRoute("kv");

        const tx = await client.kv.begin(route, {});
        await tx.put({ key: b("user:1"), value: b("Alice") });
        await tx.commit({ durability: "Sync" });
        evidence.push("kv begin/put/commit succeeded");

        const rtx = await client.kv.begin(route, { mode: "ReadOnly" });
        const result = await rtx.get({ key: b("user:1") });
        expect(result.type).toBe("found");
        if (result.type === "found") {
          const value = Buffer.from(result.value).toString();
          expect(value).toBe("Alice");
          evidence.push(`read-after-commit returned "${value}" (correct)`);
        }
      });

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-004 ─ unknown route (rpc with no worker)
  it("CS-004 unknown route", async () => {
    const result = await runScenario("CS-004", "unknown route", "P0", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        const noWorkerRoute = uniqueRoute("rpc");
        const iterator = client.rpc.call(noWorkerRoute, { body: b("ping"), timeoutMs: 500 });
        const caught = await expectRpcIteratorFailure(iterator);
        expect(caught).toBeInstanceOf(RpcError);
        expect(caught).toMatchObject({ domainCode: ErrCodeRpcRouteNotRegistered });
        evidence.push(`rpc to unregistered route threw: ${(caught as Error).constructor.name}`);

        // Client must still be usable
        const route = uniqueRoute("kv");
        const tx = await client.kv.begin(route, {});
        await tx.put({ key: b("k"), value: b("v") });
        await tx.commit({ durability: "Sync" });
        evidence.push("client remains usable after unknown-route error");
      });

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-005 ─ invalid payload
  it("CS-005 invalid payload", async () => {
    const result = await runScenario("CS-005", "invalid payload", "P0", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        await expect(
          client.schedule.create(uniqueRoute("schedule"), {
            cron: "not a cron",
            deliveryMode: "Broadcast",
            payload: b("payload"),
          }),
        ).rejects.toBeInstanceOf(ScheduleError);
        evidence.push("invalid cron produced a Schedule domain error");
        const tx = await client.kv.begin(uniqueRoute("kv"), {});
        await tx.put({ key: b("after-invalid"), value: b("ok") });
        await tx.commit({ durability: "Sync" });
        evidence.push("client remains usable after the rejected operation");
      });

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-006 ─ server error mapping
  it("CS-006 server error mapping", async () => {
    const result = await runScenario("CS-006", "server error mapping", "P0", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        const route = uniqueRoute("rpc");

        // No worker registered — server returns RPC_ERR_NO_WORKER (retryable or
        // domain error, code should be accessible on the thrown error)
        const iterator = client.rpc.call(route, { body: b("ping"), timeoutMs: 500 });
        const caught = await expectRpcIteratorFailure(iterator);
        expect(caught).toBeInstanceOf(RpcError);
        expect(caught).toMatchObject({ domainCode: ErrCodeRpcRouteNotRegistered, effect: 0 });
        expect(isRetryable(caught)).toBe(true);
        const err = caught as Error & { code?: string; domainCode?: number };
        evidence.push(`error class: ${err.constructor.name}`);
        evidence.push(`error.code: ${err.code ?? "n/a"}`);
        evidence.push(`error.domainCode: ${err.domainCode ?? "n/a"}`);

        if (err.code) {
          evidence.push("error carries a typed code (correct)");
        }

        // Also verify that kv insert conflict carries a domainCode
        const kvRoute = uniqueRoute("kv");
        const tx = await client.kv.begin(kvRoute, {});
        await tx.insert({ key: b("x"), value: b("1") });
        await tx.commit({ durability: "Sync" });

        const tx2 = await client.kv.begin(kvRoute, {});
        let kvErr: unknown;
        try {
          await tx2.insert({ key: b("x"), value: b("2") });
        } catch (e) {
          kvErr = e;
        }
        await tx2.rollback().catch(() => undefined);

        expect(kvErr).toBeTruthy();
        expect(kvErr).toBeInstanceOf(KvError);
        expect(isRetryable(kvErr)).toBe(false);
        const ke = kvErr as Error & { code?: string; domainCode?: number };
        evidence.push(`kv conflict error class: ${ke.constructor.name}`);
        evidence.push(`kv conflict domainCode: ${ke.domainCode ?? "n/a"}`);
      });

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-007 ─ timeout handling
  it("CS-007 timeout handling", async () => {
    const result = await runScenario("CS-007", "timeout handling", "P0", async () => {
      const evidence: string[] = [];

      const workerClient = createClient({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });

      const callerClient = createClient({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });

      try {
        await workerClient.connect();
        await callerClient.connect();

        const route = uniqueRoute("rpc");
        const sub = await workerClient.rpc.registerWorker(route, async () => {
          await new Promise<void>((resolve) => setTimeout(resolve, 2000));
        });

        const start = Date.now();
        const iterator = callerClient.rpc.call(route, { body: b("nobody"), timeoutMs: 250 });
        const caught = await expectRpcIteratorFailure(iterator);
        const elapsed = Date.now() - start;
        evidence.push(`rpc threw after ~${elapsed}ms`);
        evidence.push(`error class: ${(caught as Error).constructor.name}`);

        expect(caught).toBeInstanceOf(RpcError);
        expect(caught).toMatchObject({ code: "RPC_TIMEOUT", domainCode: ErrCodeRpcTimeout });
        evidence.push("error is an RPC timeout, distinguishable from cancellation");

        // Connection still healthy
        const kvRoute = uniqueRoute("kv");
        const tx = await callerClient.kv.begin(kvRoute, {});
        await tx.put({ key: b("post-timeout"), value: b("ok") });
        await tx.commit({ durability: "Sync" });
        evidence.push("connection healthy after timeout");

        await sub.unsubscribe().catch(() => undefined);
      } finally {
        await workerClient.close().catch(() => undefined);
        await callerClient.close().catch(() => undefined);
      }

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-008 ─ caller cancellation
  it("CS-008 caller cancellation", async () => {
    const result = await runScenario("CS-008", "caller cancellation", "P0", async () => {
      const evidence: string[] = [];

      const workerClient = createClient({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });

      const callerClient = createClient({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });

      try {
        await workerClient.connect();
        await callerClient.connect();

        const route = uniqueRoute("rpc");
        const sub = await workerClient.rpc.registerWorker(
          route,
          async (_req: InboundRequest, writer: ResponseWriter) => {
            await new Promise<void>((resolve) => setTimeout(resolve, 2000));
            await writer.end({ body: b("late") });
          },
        );

        const controller = new AbortController();
        const iterator = callerClient.rpc.call(route, {
          body: b("block"),
          timeoutMs: 30000,
          signal: controller.signal,
        });

        const nextPromise = iterator.next();
        // Prevent vitest unhandled rejection warnings before explicit await below.
        nextPromise.catch(() => undefined);
        await new Promise<void>((resolve) => setTimeout(resolve, 100));
        controller.abort();

        let caught: unknown;
        try {
          await nextPromise;
        } catch (err) {
          caught = err;
        }

        expect(caught).toBeTruthy();
        evidence.push(`cancellation threw: ${(caught as Error).name}`);
        expect((caught as Error).name).toBe("AbortError");
        evidence.push("error is AbortError (correct — not timeout)");

        await sub.unsubscribe().catch(() => undefined);

        // Subsequent request should succeed
        const kvRoute = uniqueRoute("kv");
        const tx = await callerClient.kv.begin(kvRoute, {});
        await tx.put({ key: b("after-cancel"), value: b("ok") });
        await tx.commit({ durability: "Sync" });
        evidence.push("subsequent request succeeded after cancellation");
      } finally {
        await workerClient.close().catch(() => undefined);
        await callerClient.close().catch(() => undefined);
      }

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-009 ─ disconnect during request
  it("CS-009 disconnect during request", async () => {
    const result = await runScenario("CS-009", "disconnect during request", "P1", async () => {
      const evidence: string[] = [];

      const workerClient = createClient({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });

      const proxy = await createTransportProxy(TRANSPORT, BROKER_ADDR);
      const callerClient = createClient({
        url: proxy.url,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
        reconnect: { enabled: false },
      });

      try {
        await workerClient.connect();
        await callerClient.connect();

        const route = uniqueRoute("rpc");
        let workerStarted = false;
        await workerClient.rpc.registerWorker(
          route,
          async (_req: InboundRequest, writer: ResponseWriter) => {
            workerStarted = true;
            await new Promise<void>((resolve) => setTimeout(resolve, 3000));
            await writer.end({ body: b("late") });
          },
        );

        const iterator = callerClient.rpc.call(route, {
          body: b("block"),
          timeoutMs: 30000,
        });

        const nextPromise = iterator.next();
        // Prevent vitest unhandled rejection warnings before explicit await below.
        nextPromise.catch(() => undefined);

        await waitFor(() => workerStarted, { timeoutMs: 5000 });
        await proxy.goDown();

        let caught: unknown;
        try {
          await nextPromise;
        } catch (err) {
          caught = err;
        }

        expect(caught).toBeInstanceOf(ConnectionError);
        evidence.push(`in-flight request threw: ${(caught as Error).constructor.name}`);
        evidence.push("in-flight request failed after actual transport loss");
      } finally {
        await workerClient.close().catch(() => undefined);
        await callerClient.close().catch(() => undefined);
        await proxy.close();
      }

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-010 ─ reconnect and retry behavior
  it("CS-010 reconnect and retry behavior", async () => {
    const result = await runScenario("CS-010", "reconnect and retry behavior", "P1", async () => {
      const evidence: string[] = [];

      const proxy = await createTransportProxy(TRANSPORT, BROKER_ADDR);
      const events: string[] = [];
      const client = createClient({
        url: proxy.url,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
        reconnect: { enabled: true, maxAttempts: 20, backoffMs: 50, maxBackoffMs: 100 },
        observability: { onLifecycleEvent: (event) => events.push(event.event) },
      });
      const peer = createClient({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });

      try {
        await client.connect();
        await peer.connect();
        const noticeRoute = uniqueRoute("notice");
        const rpcRoute = uniqueRoute("rpc");
        const received: string[] = [];
        await client.notice.subscribe(noticeRoute, async (message) => {
          received.push(Buffer.from(message.body).toString());
        });
        await client.rpc.registerWorker(rpcRoute, async (_request, writer) => {
          await writer.end({ body: b("worker-restored") });
        });
        const staleTransaction = await client.kv.begin(uniqueRoute("kv"), {});

        await proxy.goDown();
        await waitFor(() => events.includes("connection_lost"), { timeoutMs: 5000 });
        await proxy.goUp();
        await waitFor(() => events.includes("reconnect_succeeded"), { timeoutMs: 10000 });
        expect(client.isConnected()).toBe(true);
        expect(staleTransaction.isOpen()).toBe(false);
        await expect(staleTransaction.get({ key: b("stale") })).rejects.toBeTruthy();
        evidence.push(
          "same client reconnected after transport loss and invalidated its transaction",
        );

        await peer.notice.publish(noticeRoute, { body: b("notice-restored") });
        await waitFor(() => received.includes("notice-restored"), { timeoutMs: 5000 });
        const response = peer.rpc.call(rpcRoute, { body: b("request"), timeoutMs: 5000 });
        const first = await response.next();
        expect(first.done).toBe(false);
        expect(Buffer.from(first.value?.body ?? new Uint8Array()).toString()).toBe(
          "worker-restored",
        );
        await expect(response.next()).resolves.toMatchObject({ done: true });
        evidence.push("Notice subscription and RPC worker resumed on the reconnected client");

        const route = uniqueRoute("kv");
        const tx = await client.kv.begin(route, {});
        await tx.put({ key: b("after-reconnect"), value: b("ok") });
        await tx.commit({ durability: "Sync" });
        evidence.push("new request succeeded on the same reconnected client");

        return { verdict: "pass", evidence };
      } finally {
        await client.close().catch(() => undefined);
        await peer.close().catch(() => undefined);
        await proxy.close();
      }
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-011 ─ stream receive sequence
  it("CS-011 stream receive sequence", async () => {
    const result = await runScenario("CS-011", "stream receive sequence", "P1", async () => {
      const evidence: string[] = [];
      let verdict: Verdict = "pass";

      try {
        await withClient({}, async (client) => {
          const route = uniqueRoute("stream");
          const session = await client.stream.begin(route);
          await session.append({ expectedOffset: 0n, body: Uint8Array.of(10) });
          await session.append({ expectedOffset: 1n, body: Uint8Array.of(20) });
          await session.append({ expectedOffset: 2n, body: Uint8Array.of(30) });
          await session.commit({ mode: "Sync" });
          evidence.push("stream session appended 3 records");

          const records = await collectStreamRecords(
            client.stream.read(route, { fromOffset: 0n, mode: "replay", batchSize: 10 }),
          );
          if (records.length !== 3) {
            verdict = "partial";
            evidence.push(`expected 3 stream records, got ${records.length}`);
          }
          for (let i = 0; i < records.length; i++) {
            if (records[i].offset !== BigInt(i)) {
              verdict = "partial";
              evidence.push(`expected offset ${i}, got ${records[i].offset}`);
              break;
            }
          }
          if (records.length > 0) {
            evidence.push(
              `first offset: ${records[0].offset}, last: ${records[records.length - 1].offset}`,
            );
          }
        });
      } catch (err) {
        verdict = "partial";
        evidence.push(
          `stream sequence scenario threw: ${err instanceof Error ? err.message : String(err)}`,
        );
      }

      return { verdict, evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-012 ─ stream completion
  it("CS-012 stream completion", async () => {
    const result = await runScenario("CS-012", "stream completion", "P1", async () => {
      const evidence: string[] = [];
      let verdict: Verdict = "pass";

      await withClient({}, async (client) => {
        const route = uniqueRoute("stream");
        const session = await client.stream.begin(route);
        await session.append({ expectedOffset: 0n, body: b("first") });
        await session.append({ expectedOffset: 1n, body: b("last") });
        await session.commit({ mode: "Sync" });
        evidence.push("stream session committed");

        // stream.read() should return and not block forever
        const records = await collectStreamRecords(
          client.stream.read(route, { fromOffset: 0n, mode: "replay", batchSize: 100 }),
        );
        if (records.length < 2) {
          verdict = "partial";
          evidence.push(`expected >=2 records after commit, got ${records.length}`);
        } else {
          evidence.push(`stream.read() completed cleanly with ${records.length} records`);
        }
        evidence.push("iterator/read closed cleanly (no resource leak)");
      });

      return { verdict, evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-013 ─ stream error mid-flight
  it("CS-013 stream error mid-flight", async () => {
    const result = await runScenario("CS-013", "stream error mid-flight", "P1", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        const route = uniqueRoute("stream");

        // append() with a wrong expected offset → server rejects it
        const session = await client.stream.begin(route);
        await session.append({ expectedOffset: 0n, body: b("record-1") });
        await session.commit({ mode: "Sync" });
        evidence.push("written first record at offset 0");

        let caught: unknown;
        try {
          // Expected offset 0 again, but stream is now at >0 — should fail
          const wrongSession = await client.stream.begin(route);
          await wrongSession.append({ expectedOffset: 0n, body: b("record-2") });
        } catch (err) {
          caught = err;
        }

        expect(caught).toBeTruthy();
        evidence.push(`append with wrong offset threw: ${(caught as Error).constructor.name}`);
        evidence.push("stream error surfaced correctly");

        // Client must remain usable (no resource leak)
        const kvRoute = uniqueRoute("kv");
        const tx = await client.kv.begin(kvRoute, {});
        await tx.put({ key: b("after-stream-error"), value: b("ok") });
        await tx.commit({ durability: "Sync" });
        evidence.push("client still usable after stream error");
      });

      evidence.push("an error during an active streaming read was not exercised");
      return { verdict: "partial", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("partial");
  });

  // CS-014 ─ concurrent in-flight requests
  it("CS-014 concurrent in-flight requests", async () => {
    const result = await runScenario("CS-014", "concurrent in-flight requests", "P1", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        // Issue 3 kv transactions concurrently on different routes
        const routes = [uniqueRoute("kv"), uniqueRoute("kv"), uniqueRoute("kv")];

        const tasks = routes.map(async (route, i) => {
          const tx = await client.kv.begin(route, {});
          await tx.put({ key: b(`key-${i}`), value: b(`value-${i}`) });
          await tx.commit({ durability: "Sync" });
          const rtx = await client.kv.begin(route, { mode: "ReadOnly" });
          return rtx.get({ key: b(`key-${i}`) });
        });

        const results = await Promise.all(tasks);
        expect(results).toHaveLength(3);
        for (let i = 0; i < results.length; i++) {
          expect(results[i].type).toBe("found");
          if (results[i].type === "found") {
            const value = Buffer.from(
              (results[i] as { type: "found"; value: Uint8Array }).value,
            ).toString();
            expect(value).toBe(`value-${i}`);
          }
        }

        evidence.push("3 concurrent kv transactions completed correctly");
        evidence.push("all responses correlated to correct request contexts");
      });

      evidence.push("cross-domain concurrency and within-domain ordering were not exercised");
      return { verdict: "partial", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("partial");
  });

  // CS-015 ─ shutdown during active work
  it("CS-015 shutdown during active work", async () => {
    const result = await runScenario("CS-015", "shutdown during active work", "P1", async () => {
      const evidence: string[] = [];

      const client = createClient({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });

      try {
        await client.connect();

        // Start an async KV operation, close the client mid-flight
        const route = uniqueRoute("kv");
        const kvBeginPromise = client.kv.begin(route, {});
        // Prevent vitest unhandled rejection warnings before explicit await below.
        kvBeginPromise.catch(() => undefined);

        // Close the client before the operation may complete
        await client.close();

        let caught: unknown;
        try {
          const tx = await kvBeginPromise;
          await tx.put({ key: b("key"), value: b("value") });
          await tx.commit({ durability: "Sync" });
        } catch (err) {
          caught = err;
        }

        if (caught) {
          evidence.push(`in-flight begin threw: ${(caught as Error).constructor.name}`);
          expect(caught).toBeInstanceOf(Error);
        } else {
          // Operation may have completed before close — acceptable
          evidence.push("operation completed before close (race — acceptable)");
        }

        evidence.push("close during active work did not panic");

        // Double close must not throw
        await client.close();
        evidence.push("double close is safe");
      } finally {
        await client.close().catch(() => undefined);
      }

      evidence.push(
        "the KV request may have completed before close; active-work interruption was not established",
      );
      return { verdict: "partial", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("partial");
  });

  // CS-016 - filtered stream replay
  it("CS-016 filtered stream replay", async () => {
    const result = await runScenario("CS-016", "filtered stream replay", "P1", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        const route = uniqueRoute("stream");
        const session = await client.stream.begin(route);
        const firstOffset = await session.append({
          expectedOffset: 0n,
          body: b("alpha"),
          discriminator: "proj.alpha",
        });
        const secondOffset = await session.append({
          expectedOffset: 1n,
          body: b("beta"),
          discriminator: "audit.beta",
        });
        await session.commit({ mode: "Sync" });
        evidence.push(`appended records at offsets ${firstOffset} and ${secondOffset}`);

        const filter = {
          clauses: [{ kind: "Equals" as const, value: "proj.alpha" }],
        };

        const iterator = client.stream.read(route, {
          fromOffset: 0n,
          mode: "replay",
          batchSize: 10,
          filter,
        });
        const first = await iterator.next();
        expect(first.done).toBe(false);
        const page = first.value!;
        const records = page.records;
        expect(records).toHaveLength(1);
        expect(records[0].offset).toBe(firstOffset);
        expect(Buffer.from(records[0].body).toString()).toBe("alpha");
        evidence.push("event-only read returned only the matching discriminator record");

        expect(page.items).toHaveLength(2);
        expect(page.items[0].kind).toBe("event");
        if (page.items[0].kind !== "event") {
          throw new Error("expected first page item to be an event");
        }
        expect(page.items[0].record.offset).toBe(firstOffset);
        expect(Buffer.from(page.items[0].record.body).toString()).toBe("alpha");
        expect(page.items[1]).toEqual({
          kind: "filtered",
          route,
          offset: secondOffset,
          reason: "server_filter",
        });
        expect(page.nextOffset).toBe(secondOffset + 1n);
        expect(page.caughtUp).toBe(true);
        evidence.push(
          "filtered replay returned the matching record plus synthetic filtered metadata in order",
        );
      });

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-017 - global filtered multi-page replay
  it("CS-017 global filtered multi-page replay", async () => {
    const result = await runScenario(
      "CS-017",
      "global filtered multi-page replay",
      "P1",
      async () => {
        const evidence = await withClient({}, async (client) =>
          exerciseGlobalFilteredReplay(client, uniqueRoute("stream")),
        );
        return { verdict: "pass", evidence };
      },
    );

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  it("CS-019 concurrent same-message-type requests", async () => {
    const result = await runScenario(
      "CS-019",
      "concurrent same-message-type requests",
      "P0",
      async () => {
        await withClient({}, async (client) => {
          const parkedRoute = uniqueRoute("queue");
          const readyRoute = uniqueRoute("queue");
          await client.queue.enqueue(readyRoute, { body: b("second") });
          let parkedSettled = false;
          const parked = client.queue
            .reserve(parkedRoute, {
              leaseSeconds: 30,
              batchSize: 1,
              waitSeconds: 5,
            })
            .finally(() => {
              parkedSettled = true;
            });
          parked.catch(() => undefined);
          await new Promise<void>((resolve) => setTimeout(resolve, 100));
          expect(parkedSettled).toBe(false);
          const second = await Promise.race([
            client.queue.reserve(readyRoute, { leaseSeconds: 30, batchSize: 1, waitSeconds: 0 }),
            new Promise<never>((_, reject) =>
              setTimeout(() => reject(new Error("second reserve timed out")), 2000),
            ),
          ]);
          expect(Buffer.from(second[0].body).toString()).toBe("second");
          expect(parkedSettled).toBe(false);
          await client.queue.enqueue(parkedRoute, { body: b("first") });
          const first = await parked;
          expect(Buffer.from(first[0].body).toString()).toBe("first");
        });
        return {
          verdict: "pass",
          evidence: [
            "second reserve resolved first with its own payload",
            "parked first reserve resolved with its own payload",
          ],
        };
      },
    );
    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  it("CS-024 Stream event boundary and replay", async () => {
    const result = await runScenario(
      "CS-024",
      "400 KiB Stream event boundary and replay",
      "P0",
      async () => {
        await withClient({}, async (client) =>
          exerciseStreamBoundary(client, uniqueRoute("stream")),
        );
        return {
          verdict: "pass",
          evidence: [
            "409600 combined bytes accepted; 409601 rejected with BAD_REQUEST/NOT_APPLIED",
            "three complete events replayed on a 4096-byte route across multiple pages",
          ],
        };
      },
    );
    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  it("RPC burst smoke", async () => {
    const result = await runScenario("TS-RPC-BURST", "RPC burst smoke", "P1", async () => {
      const evidence: string[] = [];

      const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

      await withClient({ timeout: 750, maxInFlightRequests: 16 }, async (client) => {
        const route = uniqueRoute("rpc");

        const workerClient = createClient({
          url: BROKER_ADDR,
          transport: TRANSPORT,
          tokenProvider: tokenProvider(),
          timeout: 10000,
        });

        try {
          await workerClient.connect();
          const sub = await workerClient.rpc.registerWorker(route, async (req, writer) => {
            await pause(500);
            await writer.end({ body: req.body });
          });

          const responseTimeoutMs = 1500;
          const firstIterator = client.rpc.call(route, {
            body: b("first"),
            timeoutMs: responseTimeoutMs,
          });
          const secondIterator = client.rpc.call(route, {
            body: b("second"),
            timeoutMs: responseTimeoutMs,
          });
          const firstNext = firstIterator.next();
          firstNext.catch(() => undefined);
          const secondNext = secondIterator.next();
          secondNext.catch(() => undefined);

          const secondState = await Promise.race([
            secondNext.then(
              () => "settled",
              () => "settled",
            ),
            pause(100).then(() => "pending"),
          ]);

          expect(secondState).toBe("pending");
          evidence.push("second RPC call remained pending while first was in flight");
          evidence.push("configured maxInFlightRequests=16 and burst size=2");

          const [firstFrame, secondFrame] = await Promise.all([firstNext, secondNext]);
          expect(firstFrame.done).toBe(false);
          expect(secondFrame.done).toBe(false);
          expect(Buffer.from(firstFrame.value?.body ?? new Uint8Array()).toString()).toBe("first");
          expect(Buffer.from(secondFrame.value?.body ?? new Uint8Array()).toString()).toBe(
            "second",
          );

          await expect(firstIterator.next()).resolves.toMatchObject({ done: true });
          await expect(secondIterator.next()).resolves.toMatchObject({ done: true });
          evidence.push("both burst RPC calls completed with correlated responses");

          await sub.unsubscribe();
        } finally {
          await workerClient.close().catch(() => undefined);
        }
      });

      return { verdict: "pass", evidence };
    });

    expect(result.verdict).toBe("pass");
  });

  // ---------------------------------------------------------------------------
  // Write aggregate JSON result after all scenarios
  // ---------------------------------------------------------------------------

  afterAll(() => {
    const aggregate = collector.aggregate({
      client: CLIENT_NAME,
      transport: TRANSPORT,
      auth_mode: AUTH_MODE,
    });

    const json = JSON.stringify(aggregate, null, 2);
    try {
      mkdirSync(dirname(OUTPUT_PATH), { recursive: true });
      writeFileSync(OUTPUT_PATH, json, "utf-8");
      console.log(`\nConformance results written to: ${OUTPUT_PATH}`);
      console.log(
        `Status: ${aggregate.overall_status.toUpperCase()}  ` +
          `P0: ${Math.round(aggregate.p0_pass_rate * 100)}%  ` +
          `P1: ${Math.round(aggregate.p1_pass_rate * 100)}%`,
      );
    } catch (err) {
      throw new Error("Failed to write conformance results", { cause: err });
    }
    if (aggregate.scenarios.length > 0 && aggregate.overall_status === "fail") {
      throw new Error(
        `Conformance P0 gate failed; missing scenarios: ${aggregate.missing_scenarios.join(", ")}`,
      );
    }
  });
});
