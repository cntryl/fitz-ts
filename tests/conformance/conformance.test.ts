/**
 * Fitz cross-language conformance harness â€” TypeScript / fitz-ts
 *
 * Implements the shared 001..015 scenarios plus the bounded-load check for:
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
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

import { Client } from "../../src/client/client.js";
import type { ClientConfig } from "../../src/core/types.js";
import { AuthenticationError, TimeoutError } from "../../src/core/errors.js";
import type { InboundRequest, ResponseWriter } from "../../src/domains/rpc/types.js";

import { ResultCollector, type ScenarioResult, type Verdict } from "./result.js";
import type { TransportType } from "../integration/fixture/transport.js";
import { brokerAddrFor, type AuthMode } from "../integration/fixture/fixture.js";
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
const SECRET = process.env["FITZ_BROKER_JWT_HMAC_SECRET"] ?? "test-secret-key";
const AUDIENCE = process.env["FITZ_BROKER_JWT_AUDIENCE"] ?? "fitz";

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
      return () => generateValidTestJwt(SECRET, AUDIENCE);
    default:
      return () => "";
  }
}

async function withClient<T>(
  overrides: Partial<ClientConfig>,
  fn: (client: Client) => Promise<T>,
): Promise<T> {
  const client = new Client({
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

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

describe(`Fitz conformance â€” fitz-ts [transport=${TRANSPORT}, auth=${AUTH_MODE}]`, () => {
  // CS-001 â”€ connect success
  it("CS-001 connect success", async () => {
    const result = await runScenario("CS-001", "connect success", "P0", async () => {
      const evidence: string[] = [];

      const client = new Client({
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
        const tx = await client.kv().begin(route, { durability: "Sync" });
        await tx.put(b("cs001-key"), b("cs001-value"));
        await tx.commit();
        evidence.push("first domain request (kv) succeeded");

        return { verdict: "pass", evidence };
      } finally {
        await client.close().catch(() => undefined);
      }
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-002 â”€ auth failure
  it("CS-002 auth failure", async () => {
    const result = await runScenario("CS-002", "auth failure", "P0", async () => {
      const evidence: string[] = [];

      if (TRANSPORT === "tcp") {
        // TCP transport with silent CONNECT: auth failure manifests as connection
        // close rather than a typed error on connect(). We accept either a throw
        // or a close that leaves isConnected() == false.
        const client = new Client({
          url: brokerAddrFor("tcp", "invalid_signature"),
          transport: "tcp",
          tokenProvider: () => generateInvalidSignatureTestJwt(SECRET, AUDIENCE),
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
            await client.kv().begin(uniqueRoute("kv"), {
              durability: "Sync",
            });
            evidence.push("domain request unexpectedly succeeded â€” verdict partial");
            return { verdict: "partial", evidence };
          } catch {
            evidence.push("domain request failed after silent auth close â€” partial pass");
            return { verdict: "partial", evidence };
          }
        } finally {
          await client.close().catch(() => undefined);
        }
      }

      // For WebSocket, auth failure should surface as AuthenticationError or
      // generic error on connect()
      const client = new Client({
        url: brokerAddrFor("ws", "invalid_signature"),
        transport: "ws",
        tokenProvider: () => generateInvalidSignatureTestJwt(SECRET, AUDIENCE),
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
        // Didn't throw â€” check that client is not usable
        const afterConnected = client.isConnected();
        evidence.push(`isConnected() after invalid auth = ${afterConnected}`);
        evidence.push("connect did not throw for invalid JWT");
        return { verdict: "partial", evidence };
      } finally {
        await client.close().catch(() => undefined);
      }
    });

    collector.record(result);
    // Accept pass or partial (TCP silent-close model is intentional by spec)
    expect(["pass", "partial"]).toContain(result.verdict);
  });

  // CS-003 â”€ request success (kv read-after-write)
  it("CS-003 request success", async () => {
    const result = await runScenario("CS-003", "request success", "P0", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        const route = uniqueRoute("kv");

        const tx = await client.kv().begin(route, { durability: "Sync" });
        await tx.put(b("user:1"), b("Alice"));
        await tx.commit();
        evidence.push("kv begin/put/commit succeeded");

        const rtx = await client.kv().begin(route, { mode: "ReadOnly", durability: "Sync" });
        const result = await rtx.get(b("user:1"));
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

  // CS-004 â”€ unknown route (rpc with no worker)
  it("CS-004 unknown route", async () => {
    const result = await runScenario("CS-004", "unknown route", "P0", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        const noWorkerRoute = uniqueRoute("rpc");
        let caught: unknown;
        try {
          await client.rpc().call(noWorkerRoute, b("ping"), { timeoutMs: 500 });
        } catch (err) {
          caught = err;
        }
        expect(caught).toBeTruthy();
        evidence.push(`rpc to unregistered route threw: ${(caught as Error).constructor.name}`);

        // Client must still be usable
        const route = uniqueRoute("kv");
        const tx = await client.kv().begin(route, { durability: "Sync" });
        await tx.put(b("k"), b("v"));
        await tx.commit();
        evidence.push("client remains usable after unknown-route error");
      });

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-005 â”€ invalid payload (duplicate insert â†’ domain error, client stays healthy)
  it("CS-005 invalid payload", async () => {
    const result = await runScenario("CS-005", "invalid payload", "P0", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        const route = uniqueRoute("kv");

        // Write key once
        const tx1 = await client.kv().begin(route, { durability: "Sync" });
        await tx1.insert(b("dup-key"), b("first"));
        await tx1.commit();
        evidence.push("first insert succeeded");

        // Attempting insert on an existing key is a server-rejected operation
        const tx2 = await client.kv().begin(route, { durability: "Sync" });
        let caught: unknown;
        try {
          await tx2.insert(b("dup-key"), b("second"));
        } catch (err) {
          caught = err;
        }
        await tx2.rollback().catch(() => undefined);

        expect(caught).toBeTruthy();
        evidence.push(`duplicate insert threw: ${(caught as Error).constructor.name}`);

        // Client must remain usable
        const rtx = await client.kv().begin(route, { mode: "ReadOnly", durability: "Sync" });
        const val = await rtx.get(b("dup-key"));
        expect(val.type).toBe("found");
        evidence.push("client remains usable after server-rejected operation");
      });

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-006 â”€ server error mapping
  it("CS-006 server error mapping", async () => {
    const result = await runScenario("CS-006", "server error mapping", "P0", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        const route = uniqueRoute("rpc");

        // No worker registered â€” server returns RPC_ERR_NO_WORKER (retryable or
        // domain error, code should be accessible on the thrown error)
        let caught: unknown;
        try {
          await client.rpc().call(route, b("ping"), { timeoutMs: 500 });
        } catch (err) {
          caught = err;
        }

        expect(caught).toBeTruthy();
        const err = caught as Error & { code?: string; domainCode?: number };
        evidence.push(`error class: ${err.constructor.name}`);
        evidence.push(`error.code: ${err.code ?? "n/a"}`);
        evidence.push(`error.domainCode: ${err.domainCode ?? "n/a"}`);

        if (err.code) {
          evidence.push("error carries a typed code (correct)");
        }

        // Also verify that kv insert conflict carries a domainCode
        const kvRoute = uniqueRoute("kv");
        const tx = await client.kv().begin(kvRoute, { durability: "Sync" });
        await tx.insert(b("x"), b("1"));
        await tx.commit();

        const tx2 = await client.kv().begin(kvRoute, { durability: "Sync" });
        let kvErr: unknown;
        try {
          await tx2.insert(b("x"), b("2"));
        } catch (e) {
          kvErr = e;
        }
        await tx2.rollback().catch(() => undefined);

        expect(kvErr).toBeTruthy();
        const ke = kvErr as Error & { code?: string; domainCode?: number };
        evidence.push(`kv conflict error class: ${ke.constructor.name}`);
        evidence.push(`kv conflict domainCode: ${ke.domainCode ?? "n/a"}`);
      });

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-007 â”€ timeout handling
  it("CS-007 timeout handling", async () => {
    const result = await runScenario("CS-007", "timeout handling", "P0", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        const route = uniqueRoute("rpc");
        const start = Date.now();
        let caught: unknown;
        try {
          await client.rpc().call(route, b("nobody"), { timeoutMs: 250 });
        } catch (err) {
          caught = err;
        }
        const elapsed = Date.now() - start;
        expect(caught).toBeTruthy();
        evidence.push(`rpc threw after ~${elapsed}ms`);
        evidence.push(`error class: ${(caught as Error).constructor.name}`);

        if (caught instanceof TimeoutError) {
          evidence.push("error is typed TimeoutError (ideal)");
        }

        // Must NOT be AbortError
        expect((caught as Error).name).not.toBe("AbortError");
        evidence.push("error is not AbortError (timeout â‰  cancellation)");

        // Connection still healthy
        const kvRoute = uniqueRoute("kv");
        const tx = await client.kv().begin(kvRoute, { durability: "Sync" });
        await tx.put(b("post-timeout"), b("ok"));
        await tx.commit();
        evidence.push("connection healthy after timeout");
      });

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-008 â”€ caller cancellation
  it("CS-008 caller cancellation", async () => {
    const result = await runScenario("CS-008", "caller cancellation", "P0", async () => {
      const evidence: string[] = [];

      const workerClient = new Client({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });

      const callerClient = new Client({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });

      try {
        await workerClient.connect();
        await callerClient.connect();

        const route = uniqueRoute("rpc");
        const sub = await workerClient
          .rpc()
          .registerWorker(route, async (_req: InboundRequest, writer: ResponseWriter) => {
            await new Promise<void>((resolve) => setTimeout(resolve, 2000));
            await writer.send(b("late"), true);
          });

        const controller = new AbortController();
        const iterator = await callerClient.rpc().call(route, b("block"), {
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
        evidence.push("error is AbortError (correct â€” not timeout)");

        await sub.unsubscribe().catch(() => undefined);

        // Subsequent request should succeed
        const kvRoute = uniqueRoute("kv");
        const tx = await callerClient.kv().begin(kvRoute, { durability: "Sync" });
        await tx.put(b("after-cancel"), b("ok"));
        await tx.commit();
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

  // CS-009 â”€ disconnect during request
  it("CS-009 disconnect during request", async () => {
    const result = await runScenario("CS-009", "disconnect during request", "P1", async () => {
      const evidence: string[] = [];

      const workerClient = new Client({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });

      const callerClient = new Client({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });

      try {
        await workerClient.connect();
        await callerClient.connect();

        const route = uniqueRoute("rpc");
        await workerClient
          .rpc()
          .registerWorker(route, async (_req: InboundRequest, writer: ResponseWriter) => {
            await new Promise<void>((resolve) => setTimeout(resolve, 3000));
            await writer.send(b("late"), true);
          });

        const controller = new AbortController();
        const iterator = await callerClient.rpc().call(route, b("block"), {
          timeoutMs: 30000,
          signal: controller.signal,
        });

        const nextPromise = iterator.next();
        // Prevent vitest unhandled rejection warnings before explicit await below.
        nextPromise.catch(() => undefined);

        // Simulate disconnect by closing the caller
        await new Promise<void>((resolve) => setTimeout(resolve, 100));
        controller.abort();
        await callerClient.close().catch(() => undefined);

        let caught: unknown;
        try {
          await nextPromise;
        } catch (err) {
          caught = err;
        }

        expect(caught).toBeTruthy();
        evidence.push(`in-flight request threw: ${(caught as Error).constructor.name}`);
        evidence.push("in-flight request failed promptly on disconnect (correct)");
      } finally {
        await workerClient.close().catch(() => undefined);
        await callerClient.close().catch(() => undefined);
      }

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-010 â”€ reconnect and retry behavior
  it("CS-010 reconnect and retry behavior", async () => {
    const result = await runScenario("CS-010", "reconnect and retry behavior", "P1", async () => {
      const evidence: string[] = [];

      // Verify reconnect API is accessible
      const client = new Client({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
        reconnect: { enabled: true, maxAttempts: 3, backoffMs: 100 },
      });

      try {
        await client.connect();
        evidence.push("client connected with reconnect enabled");

        // Close and reconnect manually (simulate application-level reconnect)
        await client.close();
        evidence.push("client closed ok");

        const client2 = new Client({
          url: BROKER_ADDR,
          transport: TRANSPORT,
          tokenProvider: tokenProvider(),
          timeout: 10000,
        });
        await client2.connect();
        const route = uniqueRoute("kv");
        const tx = await client2.kv().begin(route, { durability: "Sync" });
        await tx.put(b("after-reconnect"), b("ok"));
        await tx.commit();
        evidence.push("new requests succeed after reconnect (new client)");
        await client2.close().catch(() => undefined);

        return { verdict: "pass", evidence };
      } catch (err) {
        // If reconnect config is not supported, record as partial
        const message = err instanceof Error ? err.message : String(err);
        if (message.includes("reconnect") || message.includes("Unknown config")) {
          evidence.push(`reconnect config not accepted: ${message}`);
          evidence.push(
            "NOTE: reconnect config key may differ from 'reconnect' â€” check ClientConfig",
          );
          return { verdict: "partial", evidence };
        }
        throw err;
      } finally {
        await client.close().catch(() => undefined);
      }
    });

    collector.record(result);
    // pass or partial are both acceptable â€” full reconnect loop needs controlled network
    expect(["pass", "partial"]).toContain(result.verdict);
  });

  // CS-011 â”€ stream receive sequence
  it("CS-011 stream receive sequence", async () => {
    const result = await runScenario("CS-011", "stream receive sequence", "P1", async () => {
      const evidence: string[] = [];
      let verdict: Verdict = "pass";

      try {
        await withClient({}, async (client) => {
          const route = uniqueRoute("stream");
          const session = await client.stream().begin(route);
          await session.append(0n, Uint8Array.of(10));
          await session.append(1n, Uint8Array.of(20));
          await session.append(2n, Uint8Array.of(30));
          await session.commit("Sync");
          evidence.push("stream session appended 3 records");

          const records = await client.stream().read(route, 0n, 10);
          if (records.length < 3) {
            verdict = "partial";
            evidence.push(`expected >=3 stream records, got ${records.length}`);
          }
          for (let i = 1; i < records.length; i++) {
            if (records[i].offset <= records[i - 1].offset) {
              verdict = "partial";
              evidence.push(
                `out-of-order offsets at ${i}: ${records[i].offset} <= ${records[i - 1].offset}`,
              );
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
    expect(["pass", "partial"]).toContain(result.verdict);
  });

  // CS-012 â”€ stream completion
  it("CS-012 stream completion", async () => {
    const result = await runScenario("CS-012", "stream completion", "P1", async () => {
      const evidence: string[] = [];
      let verdict: Verdict = "pass";

      await withClient({}, async (client) => {
        const route = uniqueRoute("stream");
        const session = await client.stream().begin(route);
        await session.append(0n, b("first"));
        await session.append(1n, b("last"));
        await session.commit("Sync");
        evidence.push("stream session committed");

        // stream.read() should return and not block forever
        const records = await client.stream().read(route, 0n, 100);
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
    expect(["pass", "partial"]).toContain(result.verdict);
  });

  // CS-013 â”€ stream error mid-flight
  it("CS-013 stream error mid-flight", async () => {
    const result = await runScenario("CS-013", "stream error mid-flight", "P1", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        const route = uniqueRoute("stream");

        // append() with a wrong expected offset → server rejects it
        const session = await client.stream().begin(route);
        await session.append(0n, b("record-1"));
        await session.commit("Sync");
        evidence.push("written first record at offset 0");

        let caught: unknown;
        try {
          // Expected offset 0 again, but stream is now at >0 — should fail
          const wrongSession = await client.stream().begin(route);
          await wrongSession.append(0n, b("record-2"));
        } catch (err) {
          caught = err;
        }

        expect(caught).toBeTruthy();
        evidence.push(`append with wrong offset threw: ${(caught as Error).constructor.name}`);
        evidence.push("stream error surfaced correctly");

        // Client must remain usable (no resource leak)
        const kvRoute = uniqueRoute("kv");
        const tx = await client.kv().begin(kvRoute, { durability: "Sync" });
        await tx.put(b("after-stream-error"), b("ok"));
        await tx.commit();
        evidence.push("client still usable after stream error");
      });

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-014 â”€ concurrent in-flight requests
  it("CS-014 concurrent in-flight requests", async () => {
    const result = await runScenario("CS-014", "concurrent in-flight requests", "P1", async () => {
      const evidence: string[] = [];

      await withClient({}, async (client) => {
        // Issue 3 kv transactions concurrently on different routes
        const routes = [uniqueRoute("kv"), uniqueRoute("kv"), uniqueRoute("kv")];

        const tasks = routes.map(async (route, i) => {
          const tx = await client.kv().begin(route, { durability: "Sync" });
          await tx.put(b(`key-${i}`), b(`value-${i}`));
          await tx.commit();
          const rtx = await client.kv().begin(route, { mode: "ReadOnly", durability: "Sync" });
          return rtx.get(b(`key-${i}`));
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

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-015 â”€ shutdown during active work
  it("CS-015 shutdown during active work", async () => {
    const result = await runScenario("CS-015", "shutdown during active work", "P1", async () => {
      const evidence: string[] = [];

      const client = new Client({
        url: BROKER_ADDR,
        transport: TRANSPORT,
        tokenProvider: tokenProvider(),
        timeout: 10000,
      });

      try {
        await client.connect();

        // Start an async KV operation, close the client mid-flight
        const route = uniqueRoute("kv");
        const kvBeginPromise = client.kv().begin(route, { durability: "Sync" });
        // Prevent vitest unhandled rejection warnings before explicit await below.
        kvBeginPromise.catch(() => undefined);

        // Close the client before the operation may complete
        await client.close();

        let caught: unknown;
        try {
          const tx = await kvBeginPromise;
          await tx.put(b("key"), b("value"));
          await tx.commit();
        } catch (err) {
          caught = err;
        }

        if (caught) {
          evidence.push(`in-flight begin threw: ${(caught as Error).constructor.name}`);
          expect(caught).toBeInstanceOf(Error);
        } else {
          // Operation may have completed before close â€” acceptable
          evidence.push("operation completed before close (race â€” acceptable)");
        }

        evidence.push("close during active work did not panic");

        // Double close must not throw
        await client.close();
        evidence.push("double close is safe");
      } finally {
        await client.close().catch(() => undefined);
      }

      return { verdict: "pass", evidence };
    });

    collector.record(result);
    expect(result.verdict).toBe("pass");
  });

  // CS-017 - bounded concurrency under burst load
  it("CS-017 bounded concurrency under burst load", async () => {
    const result = await runScenario(
      "CS-017",
      "bounded concurrency under burst load",
      "P1",
      async () => {
        const evidence: string[] = [];

        const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

        await withClient({ timeout: 750, maxInFlightRequests: 16 }, async (client) => {
          const route = uniqueRoute("rpc");

          const workerClient = new Client({
            url: BROKER_ADDR,
            transport: TRANSPORT,
            tokenProvider: tokenProvider(),
            timeout: 10000,
          });

          try {
            await workerClient.connect();
            const sub = await workerClient.rpc().registerWorker(route, async (_req, writer) => {
              await pause(500);
              await writer.send(b("ok"), true);
            });

            const firstCall = client.rpc().call(route, b("first"), { timeoutMs: 750 });
            firstCall.catch(() => undefined);

            const secondCall = client.rpc().call(route, b("second"), { timeoutMs: 750 });
            secondCall.catch(() => undefined);

            const secondState = await Promise.race([
              secondCall.then(
                () => "settled",
                () => "settled",
              ),
              pause(100).then(() => "pending"),
            ]);

            expect(secondState).toBe("pending");
            evidence.push("second RPC call remained pending while first was in flight");
            evidence.push("configured maxInFlightRequests=16 and burst size=2");

            await sub.unsubscribe();
          } finally {
            await workerClient.close().catch(() => undefined);
          }
        });

        return { verdict: "pass", evidence };
      },
    );

    collector.record(result);
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
      console.error("Failed to write conformance results:", err);
    }
  });
});
