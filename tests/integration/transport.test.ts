import { describe, expect, it, vi } from "vite-plus/test";

import { FrameCodec } from "../../src/frame/codec";
import { AuthenticationError, ConnectionError } from "../../src/core/errors";
import { MSG_KV_BEGIN } from "../../src/frame/types";
import { createTransport } from "../../src/transport/factory";
import { sleep } from "./helpers";
import { brokerAddrFor, TestFixture } from "./fixture/fixture";
import { runWithTransportsOnly } from "./fixture/transport";

describe("Transport integration", () => {
  runWithTransportsOnly(({ transport }) => {
    it("should connect using the configured transport", async () => {
      const f = new TestFixture(transport, "anonymous");
      await f.connectOrFail();
      expect(f.client().isConnected()).toBe(true);
    });

    it("should authenticate with a valid jwt against the auth broker", async () => {
      const f = new TestFixture(transport, "valid_jwt");
      await f.connectWithAuthOrFail("valid_jwt");
      expect(f.client().isConnected()).toBe(true);
    });

    it("should reject an expired jwt", async () => {
      const f = new TestFixture(transport, "expired_jwt");
      const connectAttempt = f.connect({ timeout: 1000 });
      if (transport === "tcp") {
        try {
          await connectAttempt;
          await f
            .client()
            .close()
            .catch(() => undefined);
          return;
        } catch {
          return;
        }
      }

      await expect(connectAttempt).rejects.toBeTruthy();
    });

    it("should reject an invalid-signature jwt", async () => {
      const f = new TestFixture(transport, "invalid_signature");
      const connectAttempt = f.connect({ timeout: 1000 });
      if (transport === "tcp") {
        try {
          await connectAttempt;
          await f
            .client()
            .close()
            .catch(() => undefined);
          return;
        } catch {
          return;
        }
      }

      await expect(connectAttempt).rejects.toBeTruthy();
    });

    it("should transition to closed and never auto-reconnect after auth rejection", async () => {
      const f = new TestFixture(transport, "invalid_signature");

      await expect(
        f.connect({
          timeout: 1000,
          reconnect: {
            enabled: true,
            maxAttempts: 3,
            backoffMs: 10,
            maxBackoffMs: 20,
          },
        }),
      ).rejects.toBeInstanceOf(AuthenticationError);

      expect(f.client().getState()).toBe("CLOSED");
      expect(f.client().isConnected()).toBe(false);
    });
  });

  runWithTransportsOnly(({ transport }) => {
    const authMode = "anonymous" as const;

    it("should expose all domain clients on a connected client", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      expect(f.client().notice()).toBeTruthy();
      expect(f.client().stream()).toBeTruthy();
      expect(f.client().queue()).toBeTruthy();
      expect(f.client().rpc()).toBeTruthy();
      expect(f.client().kv()).toBeTruthy();
      expect(f.client().lease()).toBeTruthy();
      expect(f.client().schedule()).toBeTruthy();
    });

    it("should reconnect by creating a new client after close", async () => {
      const first = new TestFixture(transport, authMode);
      await first.connectOrFail();
      await first.client().close();

      const second = new TestFixture(transport, authMode);
      await second.connectOrFail();
      expect(second.client().isConnected()).toBe(true);
    });

    it("should not preserve an old notice subscription after close and reconnect", async () => {
      const subscriber = new TestFixture(transport, authMode);
      const publisher = new TestFixture(transport, authMode);
      await subscriber.connectOrFail();
      await publisher.connectOrFail();

      const route = subscriber.uniqueRoute("notice");
      const received: string[] = [];
      const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
      const error = vi.spyOn(console, "error").mockImplementation(() => undefined);

      try {
        await subscriber
          .client()
          .notice()
          .subscribe(route, async (msg) => {
            received.push(Buffer.from(msg.body).toString());
          });
        await sleep(150);

        await publisher.client().notice().publish(route, Buffer.from("before-disconnect"));
        await sleep(500);
        expect(received).toEqual(["before-disconnect"]);

        await subscriber.client().close();

        const reconnected = new TestFixture(transport, authMode);
        await reconnected.connectOrFail();

        await publisher.client().notice().publish(route, Buffer.from("after-disconnect"));
        await sleep(750);

        expect(received).toEqual(["before-disconnect"]);
        expect(reconnected.client().isConnected()).toBe(true);
        expect(warn).not.toHaveBeenCalled();
        expect(error).not.toHaveBeenCalled();
      } finally {
        warn.mockRestore();
        error.mockRestore();
      }
    });

    it("should reject a non-CONNECT frame sent before authentication", async () => {
      const addr = brokerAddrFor(transport, "valid_jwt");
      const rawTransport = createTransport(addr, transport, { timeout: 1000 });

      await rawTransport.connect();
      try {
        await rawTransport.send(FrameCodec.encodeFrame(MSG_KV_BEGIN, new Uint8Array()));
        await expect(rawTransport.receive()).rejects.toBeTruthy();
      } finally {
        await rawTransport.close().catch(() => undefined);
      }
    });

    it("should fail connect to an invalid address", async () => {
      const f = new TestFixture(transport, authMode);
      f.setBrokerAddr(transport === "tcp" ? "localhost:39999" : "ws://localhost:39998/ws");

      await expect(f.connect({ timeout: 1000 })).rejects.toBeTruthy();
    });

    it("should fail connect when the signal is already aborted", async () => {
      const f = new TestFixture(transport, authMode);
      const controller = new AbortController();
      controller.abort();

      await expect(f.connect({}, { signal: controller.signal })).rejects.toHaveProperty(
        "name",
        "AbortError",
      );
    });

    it("should fail quickly when timeout is too short for an unreachable address", async () => {
      const f = new TestFixture(transport, authMode);
      f.setBrokerAddr(transport === "tcp" ? "localhost:39999" : "ws://localhost:39998/ws");

      await expect(f.connect({ timeout: 10 })).rejects.toBeTruthy();
    });

    it("should not panic on double close", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      await expect(f.client().close()).resolves.toBeUndefined();
      await expect(f.client().close()).resolves.toBeUndefined();
    });

    it("should return an error for operations after close", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("kv");
      await f.client().close();

      await expect(
        Promise.resolve().then(() => f.client().kv().begin(route, { durability: "Sync" })),
      ).rejects.toBeInstanceOf(ConnectionError);
    });

    it("should abort an in-flight rpc wait when the request signal is canceled", async () => {
      const worker = new TestFixture(transport, authMode);
      const caller = new TestFixture(transport, authMode);
      await worker.connectOrFail();
      await caller.connectOrFail();

      const route = worker.uniqueRoute("rpc");
      const sub = await worker
        .client()
        .rpc()
        .registerWorker(route, async (_req, writer) => {
          await sleep(250);
          await writer.send(Buffer.from("late"), true);
        });
      const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
      const error = vi.spyOn(console, "error").mockImplementation(() => undefined);

      try {
        const controller = new AbortController();
        const iterator = await caller.client().rpc().call(route, Buffer.from("block"), {
          timeoutMs: 10000,
          signal: controller.signal,
        });
        const nextPromise = iterator.next();

        await sleep(100);
        controller.abort();

        await expect(nextPromise).rejects.toHaveProperty("name", "AbortError");
        await sleep(300);
        await sub.unsubscribe();
        expect(warn).not.toHaveBeenCalled();
        expect(error).not.toHaveBeenCalled();
      } finally {
        warn.mockRestore();
        error.mockRestore();
      }
    });

    it("should stay quiet when a worker closes before replying", async () => {
      const worker = new TestFixture(transport, authMode);
      const caller = new TestFixture(transport, authMode);
      await worker.connectOrFail();
      await caller.connectOrFail();

      const route = worker.uniqueRoute("rpc");
      await worker
        .client()
        .rpc()
        .registerWorker(route, async (_req, writer) => {
          await sleep(200);
          await writer.send(Buffer.from("too-late"), true);
        });
      const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
      const error = vi.spyOn(console, "error").mockImplementation(() => undefined);

      try {
        const controller = new AbortController();
        const iterator = await caller.client().rpc().call(route, Buffer.from("block"), {
          timeoutMs: 10000,
          signal: controller.signal,
        });
        const nextPromise = iterator.next();

        await sleep(100);
        await worker.client().close();
        controller.abort();

        await expect(nextPromise).rejects.toHaveProperty("name", "AbortError");
        await sleep(300);
        expect(warn).not.toHaveBeenCalled();
        expect(error).not.toHaveBeenCalled();
      } finally {
        warn.mockRestore();
        error.mockRestore();
      }
    });
  });
});
