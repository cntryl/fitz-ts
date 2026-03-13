import { describe, expect, it } from "vitest";

import { collectAsyncIterable } from "./helpers";
import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";

const b = (value: string) => Buffer.from(value);

describe("KV integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should open and commit transaction", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv().begin(route);
      await tx.put(b("user:123"), b("Alice"));
      await tx.commit();

      const rtx = await f.client().kv().begin(route, "ReadOnly");
      const result = await rtx.get(b("user:123"));
      expect(result.type).toBe("found");
      if (result.type === "found") {
        expect(Buffer.from(result.value).toString()).toBe("Alice");
      }
    });

    it("should read existing value", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv().begin(route);
      await tx.put(b("colour"), b("blue"));
      await tx.commit();

      const rtx = await f.client().kv().begin(route, "ReadOnly");
      const result = await rtx.get(b("colour"));
      expect(result.type).toBe("found");
      if (result.type === "found") {
        expect(Buffer.from(result.value).toString()).toBe("blue");
      }
    });

    it("should return not found for missing key", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const rtx = await f.client().kv().begin(f.uniqueRoute("kv"), "ReadOnly");
      const result = await rtx.get(b("missing"));
      expect(result).toEqual({ type: "not-found" });
    });

    it("should write value given valid key", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv().begin(route);
      await tx.put(b("k1"), b("v1"));
      const result = await tx.get(b("k1"));
      expect(result.type).toBe("found");
      await tx.commit();
    });

    it("should insert new key", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv().begin(route);
      await tx.insert(b("new-key"), b("new-value"));
      const result = await tx.get(b("new-key"));
      expect(result.type).toBe("found");
      await tx.commit();
    });

    it("should fail insert on existing key", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv().begin(route);
      await tx.insert(b("dup"), b("first"));
      await tx.commit();

      const tx2 = await f.client().kv().begin(route);
      await expect(tx2.insert(b("dup"), b("second"))).rejects.toBeTruthy();
      await tx2.rollback();
    });

    it("should delete key", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv().begin(route);
      await tx.put(b("to-delete"), b("value"));
      await tx.commit();

      const tx2 = await f.client().kv().begin(route);
      await tx2.delete(b("to-delete"));
      await tx2.commit();

      const rtx = await f.client().kv().begin(route, "ReadOnly");
      expect(await rtx.get(b("to-delete"))).toEqual({ type: "not-found" });
    });

    it("should scan keys in order", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv().begin(route);
      await tx.put(b("b"), b("2"));
      await tx.put(b("a"), b("1"));
      await tx.put(b("c"), b("3"));
      await tx.commit();

      const rtx = await f.client().kv().begin(route, "ReadOnly");
      const keys = await collectAsyncIterable(
        await rtx.scan({ startKey: b("a"), endKey: b("d"), limit: 10 }),
      );
      expect(keys.map((key) => Buffer.from(key).toString())).toEqual([
        "a",
        "b",
        "c",
      ]);
    });

    it("should delete range", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv().begin(route);
      await tx.put(b("a"), b("1"));
      await tx.put(b("b"), b("2"));
      await tx.put(b("c"), b("3"));
      await tx.put(b("d"), b("4"));
      await tx.commit();

      const tx2 = await f.client().kv().begin(route);
      await tx2.deleteRange(b("b"), b("d"));
      await tx2.commit();

      const rtx = await f.client().kv().begin(route, "ReadOnly");
      const keys = await collectAsyncIterable(
        await rtx.scan({ startKey: b("a"), endKey: b("z"), limit: 10 }),
      );
      expect(keys.map((key) => Buffer.from(key).toString())).toEqual([
        "a",
        "d",
      ]);
    });

    it("should respect scan limit", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv().begin(route);
      await tx.put(b("a"), b("1"));
      await tx.put(b("b"), b("2"));
      await tx.put(b("c"), b("3"));
      await tx.commit();

      const rtx = await f.client().kv().begin(route, "ReadOnly");
      const keys = await collectAsyncIterable(
        await rtx.scan({ startKey: b("a"), endKey: b("z"), limit: 2 }),
      );
      expect(keys).toHaveLength(2);
    });

    it("should rollback changes", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv().begin(route);
      await tx.put(b("ephemeral"), b("gone"));
      await tx.rollback();

      const rtx = await f.client().kv().begin(route, "ReadOnly");
      expect(await rtx.get(b("ephemeral"))).toEqual({ type: "not-found" });
    });

    it("should isolate transactions on same resource", async () => {
      const f1 = new TestFixture(transport, authMode);
      const f2 = new TestFixture(transport, authMode);
      await f1.connectOrFail();
      await f2.connectOrFail();
      const route = f1.uniqueRoute("kv");

      const tx1 = await f1.client().kv().begin(route);
      await expect(f2.client().kv().begin(route)).rejects.toBeTruthy();
      await tx1.rollback();
    });

    it("should reject write in read only mode", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv().begin(route, "ReadOnly");
      await expect(tx.put(b("k"), b("v"))).rejects.toBeTruthy();
      await tx.rollback();
    });

    it("should reject invalid route", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      await expect(
        f.client().kv().begin("invalid-route-not-kv-format"),
      ).rejects.toBeTruthy();
    });

    it("should reject second commit after commit", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv().begin(route);
      await tx.put(b("k"), b("v"));
      await tx.commit();
      await expect(tx.commit()).rejects.toBeTruthy();
    });
  });
});
