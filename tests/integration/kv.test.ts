import { describe, expect, it } from "vite-plus/test";

import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";

const b = (value: string) => Buffer.from(value);

describe("KV integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should deliver the exact KV route for a wildcard subscription", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const uniqueParts = f.uniqueRoute("kv").slice("kv://".length).split("/");
      const realm = uniqueParts[0]!;
      const uniqueArea = uniqueParts.at(-1)!;
      const route = `kv://${realm}/${uniqueArea}/resource`;
      let resolveNotification: ((value: { route: string; mutationCount: bigint }) => void) | null =
        null;
      const notification = new Promise<{ route: string; mutationCount: bigint }>((resolve) => {
        resolveNotification = resolve;
      });
      const subscription = await f
        .client()
        .kv.subscribe(`kv://${realm}/${uniqueArea}/**`, async (value) =>
          resolveNotification?.(value),
        );
      f.addCleanup(() => subscription.unsubscribe());

      const tx = await f.client().kv.begin(route, { durability: "Sync" });
      await tx.put({ key: b("key"), value: b("value") });
      await tx.commit();

      await expect(notification).resolves.toEqual({ route, mutationCount: 1n });
    });

    it("should open and commit transaction", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });
      await tx.put({ key: b("user:123"), value: b("Alice") });
      await tx.commit();

      const rtx = await f.client().kv.begin(route, { mode: "ReadOnly", durability: "Sync" });
      const result = await rtx.get({ key: b("user:123") });
      expect(result.type).toBe("found");
      if (result.type === "found") {
        expect(Buffer.from(result.value).toString()).toBe("Alice");
      }
    });

    it("should read existing value", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });
      await tx.put({ key: b("colour"), value: b("blue") });
      await tx.commit();

      const rtx = await f.client().kv.begin(route, { mode: "ReadOnly", durability: "Sync" });
      const result = await rtx.get({ key: b("colour") });
      expect(result.type).toBe("found");
      if (result.type === "found") {
        expect(Buffer.from(result.value).toString()).toBe("blue");
      }
    });

    it("should return not found for missing key", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const rtx = await f.client().kv.begin(f.uniqueRoute("kv"), {
        mode: "ReadOnly",
        durability: "Sync",
      });
      const result = await rtx.get({ key: b("missing") });
      expect(result).toEqual({ type: "not-found" });
    });

    it("should write value given valid key", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });
      await tx.put({ key: b("k1"), value: b("v1") });
      const result = await tx.get({ key: b("k1") });
      expect(result.type).toBe("found");
      await tx.commit();
    });

    it("should insert new key", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });
      await tx.insert({ key: b("new-key"), value: b("new-value") });
      const result = await tx.get({ key: b("new-key") });
      expect(result.type).toBe("found");
      await tx.commit();
    });

    it("should fail insert on existing key", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });
      await tx.insert({ key: b("dup"), value: b("first") });
      await tx.commit();

      const tx2 = await f.client().kv.begin(route, { durability: "Sync" });
      await expect(tx2.insert({ key: b("dup"), value: b("second") })).rejects.toBeTruthy();
      await tx2.rollback();
    });

    it("should delete key", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });
      await tx.put({ key: b("to-delete"), value: b("value") });
      await tx.commit();

      const tx2 = await f.client().kv.begin(route, { durability: "Sync" });
      await tx2.delete({ key: b("to-delete") });
      await tx2.commit();

      const rtx = await f.client().kv.begin(route, { mode: "ReadOnly", durability: "Sync" });
      expect(await rtx.get({ key: b("to-delete") })).toEqual({ type: "not-found" });
    });

    it("should scan keys in order", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });
      await tx.put({ key: b("b"), value: b("2") });
      await tx.put({ key: b("a"), value: b("1") });
      await tx.put({ key: b("c"), value: b("3") });
      await tx.commit();

      const rtx = await f.client().kv.begin(route, { mode: "ReadOnly", durability: "Sync" });
      const { entries: pairs } = await rtx.scan({
        startKey: b("a"),
        endKey: b("d"),
        limit: 10,
      });
      expect(pairs.map((pair) => Buffer.from(pair.key).toString())).toEqual(["a", "b", "c"]);
    });

    it("should delete range", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });
      await tx.put({ key: b("a"), value: b("1") });
      await tx.put({ key: b("b"), value: b("2") });
      await tx.put({ key: b("c"), value: b("3") });
      await tx.put({ key: b("d"), value: b("4") });
      await tx.commit();

      const tx2 = await f.client().kv.begin(route, { durability: "Sync" });
      await tx2.deleteRange({ startKey: b("b"), endKey: b("d") });
      await tx2.commit();

      const rtx = await f.client().kv.begin(route, { mode: "ReadOnly", durability: "Sync" });
      const { entries: pairs } = await rtx.scan({
        startKey: b("a"),
        endKey: b("z"),
        limit: 10,
      });
      expect(pairs.map((pair) => Buffer.from(pair.key).toString())).toEqual(["a", "d"]);
    });

    it("should respect scan limit", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });
      await tx.put({ key: b("a"), value: b("1") });
      await tx.put({ key: b("b"), value: b("2") });
      await tx.put({ key: b("c"), value: b("3") });
      await tx.commit();

      const rtx = await f.client().kv.begin(route, { mode: "ReadOnly", durability: "Sync" });
      const page = await rtx.scan({ startKey: b("a"), endKey: b("z"), limit: 2 });
      expect(page.entries).toHaveLength(2);
      expect(page.hasMore).toBe(true);
    });

    it("should rollback changes", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });
      await tx.put({ key: b("ephemeral"), value: b("gone") });
      await tx.rollback();

      const rtx = await f.client().kv.begin(route, { mode: "ReadOnly", durability: "Sync" });
      expect(await rtx.get({ key: b("ephemeral") })).toEqual({ type: "not-found" });
    });

    it("should isolate transactions on same resource", async () => {
      const f1 = new TestFixture(transport, authMode);
      const f2 = new TestFixture(transport, authMode);
      await f1.connectOrFail();
      await f2.connectOrFail();
      const route = f1.uniqueRoute("kv");

      const tx1 = await f1.client().kv.begin(route, { durability: "Sync" });
      await expect(f2.client().kv.begin(route, { durability: "Sync" })).rejects.toBeTruthy();
      await tx1.rollback();
    });

    it("should reject write in read only mode", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { mode: "ReadOnly", durability: "Sync" });
      await expect(tx.put({ key: b("k"), value: b("v") })).rejects.toBeTruthy();
      await tx.rollback();
    });

    it("should reject invalid route", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      await expect(
        f.client().kv.begin("invalid-route-not-kv-format", { durability: "Sync" }),
      ).rejects.toBeTruthy();
    });

    it("should reject inverted bounds given an invalid range when scan is called", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });

      await expect(tx.scan({ startKey: b("z"), endKey: b("a") })).rejects.toBeTruthy();
      await tx.rollback();
    });

    it("should reject inverted bounds given an invalid range when delete-range is called", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });

      await expect(tx.deleteRange({ startKey: b("z"), endKey: b("a") })).rejects.toBeTruthy();
      await tx.rollback();
    });

    it("should reject a second commit given a completed transaction when commit is called", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();
      const route = f.uniqueRoute("kv");

      const tx = await f.client().kv.begin(route, { durability: "Sync" });
      await tx.put({ key: b("k"), value: b("v") });
      await tx.commit();
      await expect(tx.commit()).rejects.toBeTruthy();
    });
  });
});
