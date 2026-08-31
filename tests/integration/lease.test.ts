import { describe, expect, it } from "vite-plus/test";

import { waitFor } from "./helpers";
import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";
import { ErrCodeLeaseHeld, LeaseError } from "../../src";

const inventoryIt = it.skipIf(process.env.FITZ_PATTERNED_LEASE_INVENTORY === "0");

describe("Lease integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should acquire lease when it is free", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const lease = await f.client().lease.acquire(f.uniqueRoute("lease"), { ttlSeconds: 30 });
      expect(lease).toBeTruthy();
      expect(lease.getExpiry()).toBeGreaterThan(BigInt(Math.floor(Date.now() / 1000)));
    });

    it("should expose lease held code given held lease when acquiring", async () => {
      const f1 = new TestFixture(transport, authMode);
      const f2 = new TestFixture(transport, authMode);
      await f1.connectOrFail();
      await f2.connectOrFail();

      const route = f1.uniqueRoute("lease");
      const lease = await f1.client().lease.acquire(route, { ttlSeconds: 30 });
      expect(lease.getExpiry()).toBeGreaterThan(BigInt(Math.floor(Date.now() / 1000)));

      await expect(f2.client().lease.acquire(route, { ttlSeconds: 30 })).rejects.toMatchObject({
        name: LeaseError.name,
        domainCode: ErrCodeLeaseHeld,
      });
    });

    it("should extend ttl when renew is called with a valid token", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const lease = await f.client().lease.acquire(f.uniqueRoute("lease"), { ttlSeconds: 10 });
      const originalExpiry = lease.getExpiry();
      const newExpiry = await lease.extend({ ttlSeconds: 60 });

      expect(newExpiry).toBeGreaterThan(originalExpiry);
      expect(lease.getExpiry()).toBe(newExpiry);
    });

    it("should reject renew when token does not match", async () => {
      const f1 = new TestFixture(transport, authMode);
      const f2 = new TestFixture(transport, authMode);
      await f1.connectOrFail();
      await f2.connectOrFail();

      const route = f1.uniqueRoute("lease");
      const staleLease = await f1.client().lease.acquire(route, { ttlSeconds: 1 });
      await waitFor(
        async () => {
          try {
            await f2.client().lease.acquire(route, { ttlSeconds: 30 });
            return true;
          } catch {
            return false;
          }
        },
        {
          timeoutMs: 3000,
          intervalMs: 100,
          timeoutMessage: "lease was not reacquired after ttl expiry",
        },
      );

      await expect(staleLease.extend({ ttlSeconds: 60 })).rejects.toBeTruthy();
    });

    it("should release lease when token is valid", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("lease");
      const lease = await f.client().lease.acquire(route, { ttlSeconds: 30 });
      await lease.release();

      const reacquired = await f.client().lease.acquire(route, { ttlSeconds: 30 });
      expect(reacquired.getExpiry()).toBeGreaterThan(BigInt(Math.floor(Date.now() / 1000)));
    });

    it("should advance managed admission fencing tokens after handoff", async () => {
      const first = new TestFixture(transport, authMode);
      const successor = new TestFixture(transport, authMode);
      await first.connectOrFail();
      await successor.connectOrFail();

      const route = first.uniqueRoute("lease");
      let firstToken: bigint | undefined;
      await first.client().lease.withLease(
        route,
        (_signal, authority) => {
          firstToken = authority.fencingToken;
        },
        { ttlSeconds: 30 },
      );

      let successorToken: bigint | undefined;
      await successor.client().lease.withLease(
        route,
        (_signal, authority) => {
          successorToken = authority.fencingToken;
        },
        { ttlSeconds: 30 },
      );

      expect(firstToken).toBeTypeOf("bigint");
      expect(successorToken).toBeTypeOf("bigint");
      expect(successorToken!).toBeGreaterThan(firstToken!);
    });

    it("should reject release when token does not match", async () => {
      const f1 = new TestFixture(transport, authMode);
      const f2 = new TestFixture(transport, authMode);
      await f1.connectOrFail();
      await f2.connectOrFail();

      const route = f1.uniqueRoute("lease");
      const staleLease = await f1.client().lease.acquire(route, { ttlSeconds: 1 });
      await waitFor(
        async () => {
          try {
            await f2.client().lease.acquire(route, { ttlSeconds: 30 });
            return true;
          } catch {
            return false;
          }
        },
        {
          timeoutMs: 3000,
          intervalMs: 100,
          timeoutMessage: "lease was not reacquired after ttl expiry",
        },
      );

      await expect(staleLease.release()).rejects.toBeTruthy();
    });

    it("should allow re-acquire after ttl expires", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("lease");
      const lease = await f.client().lease.acquire(route, { ttlSeconds: 1 });
      expect(lease.getExpiry()).toBeGreaterThan(BigInt(Math.floor(Date.now() / 1000)));

      let reacquired: { getExpiry(): bigint } | null = null;
      await waitFor(
        async () => {
          try {
            reacquired = await f.client().lease.acquire(route, { ttlSeconds: 30 });
            return true;
          } catch {
            return false;
          }
        },
        {
          timeoutMs: 3000,
          intervalMs: 100,
          timeoutMessage: "lease was not reacquired after ttl expiry",
        },
      );

      if (!reacquired) {
        throw new Error("lease was not reacquired after ttl expiry");
      }
      const reacquiredLease = reacquired as { getExpiry(): bigint };
      expect(reacquiredLease.getExpiry()).toBeGreaterThan(BigInt(Math.floor(Date.now() / 1000)));
    });

    it("should query lease status for an existing lease", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("lease");
      await f.client().lease.acquire(route, { ttlSeconds: 30 });

      const info = await f.client().lease.query(route);
      expect(info.isHeld).toBe(true);
      expect(
        info.owner !== undefined || info.ttlRemainingSecs !== undefined || info.token !== undefined,
      ).toBe(true);
    });

    it("should deliver subscription notifications on release", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("lease");
      let resolveNotification!: (value: string) => void;
      let timer: ReturnType<typeof setTimeout>;
      const notification = new Promise<string>((resolve, reject) => {
        resolveNotification = resolve;
        timer = setTimeout(() => {
          reject(new Error("timed out waiting for lease notification"));
        }, 5000);
      });

      const subscription = await f.client().lease.subscribe(route, async (notif) => {
        clearTimeout(timer);
        resolveNotification(notif.route);
      });

      const lease = await f.client().lease.acquire(route, { ttlSeconds: 30 });
      await lease.release();
      await expect(notification).resolves.toBe(route);
      await subscription.unsubscribe();

      await expect(notification).resolves.toBe(route);
    });

    inventoryIt("should list a held lease matching a wildcard pattern", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("lease");
      const [realm, area] = route.slice("lease://".length).split("/");
      const pattern = `lease://${realm}/${area}/*`;
      const lease = await f.client().lease.acquire(route, { ttlSeconds: 30 });

      const page = await f.client().lease.listPage(pattern);
      expect(page.items.some((item) => item.route === route)).toBe(true);

      await lease.release();
    });

    inventoryIt("should page through list() results using the returned cursor", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const realm = f.uniqueRealm();
      const area = f.uniqueArea();
      const routes = [
        `lease://${realm}/${area}/${f.uniqueResource()}`,
        `lease://${realm}/${area}/${f.uniqueResource()}`,
      ];
      const leases = await Promise.all(
        routes.map((route) => f.client().lease.acquire(route, { ttlSeconds: 30 })),
      );

      const seen = new Set<string>();
      for await (const page of f.client().lease.list(`lease://${realm}/${area}/*`, {
        pageSize: 1,
      })) {
        for (const item of page) seen.add(item.route);
      }
      for (const route of routes) expect(seen.has(route)).toBe(true);

      await Promise.all(leases.map((lease) => lease.release()));
    });

    inventoryIt("observeInventory() bootstraps a view and tracks acquire/release", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const realm = f.uniqueRealm();
      const area = f.uniqueArea();
      const pattern = `lease://${realm}/${area}/*`;
      const route = `lease://${realm}/${area}/${f.uniqueResource()}`;

      await using observer = await f.client().lease.observeInventory(pattern);
      expect(observer.ready).toBe(true);
      expect(observer.snapshot().has(route)).toBe(false);

      const lease = await f.client().lease.acquire(route, { ttlSeconds: 30 });
      await waitFor(() => observer.snapshot().has(route), {
        timeoutMessage: "observer never picked up the acquired route",
      });
      expect(observer.snapshot().get(route)?.route).toBe(route);

      await lease.release();
      await waitFor(() => !observer.snapshot().has(route), {
        timeoutMessage: "observer never dropped the released route",
      });
    });
  });
});
