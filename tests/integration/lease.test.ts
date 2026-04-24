import { describe, expect, it } from "vite-plus/test";

import { waitFor } from "./helpers";
import { TestFixture } from "./fixture/fixture";
import { runWithBothTransports } from "./fixture/transport";

describe("Lease integration", () => {
  runWithBothTransports(({ transport, authMode }) => {
    it("should acquire lease when it is free", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const lease = await f.client().lease().acquire(f.uniqueRoute("lease"), 30);
      expect(lease).toBeTruthy();
      expect(lease.getExpiry()).toBeGreaterThan(BigInt(Math.floor(Date.now() / 1000)));
    });

    it("should reject acquire when lease is already held", async () => {
      const f1 = new TestFixture(transport, authMode);
      const f2 = new TestFixture(transport, authMode);
      await f1.connectOrFail();
      await f2.connectOrFail();

      const route = f1.uniqueRoute("lease");
      const lease = await f1.client().lease().acquire(route, 30);
      expect(lease.getExpiry()).toBeGreaterThan(BigInt(Math.floor(Date.now() / 1000)));

      await expect(f2.client().lease().acquire(route, 30)).rejects.toBeTruthy();
    });

    it("should extend ttl when renew is called with a valid token", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const lease = await f.client().lease().acquire(f.uniqueRoute("lease"), 10);
      const originalExpiry = lease.getExpiry();
      const newExpiry = await lease.extend(60);

      expect(newExpiry).toBeGreaterThan(originalExpiry);
      expect(lease.getExpiry()).toBe(newExpiry);
    });

    it("should reject renew when token does not match", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const lease = await f.client().lease().acquire(f.uniqueRoute("lease"), 30);
      await expect(
        lease.testOnlyExtendWithToken(lease.testOnlyInvalidToken(), 60),
      ).rejects.toBeTruthy();
    });

    it("should release lease when token is valid", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("lease");
      const lease = await f.client().lease().acquire(route, 30);
      await lease.release();

      const reacquired = await f.client().lease().acquire(route, 30);
      expect(reacquired.getExpiry()).toBeGreaterThan(BigInt(Math.floor(Date.now() / 1000)));
    });

    it("should reject release when token does not match", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const lease = await f.client().lease().acquire(f.uniqueRoute("lease"), 30);
      await expect(
        lease.testOnlyReleaseWithToken(lease.testOnlyInvalidToken()),
      ).rejects.toBeTruthy();
    });

    it("should allow re-acquire after ttl expires", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("lease");
      const lease = await f.client().lease().acquire(route, 1);
      expect(lease.getExpiry()).toBeGreaterThan(BigInt(Math.floor(Date.now() / 1000)));

      let reacquired: { getExpiry(): bigint } | null = null;
      await waitFor(
        async () => {
          try {
            reacquired = await f.client().lease().acquire(route, 30);
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
      await f.client().lease().acquire(route, 30);

      const info = await f.client().lease().query(route);
      expect(info.isHeld).toBe(true);
      expect(
        info.owner !== undefined || info.ttlRemainingSecs !== undefined || info.token !== undefined,
      ).toBe(true);
    });

    it("should deliver subscription notifications on release", async () => {
      const f = new TestFixture(transport, authMode);
      await f.connectOrFail();

      const route = f.uniqueRoute("lease");
      const notification = new Promise<string>((resolve, reject) => {
        const timer = setTimeout(() => {
          reject(new Error("timed out waiting for lease notification"));
        }, 5000);

        void f
          .client()
          .lease()
          .subscribe(route, async (notif) => {
            clearTimeout(timer);
            resolve(notif.route);
          });
      });

      const lease = await f.client().lease().acquire(route, 30);
      await lease.release();

      await expect(notification).resolves.toBe(route);
    });
  });
});
