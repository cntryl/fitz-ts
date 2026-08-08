import { describe, expect, it, vi } from "vite-plus/test";

import { restoreMapEntriesAtomically } from "../../../src/domains/internal/restore";

describe("restoreMapEntriesAtomically", () => {
  it("rolls back successful wire restores and preserves the live map after partial failure", async () => {
    const entries = new Map([
      ["first", 1],
      ["second", 2],
      ["third", 3],
    ]);
    const rollback = vi.fn(async () => undefined);

    await expect(
      restoreMapEntriesAtomically(
        entries,
        async (key, value) => {
          if (key === "third") throw new Error("restore failed");
          return value + 10;
        },
        rollback,
      ),
    ).rejects.toThrow("restore failed");

    expect(Array.from(entries.entries())).toEqual([
      ["first", 1],
      ["second", 2],
      ["third", 3],
    ]);
    expect(rollback.mock.calls).toEqual([
      ["second", 12],
      ["first", 11],
    ]);
  });
});
