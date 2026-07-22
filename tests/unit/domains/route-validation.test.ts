import { describe, expect, it } from "vite-plus/test";

import {
  isConcreteRouteShape,
  isRouteShape,
  isSelectorRouteShape,
} from "../../../src/domains/_routes";

describe("opaque route validation", () => {
  it("forwards broker-owned route and selector grammar", () => {
    const opaqueValues = [
      "",
      "not-a-uri",
      "wrong://realm//resource",
      "schedule://*/**/unusual*selector",
    ];

    for (const route of opaqueValues) {
      expect(isRouteShape(route, "queue", 3)).toBe(true);
      expect(isConcreteRouteShape(route, "rpc")).toBe(true);
      expect(isSelectorRouteShape(route, "schedule", 4)).toBe(true);
    }
  });

  it("enforces the 65,535-byte wire boundary", () => {
    expect(isRouteShape("r".repeat(65_535), "ignored", 0)).toBe(true);
    expect(isRouteShape("r".repeat(65_536), "ignored", 0)).toBe(false);
  });

  it("measures UTF-8 bytes rather than JavaScript code units", () => {
    expect(isRouteShape("é".repeat(32_767), "ignored", 0)).toBe(true);
    expect(isRouteShape("é".repeat(32_768), "ignored", 0)).toBe(false);
  });
});
