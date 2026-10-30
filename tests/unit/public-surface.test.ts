import * as publicApi from "../../src/index";
/// <reference types="node" />

import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { describe, expect, expectTypeOf, it } from "vite-plus/test";

import type { Client } from "../../src/client/client";
import type { ConnectWhenReadyOptions } from "../../src/index";
import type {
  BrowserClient,
  BrowserTransportType,
  BrowserWebSocketOptions,
} from "../../src/client/browser-client";
import type {
  Client as BrowserFacadeClient,
  ConnectWhenReadyOptions as BrowserConnectWhenReadyOptions,
} from "../../src/index.browser";

function collectExportNames(source: string): string[] {
  const names: string[] = [];
  const re = /export\s+(?:type\s+)?\{([\s\S]*?)\}\s+from\s+["'][^"']+["'];/g;

  for (const match of source.matchAll(re)) {
    const block = match[1];
    for (const rawPart of block.split(",")) {
      const part = rawPart.trim();
      if (!part) {
        continue;
      }

      const aliasMatch = part.match(/^(?:type\s+)?([A-Za-z0-9_]+)\s+as\s+([A-Za-z0-9_]+)$/);
      if (aliasMatch) {
        names.push(aliasMatch[2]);
        continue;
      }

      names.push(part.replace(/^type\s+/, "").trim());
    }
  }

  return names;
}

function readSource(relativePath: string): string {
  const filePath = fileURLToPath(new URL(relativePath, import.meta.url));
  return readFileSync(filePath, "utf8");
}

function publicDocumentationGaps(source: string, fileName: string): string[] {
  const gaps: string[] = [];
  const lines = source.split("\n");
  const exportBlocks = [...source.matchAll(/^export\s+(?:type\s*)?\{([^}]*)\}\s*;?$/gm)].map(
    (match) => match[1]!,
  );
  const inlineExports = [
    ...source.matchAll(
      /^export\s+declare\s+(?:type|interface|class|enum|function|const)\s+([A-Za-z_$][\w$]*)/gm,
    ),
  ].map((match) => match[1]!);
  const listedNames = exportBlocks.flatMap((block) =>
    block
      .split(",")
      .map((entry) => entry.trim().replace(/^type\s+/, ""))
      .filter(Boolean)
      .map((entry) => entry.split(/\s+as\s+/)[0]!),
  );
  if (listedNames.length === 0 && inlineExports.length === 0) {
    return [`${fileName}: public export block not found`];
  }
  const rootNames = new Set([...listedNames, ...inlineExports]);
  const reachableNames = new Set(rootNames);

  const declarationBodies = new Map<string, string>();
  for (let index = 0; index < lines.length; index += 1) {
    const declaration = lines[index]!.match(
      /^(?:export )?(?:declare )?(?:type|interface|class|enum|function|const)\s+([A-Za-z_$][\w$]*)/,
    );
    if (!declaration) continue;

    const body: string[] = [];
    let braces = 0;
    do {
      const line = lines[index]!;
      body.push(line);
      braces += (line.match(/\{/g) ?? []).length - (line.match(/\}/g) ?? []).length;
      if (braces === 0 && (line.trimEnd().endsWith(";") || line.trimEnd().endsWith("}"))) break;
      index += 1;
    } while (index < lines.length);
    declarationBodies.set(declaration[1]!, body.join("\n"));
  }

  for (const rootName of rootNames) {
    if (!declarationBodies.has(rootName)) {
      gaps.push(`${fileName}: exported declaration not found: ${rootName}`);
    }
  }

  const pending = [...reachableNames];
  while (pending.length > 0) {
    const body = declarationBodies.get(pending.pop()!);
    if (!body) continue;
    for (const reference of body.matchAll(/\b[A-Za-z_$][\w$]*\b/g)) {
      const name = reference[0];
      if (declarationBodies.has(name) && !reachableNames.has(name)) {
        reachableNames.add(name);
        pending.push(name);
      }
    }
  }
  let inJSDoc = false;
  let hasJSDoc = false;
  let publicBlockDepth = 0;
  let publicBlockIndent = 0;
  let publicBlockKind: "enum" | "other" | undefined;

  for (const [index, line] of lines.entries()) {
    const trimmed = line.trim();
    if (trimmed.startsWith("/**")) {
      inJSDoc = true;
      hasJSDoc = trimmed.endsWith("*/");
      continue;
    }
    if (inJSDoc) {
      if (trimmed.endsWith("*/")) {
        inJSDoc = false;
        hasJSDoc = true;
      }
      continue;
    }
    if (!trimmed || trimmed.startsWith("//")) {
      continue;
    }

    const declaration = trimmed.match(
      /^(?:export )?(?:declare )?(?:type|interface|class|enum|function|const)\s+([A-Za-z_$][\w$]*)/,
    );
    const topLevelDeclaration = declaration ? reachableNames.has(declaration[1]!) : false;
    const indent = line.length - line.trimStart().length;
    const member =
      publicBlockDepth > 0 &&
      indent > publicBlockIndent &&
      (/(?:readonly )?(?:[A-Za-z_$][\w$]*|\[Symbol\.[\w$]+\])(?:\?|!|\([^)]*\))?\s*(?::|\()/.test(
        trimmed,
      ) ||
        (publicBlockKind === "enum" && /^[A-Za-z_$][\w$]*\s*=/.test(trimmed)));

    if ((topLevelDeclaration || member) && !hasJSDoc) {
      gaps.push(`${fileName}:${index + 1}: ${trimmed}`);
    }
    if (topLevelDeclaration && /\{\s*$/.test(trimmed)) {
      publicBlockDepth = 1;
      publicBlockIndent = indent;
      publicBlockKind = /^(?:export )?(?:declare )?enum\s/.test(trimmed) ? "enum" : "other";
    } else if (publicBlockDepth > 0) {
      publicBlockDepth += (line.match(/\{/g) ?? []).length;
      publicBlockDepth -= (line.match(/\}/g) ?? []).length;
      if (publicBlockDepth === 0) {
        publicBlockKind = undefined;
      }
    }
    if (!trimmed.startsWith("*")) {
      hasJSDoc = false;
    }
  }

  return gaps;
}

describe("public surface", () => {
  it("documents every declaration and member emitted in the public type bundles", () => {
    const bundles = ["../../dist/node/index.node.d.mts", "../../dist/browser/index.browser.d.ts"];
    const gaps = bundles.flatMap((bundle) => publicDocumentationGaps(readSource(bundle), bundle));

    expect(gaps).toEqual([]);
  });

  it("keeps one-off automation out of a top-level scripts directory", () => {
    expect(existsSync(fileURLToPath(new URL("../../scripts", import.meta.url)))).toBe(false);
  });

  it("exposes the Wire 2 broker failure contract", () => {
    // Arrange
    const error = new publicApi.QueueError("outcome unknown", "ERROR", 4007);
    error.errorClass = 7;
    error.effect = 1;
    // Act
    const retryable = publicApi.isRetryable(error);
    // Assert
    expect(publicApi.ErrCodeScheduleBusy).toBe(7005);
    expect(error.domainCode).toBe(4007);
    expect(retryable).toBe(false);
  });

  it("exports ScheduleListPage from both the node and browser entry points", () => {
    // The browser entry point hand-maintains its own export list rather
    // than re-exporting ./index like index.node.ts does, so it's easy for
    // a type added to the node surface to silently miss the browser one —
    // confirmed via tsc that importing ScheduleListPage from
    // "@cntryl/fitz" (browser) used to fail to compile.
    const nodeExports = collectExportNames(readSource("../../src/index.ts"));
    const browserExports = collectExportNames(readSource("../../src/index.browser.ts"));
    expect(nodeExports).toContain("ScheduleListPage");
    expect(browserExports).toContain("ScheduleListPage");
  });

  it("keeps rpc worker request correlation ids private", () => {
    const source = readSource("../../src/domains/rpc/types.ts");
    expect(source).not.toContain("correlationId");
  });

  it("keeps queue item ids private and exposes subscription factories", () => {
    const source = readSource("../../src/domains/queue/types.ts");
    expect(source).not.toContain("private id: bigint;");
    expect(source).not.toContain("private token: bigint;");
    expect(source).toContain("export interface QueueItem");
    expect(source).toContain("export function createQueueItem(");
    expect(source).toContain("export interface QueueSubscription extends AsyncDisposable");
    expect(source).toContain("export function createQueueSubscription(");
  });

  it("keeps lease tokens private and exposes lease factories", () => {
    const source = readSource("../../src/domains/lease/types.ts");
    expect(source).not.toContain("private token: bigint;");
    expect(source).toContain("export interface Lease");
    expect(source).toContain("export function createLease(");
    expect(source).toContain("export interface LeaseSubscription extends AsyncDisposable");
    expect(source).toContain("export function createLeaseSubscription(");
  });

  it("exposes notice, schedule, and stream subscription factories", () => {
    const noticeSource = readSource("../../src/domains/notice/types.ts");
    expect(noticeSource).toContain("export function createNoticeSubscription(");

    const scheduleSource = readSource("../../src/domains/schedule/types.ts");
    expect(scheduleSource).toContain("export function createScheduleSubscription(");

    const streamSource = readSource("../../src/domains/stream/types.ts");
    expect(streamSource).toContain("export function createStreamSubscription(");
  });

  it("keeps browser client config browser-safe", () => {
    expectTypeOf<BrowserFacadeClient>().toEqualTypeOf<BrowserClient>();
    expectTypeOf<BrowserClient["config"]["transport"]>().toEqualTypeOf<BrowserTransportType>();
    expectTypeOf<BrowserClient["config"]["webSocket"]>().toEqualTypeOf<BrowserWebSocketOptions>();
  });

  it("exports connectWhenReady from root and browser public surfaces", () => {
    expectTypeOf<Client["connectWhenReady"]>().toEqualTypeOf<
      (options?: ConnectWhenReadyOptions) => Promise<void>
    >();
    expectTypeOf<BrowserFacadeClient["connectWhenReady"]>().toEqualTypeOf<
      (options?: BrowserConnectWhenReadyOptions) => Promise<void>
    >();
  });
});
