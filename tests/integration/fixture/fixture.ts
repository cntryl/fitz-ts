import { expect, onTestFinished } from "vite-plus/test";

import { createClient, type Client } from "../../../src/client/client";
import type { ClientConfig, ConnectWhenReadyOptions } from "../../../src/core/types";
import {
  generateExpiredTestJwt,
  generateInvalidSignatureTestJwt,
  generateValidTestJwt,
} from "./jwt";
import type { TransportType } from "./transport";

export type AuthMode = "anonymous" | "valid_jwt" | "expired_jwt" | "invalid_signature";

export const EnvBrokerTCPAddr = "FITZ_BROKER_TCP_ADDR";
export const EnvBrokerWSAddr = "FITZ_BROKER_WS_ADDR";
export const EnvBrokerAuthTCPAddr = "FITZ_BROKER_AUTH_TCP_ADDR";
export const EnvBrokerAuthWSAddr = "FITZ_BROKER_AUTH_WS_ADDR";
export const EnvBrokerAnonTCPAddr = "FITZ_BROKER_ANON_TCP_ADDR";
export const EnvBrokerAnonWSAddr = "FITZ_BROKER_ANON_WS_ADDR";
export const EnvBrokerJWTHMACSecret = "FITZ_BROKER_JWT_HMAC_SECRET";
export const EnvBrokerJWTAudience = "FITZ_BROKER_JWT_AUDIENCE";
export const EnvBrokerJWTTenant = "FITZ_BROKER_JWT_TENANT";

const DEFAULT_SECRET = "dev-test-secret";
const DEFAULT_AUDIENCE = "fitz";
const DEFAULT_TENANT = "dev";

function env(name: string): string | undefined {
  const value = process.env[name];
  return value && value.length > 0 ? value : undefined;
}

function brokerAddrFromEnv(
  transport: TransportType,
  tcpEnv: string,
  wsEnv: string,
  tcpDefault: string,
  wsDefault: string,
): string {
  if (transport === "tcp") {
    return env(tcpEnv) ?? env(EnvBrokerTCPAddr) ?? tcpDefault;
  }

  return env(wsEnv) ?? env(EnvBrokerWSAddr) ?? wsDefault;
}

export function brokerAddrFor(transport: TransportType, authMode: AuthMode): string {
  switch (authMode) {
    case "anonymous":
      return brokerAddrFromEnv(
        transport,
        EnvBrokerAnonTCPAddr,
        EnvBrokerAnonWSAddr,
        "localhost:4191",
        "ws://localhost:4190/ws",
      );
    case "valid_jwt":
    case "expired_jwt":
    case "invalid_signature":
      return brokerAddrFromEnv(
        transport,
        EnvBrokerAuthTCPAddr,
        EnvBrokerAuthWSAddr,
        "localhost:4091",
        "ws://localhost:4090/ws",
      );
    default:
      throw new Error("unsupported auth mode");
  }
}

function tokenProviderForMode(authMode: AuthMode): () => string | Promise<string> {
  const secret = env(EnvBrokerJWTHMACSecret) ?? DEFAULT_SECRET;
  const audience = env(EnvBrokerJWTAudience) ?? DEFAULT_AUDIENCE;
  const tenant = env(EnvBrokerJWTTenant) ?? DEFAULT_TENANT;

  switch (authMode) {
    case "anonymous":
      return () => "";
    case "valid_jwt":
      return () => generateValidTestJwt(secret, audience, tenant);
    case "expired_jwt":
      return () => generateExpiredTestJwt(secret, audience, tenant);
    case "invalid_signature":
      return () => generateInvalidSignatureTestJwt(secret, audience, tenant);
    default:
      throw new Error("unsupported auth mode");
  }
}

export class TestFixture {
  private cleanupFns: Array<() => void | Promise<void>> = [];
  private clientInstance: Client | null = null;
  private brokerAddr: string;
  private tokenProviderOverride: (() => string | Promise<string>) | null = null;

  constructor(
    public readonly transport: TransportType,
    public authMode: AuthMode = "anonymous",
  ) {
    this.brokerAddr = brokerAddrFor(transport, authMode);
    onTestFinished(async () => {
      await this.cleanup();
    });
  }

  setAuthMode(mode: AuthMode): void {
    this.authMode = mode;
    this.brokerAddr = brokerAddrFor(this.transport, mode);
  }

  setBrokerAddr(addr: string): void {
    this.brokerAddr = addr;
  }

  setTokenProvider(provider: () => string | Promise<string>): void {
    this.tokenProviderOverride = provider;
  }

  addCleanup(fn: () => void | Promise<void>): void {
    this.cleanupFns.push(fn);
  }

  async connect(
    overrides: Partial<ClientConfig> = {},
    options: { signal?: AbortSignal } = {},
  ): Promise<void> {
    this.clientInstance = createClient({
      url: this.brokerAddr,
      transport: this.transport,
      tokenProvider: this.tokenProviderOverride ?? tokenProviderForMode(this.authMode),
      timeout: 30000,
      ...overrides,
    });
    await this.clientInstance.connect(options);
  }

  async connectWhenReady(
    overrides: Partial<ClientConfig> = {},
    options: ConnectWhenReadyOptions = {},
  ): Promise<void> {
    this.clientInstance = createClient({
      url: this.brokerAddr,
      transport: this.transport,
      tokenProvider: this.tokenProviderOverride ?? tokenProviderForMode(this.authMode),
      timeout: 30000,
      ...overrides,
    });
    await this.clientInstance.connectWhenReady(options);
  }

  async connectOrFail(
    overrides: Partial<ClientConfig> = {},
    options: { signal?: AbortSignal } = {},
  ): Promise<void> {
    await expect(this.connect(overrides, options)).resolves.toBeUndefined();
  }

  async connectWithAuthOrFail(
    mode: AuthMode,
    overrides: Partial<ClientConfig> = {},
    options: { signal?: AbortSignal } = {},
  ): Promise<void> {
    this.setAuthMode(mode);
    await this.connectOrFail(overrides, options);
  }

  client(): Client {
    if (!this.clientInstance) {
      throw new Error("client not connected; call connect() first");
    }
    return this.clientInstance;
  }

  uniqueRealm(): string {
    return `test-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
  }

  uniqueArea(): string {
    return `area-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
  }

  uniqueResource(): string {
    return `resource-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
  }

  uniqueRoute(scheme: string): string {
    const realm = this.uniqueRealm();
    const area = this.uniqueArea();
    const resource = this.uniqueResource();
    if (scheme === "schedule") {
      return `${scheme}://${realm}/${area}/${resource}/run`;
    }
    return `${scheme}://${realm}/${area}/${resource}`;
  }

  async cleanup(): Promise<void> {
    while (this.cleanupFns.length > 0) {
      const fn = this.cleanupFns.pop();
      if (fn) {
        await fn();
      }
    }

    if (this.clientInstance) {
      try {
        await this.clientInstance.close();
      } finally {
        this.clientInstance = null;
      }
    }
  }
}
