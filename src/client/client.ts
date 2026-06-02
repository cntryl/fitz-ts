/**
 * Main Fitz client facade.
 */

import { ConnectionState } from "../core/types";
import type { ClientConfig, TokenProvider } from "../core/types";
import { Connection, createConnection } from "./connection";
import { createTransport } from "../transport/factory";
import { ConnectionError } from "../core/errors";
import { KvClient } from "../domains/kv/client";
import { QueueClient } from "../domains/queue/client";
import { RpcClient } from "../domains/rpc/client";
import { LeaseClient } from "../domains/lease/client";
import { NoticeClient } from "../domains/notice/client";
import { StreamClient } from "../domains/stream/client";
import { ScheduleClient } from "../domains/schedule/client";

export type Client = ReturnType<typeof createClient>;

export function createClient(config: ClientConfig) {
  const observability = config.observability;
  const resolvedConfig: Required<
    Omit<ClientConfig, "tokenProvider" | "reconnect" | "asyncHandlers">
  > &
    Pick<ClientConfig, "tokenProvider" | "reconnect" | "asyncHandlers"> = {
    timeout: 30000,
    transport: "auto",
    maxFrameSize: 65535,
    authSettleDelayMs: 100,
    maxInFlightRequests: 256,
    observability: config.observability ?? {},
    reconnect: {
      enabled: false,
      maxAttempts: Infinity,
      backoffMs: 250,
      maxBackoffMs: 5000,
      ...config.reconnect,
    },
    asyncHandlers: {
      maxConcurrency: Infinity,
      timeoutMs: 30000,
      ...config.asyncHandlers,
    },
    ...config,
  };

  if (!resolvedConfig.url) {
    throw new Error("URL is required");
  }

  let connection: Connection | null = null;
  let kvClient: KvClient | null = null;
  let queueClient: QueueClient | null = null;
  let rpcClient: RpcClient | null = null;
  let leaseClient: LeaseClient | null = null;
  let noticeClient: NoticeClient | null = null;
  let streamClient: StreamClient | null = null;
  let scheduleClient: ScheduleClient | null = null;

  const resolveTokenProvider = (): TokenProvider => {
    if (resolvedConfig.tokenProvider) {
      return resolvedConfig.tokenProvider;
    }

    return () => "";
  };

  const ensureConnection = (): Connection => {
    if (!connection) {
      throw new ConnectionError("Not connected to Fitz server. Call connect() first.", {
        state: getState(),
      });
    }

    return connection;
  };

  const connect = async (options: { signal?: AbortSignal } = {}): Promise<void> => {
    if (connection?.isConnected()) {
      return;
    }

    const tokenProvider = resolveTokenProvider();

    connection = createConnection(
      () =>
        createTransport(resolvedConfig.url, resolvedConfig.transport, {
          timeout: resolvedConfig.timeout,
          maxFrameSize: resolvedConfig.maxFrameSize,
        }),
      tokenProvider,
      {
        timeout: resolvedConfig.timeout,
        authSettleDelayMs: resolvedConfig.authSettleDelayMs,
        maxInFlightRequests: resolvedConfig.maxInFlightRequests,
        reconnect: resolvedConfig.reconnect,
        observability,
        asyncHandlers: resolvedConfig.asyncHandlers,
      },
    );

    await connection.connect(options);
  };

  const close = async (): Promise<void> => {
    if (connection) {
      await connection.close();
      connection = null;
    }
    kvClient = null;
    queueClient = null;
    rpcClient = null;
    leaseClient = null;
    noticeClient = null;
    streamClient = null;
    scheduleClient = null;
  };

  const isConnected = (): boolean => {
    return connection?.isConnected() ?? false;
  };

  const kv = (): KvClient => {
    const activeConnection = ensureConnection();
    if (!kvClient) {
      kvClient = new KvClient(activeConnection);
    }
    return kvClient;
  };

  const queue = (): QueueClient => {
    const activeConnection = ensureConnection();
    if (!queueClient) {
      queueClient = new QueueClient(activeConnection);
    }
    return queueClient;
  };

  const rpc = (): RpcClient => {
    const activeConnection = ensureConnection();
    if (!rpcClient) {
      rpcClient = new RpcClient(activeConnection);
    }
    return rpcClient;
  };

  const lease = (): LeaseClient => {
    const activeConnection = ensureConnection();
    if (!leaseClient) {
      leaseClient = new LeaseClient(activeConnection);
    }
    return leaseClient;
  };

  const notice = (): NoticeClient => {
    const activeConnection = ensureConnection();
    if (!noticeClient) {
      noticeClient = new NoticeClient(activeConnection);
    }
    return noticeClient;
  };

  const stream = (): StreamClient => {
    const activeConnection = ensureConnection();
    if (!streamClient) {
      streamClient = new StreamClient(activeConnection);
    }
    return streamClient;
  };

  const schedule = (): ScheduleClient => {
    const activeConnection = ensureConnection();
    if (!scheduleClient) {
      scheduleClient = new ScheduleClient(activeConnection);
    }
    return scheduleClient;
  };

  const getUrl = (): string => {
    return ensureConnection().getUrl();
  };

  const getState = (): ConnectionState => {
    return connection?.getState() ?? ConnectionState.Disconnected;
  };

  return {
    config: resolvedConfig,
    connect,
    close,
    isConnected,
    kv,
    queue,
    rpc,
    lease,
    notice,
    stream,
    schedule,
    getUrl,
    getState,
  };
}

type ClientConstructor = {
  new (config: ClientConfig): Client;
  (config: ClientConfig): Client;
};

export const Client: ClientConstructor = function (config: ClientConfig) {
  return createClient(config);
} as unknown as ClientConstructor;
