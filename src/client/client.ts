/**
 * Main Fitz client facade.
 */

import { ConnectionState } from "../core/types";
import type { ClientConfig, ClientConnectOptions, TokenProvider } from "../core/types";
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
import { throwIfAborted, waitForSharedPromise } from "./internal/async";

export type Client = ReturnType<typeof createClient>;

type ManagedConnection = Connection & {
  waitUntilReady?: (signal?: AbortSignal, waitTimeoutMs?: number) => Promise<void>;
  shouldWaitForReconnect?: () => boolean;
};

type DomainClients = {
  kv: KvClient;
  queue: QueueClient;
  rpc: RpcClient;
  lease: LeaseClient;
  notice: NoticeClient;
  stream: StreamClient;
  schedule: ScheduleClient;
};

type DomainKey = keyof DomainClients;

export function createClient(config: ClientConfig) {
  const observability = config.observability;
  const resolvedConfig: Required<
    Omit<ClientConfig, "tokenProvider" | "reconnect" | "asyncHandlers" | "retry" | "heartbeat">
  > &
    Pick<ClientConfig, "tokenProvider" | "reconnect" | "asyncHandlers" | "retry" | "heartbeat"> = {
    timeout: 30000,
    transport: "auto",
    webSocket: {},
    maxFrameSize: 65535,
    authSettleDelayMs: 100,
    maxInFlightRequests: 256,
    maxRequestQueueSize: 1024,
    observability: config.observability ?? {},
    reconnect: {
      enabled: true,
      maxAttempts: Infinity,
      backoffMs: 250,
      maxBackoffMs: 5000,
      ...config.reconnect,
    },
    retry: {
      enabled: true,
      maxAttempts: 3,
      backoffMs: 100,
      maxBackoffMs: 1000,
      ...config.retry,
    },
    heartbeat: {
      enabled: true,
      intervalMs: 10000,
      timeoutMs: 30000,
      ...config.heartbeat,
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
  const domainCache = new Map<DomainKey, DomainClients[DomainKey]>();
  let clientClosed = false;
  let pendingConnectPromise: Promise<void> | null = null;

  const domainFactories: {
    [K in DomainKey]: (connection: Connection) => DomainClients[K];
  } = {
    kv: (activeConnection) => new KvClient(activeConnection),
    queue: (activeConnection) => new QueueClient(activeConnection),
    rpc: (activeConnection) => new RpcClient(activeConnection),
    lease: (activeConnection) => new LeaseClient(activeConnection),
    notice: (activeConnection) => new NoticeClient(activeConnection),
    stream: (activeConnection) => new StreamClient(activeConnection),
    schedule: (activeConnection) => new ScheduleClient(activeConnection),
  };

  const resolveTokenProvider = (): TokenProvider => {
    if (resolvedConfig.tokenProvider) {
      return resolvedConfig.tokenProvider;
    }

    return () => "";
  };

  const clientClosedError = (): ConnectionError =>
    new ConnectionError("Client is closed", {
      state: ConnectionState.Closed,
    });

  const throwIfClientClosed = (): void => {
    if (clientClosed) {
      throw clientClosedError();
    }
  };

  const ensureConnection = (): Connection => {
    if (clientClosed) {
      throw clientClosedError();
    }

    if (!connection) {
      throw new ConnectionError("Not connected to Fitz server. Call connect() first.", {
        state: getState(),
      });
    }

    return connection;
  };

  const createOwnedConnection = (): ManagedConnection => {
    if (connection) {
      return connection as ManagedConnection;
    }

    const tokenProvider = resolveTokenProvider();

    connection = createConnection(
      () =>
        createTransport(resolvedConfig.url, resolvedConfig.transport, {
          timeout: resolvedConfig.timeout,
          maxFrameSize: resolvedConfig.maxFrameSize,
          receiveTimeout: resolvedConfig.heartbeat?.enabled === false,
          webSocket: resolvedConfig.webSocket,
        }),
      tokenProvider,
      {
        timeout: resolvedConfig.timeout,
        authSettleDelayMs: resolvedConfig.authSettleDelayMs,
        maxInFlightRequests: resolvedConfig.maxInFlightRequests,
        maxRequestQueueSize: resolvedConfig.maxRequestQueueSize,
        reconnect: resolvedConfig.reconnect,
        retry: resolvedConfig.retry,
        heartbeat: resolvedConfig.heartbeat,
        observability,
        asyncHandlers: resolvedConfig.asyncHandlers,
      },
    );

    return connection as ManagedConnection;
  };

  const connect = async (options: ClientConnectOptions = {}): Promise<void> => {
    if (clientClosed) {
      throw clientClosedError();
    }

    throwIfAborted(options.signal);

    const activeConnection = createOwnedConnection();

    if (activeConnection.isConnected()) {
      return;
    }

    if (pendingConnectPromise) {
      await waitForSharedPromise(pendingConnectPromise, options.signal);
      throwIfClientClosed();
      return;
    }

    const state = activeConnection.getState();
    const shouldWait =
      state === ConnectionState.Connecting ||
      state === ConnectionState.Connected ||
      state === ConnectionState.Authenticating ||
      state === ConnectionState.Reconnecting ||
      (state === ConnectionState.Disconnected &&
        typeof activeConnection.shouldWaitForReconnect === "function" &&
        activeConnection.shouldWaitForReconnect());

    const sharedConnectPromise = shouldWait
      ? typeof activeConnection.waitUntilReady === "function"
        ? activeConnection.waitUntilReady(undefined, resolvedConfig.timeout)
        : activeConnection.connect()
      : activeConnection.connect(options);

    const trackedConnectPromise = sharedConnectPromise.finally(() => {
      if (pendingConnectPromise === trackedConnectPromise) {
        pendingConnectPromise = null;
      }
    });
    pendingConnectPromise = trackedConnectPromise;

    await waitForSharedPromise(trackedConnectPromise, options.signal);
    throwIfClientClosed();
  };

  const close = async (): Promise<void> => {
    if (clientClosed && !connection) {
      domainCache.clear();
      return;
    }

    clientClosed = true;
    pendingConnectPromise = null;

    if (connection) {
      const activeConnection = connection;
      try {
        await activeConnection.close();
      } finally {
        if (connection === activeConnection) {
          connection = null;
        }
      }
    }
    domainCache.clear();
  };

  const isConnected = (): boolean => {
    return !clientClosed && (connection?.isConnected() ?? false);
  };

  const getDomain = <K extends DomainKey>(key: K): DomainClients[K] => {
    const activeConnection = ensureConnection();
    const cached = domainCache.get(key) as DomainClients[K] | undefined;
    if (cached) {
      return cached;
    }

    const created = domainFactories[key](activeConnection);
    domainCache.set(key, created);
    return created;
  };

  const kv = (): KvClient => {
    return getDomain("kv");
  };

  const queue = (): QueueClient => {
    return getDomain("queue");
  };

  const rpc = (): RpcClient => {
    return getDomain("rpc");
  };

  const lease = (): LeaseClient => {
    return getDomain("lease");
  };

  const notice = (): NoticeClient => {
    return getDomain("notice");
  };

  const stream = (): StreamClient => {
    return getDomain("stream");
  };

  const schedule = (): ScheduleClient => {
    return getDomain("schedule");
  };

  const getUrl = (): string => {
    return ensureConnection().getUrl();
  };

  const getState = (): ConnectionState => {
    if (clientClosed) {
      return ConnectionState.Closed;
    }

    if (!connection) {
      return ConnectionState.Disconnected;
    }

    const state = connection.getState();
    return state === ConnectionState.Closed ? ConnectionState.Disconnected : state;
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
