/**
 * Main Fitz client facade.
 */

import { ConnectionState } from "../core/types";
import type {
  AsyncHandlerOptions,
  ClientConfig,
  ClientConnectOptions,
  ConnectWhenReadyOptions,
  HeartbeatOptions,
  ReconnectOptions,
  RetryOptions,
  TokenProvider,
  TransportType,
} from "../core/types";
import { createConnection, type Connection } from "./connection";
import type { Transport, TransportOptions } from "../transport/types";
import { ConnectionError, TimeoutError, TransportError } from "../core/errors";
import { createKvClient, type KvClient } from "../domains/kv/client";
import { createQueueClient, type QueueClient } from "../domains/queue/client";
import { createRpcClient, type RpcClient } from "../domains/rpc/client";
import { createLeaseClient, type LeaseClient } from "../domains/lease/client";
import { createNoticeClient, type NoticeClient } from "../domains/notice/client";
import { createStreamClient, type StreamClient } from "../domains/stream/client";
import { createScheduleClient, type ScheduleClient } from "../domains/schedule/client";
import {
  isAbortError,
  sleepWithAbort,
  throwIfAborted,
  waitForSharedPromise,
} from "./internal/async";
import { jitteredBackoffMs } from "./internal/backoff";

const DEFAULT_CONNECT_WHEN_READY_BACKOFF_MS = 250;
const DEFAULT_CONNECT_WHEN_READY_MAX_BACKOFF_MS = 2000;

function closedConnectionError(error: unknown): boolean {
  return error instanceof ConnectionError && error.getContext()?.state === ConnectionState.Closed;
}

function startupReadinessError(error: unknown): boolean {
  return (
    error instanceof TransportError ||
    error instanceof ConnectionError ||
    error instanceof TimeoutError
  );
}

function timeoutWaitingForReady(timeoutMs: number): TimeoutError {
  return new TimeoutError("Timed out waiting for Fitz to become ready", { timeoutMs });
}

function resolveWaitOption(value: number | undefined, fallback: number): number {
  if (value === undefined || !Number.isFinite(value)) {
    return fallback;
  }

  return Math.max(0, value);
}

function resolveBackoffOption(value: number | undefined, fallback: number): number {
  return Math.max(1, resolveWaitOption(value, fallback));
}

function createConnectWhenReadyDeadline(timeoutMs: number, callerSignal?: AbortSignal) {
  const controller = new AbortController();
  const deadlineMs = Date.now() + timeoutMs;
  let timedOut = false;

  const onCallerAbort = (): void => {
    controller.abort();
  };

  const timeoutId = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, timeoutMs);

  if (callerSignal?.aborted) {
    controller.abort();
  } else {
    callerSignal?.addEventListener("abort", onCallerAbort, { once: true });
  }

  return {
    signal: controller.signal,
    hasExpired: (): boolean => timedOut || Date.now() >= deadlineMs,
    remainingMs: (): number => Math.max(0, deadlineMs - Date.now()),
    dispose: (): void => {
      clearTimeout(timeoutId);
      callerSignal?.removeEventListener("abort", onCallerAbort);
    },
  };
}

type DefaultedClientConfigKeys =
  | "asyncHandlers"
  | "heartbeat"
  | "reconnect"
  | "retry"
  | "tokenProvider";

type DefinedConfigShape<TConfig extends ClientConfig> = {
  [K in Exclude<keyof TConfig, DefaultedClientConfigKeys>]-?: Exclude<TConfig[K], undefined>;
};

export type ResolvedClientConfig<TConfig extends ClientConfig> = DefinedConfigShape<TConfig> & {
  asyncHandlers: AsyncHandlerOptions;
  heartbeat: HeartbeatOptions;
  reconnect: ReconnectOptions;
  retry: RetryOptions;
  tokenProvider?: TConfig["tokenProvider"];
};

export type Client<TConfig extends ClientConfig = ClientConfig> = {
  config: ResolvedClientConfig<TConfig>;
  connect: (options?: ClientConnectOptions) => Promise<void>;
  connectWhenReady: (options?: ConnectWhenReadyOptions) => Promise<void>;
  close: () => Promise<void>;
  isConnected: () => boolean;
  kv: () => KvClient;
  queue: () => QueueClient;
  rpc: () => RpcClient;
  lease: () => LeaseClient;
  notice: () => NoticeClient;
  stream: () => StreamClient;
  schedule: () => ScheduleClient;
  getUrl: () => string;
  getState: () => ConnectionState;
};

export type ClientTransportFactory = (
  url: string,
  transportType: TransportType,
  options: TransportOptions,
) => Transport;

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

export function createClientWithTransport<TConfig extends ClientConfig>(
  config: TConfig,
  transportFactory: ClientTransportFactory,
): Client<TConfig> {
  const observability = config.observability;
  const resolvedConfig = {
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
  } as unknown as ResolvedClientConfig<TConfig>;

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
    kv: (activeConnection) => createKvClient(activeConnection),
    queue: (activeConnection) => createQueueClient(activeConnection),
    rpc: (activeConnection) => createRpcClient(activeConnection),
    lease: (activeConnection) => createLeaseClient(activeConnection),
    notice: (activeConnection) => createNoticeClient(activeConnection),
    stream: (activeConnection) => createStreamClient(activeConnection),
    schedule: (activeConnection) => createScheduleClient(activeConnection),
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
        transportFactory(resolvedConfig.url, resolvedConfig.transport, {
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

  const connectWhenReady = async (options: ConnectWhenReadyOptions = {}): Promise<void> => {
    if (clientClosed) {
      throw clientClosedError();
    }

    const timeoutMs = resolveWaitOption(options.timeoutMs, resolvedConfig.timeout);
    const maxBackoffMs = resolveBackoffOption(
      options.maxBackoffMs,
      DEFAULT_CONNECT_WHEN_READY_MAX_BACKOFF_MS,
    );
    let backoffMs = resolveBackoffOption(options.backoffMs, DEFAULT_CONNECT_WHEN_READY_BACKOFF_MS);
    const deadline = createConnectWhenReadyDeadline(timeoutMs, options.signal);

    try {
      while (true) {
        throwIfClientClosed();
        throwIfAborted(options.signal);

        try {
          await connect({ signal: deadline.signal });
          return;
        } catch (error) {
          if (isAbortError(error)) {
            if (!options.signal?.aborted && deadline.hasExpired()) {
              throw timeoutWaitingForReady(timeoutMs);
            }

            throw error;
          }

          if (clientClosed || closedConnectionError(error)) {
            throw error;
          }

          if (!startupReadinessError(error)) {
            throw error;
          }

          if (deadline.hasExpired()) {
            throw timeoutWaitingForReady(timeoutMs);
          }

          const delayMs = Math.min(
            jitteredBackoffMs(backoffMs, maxBackoffMs),
            deadline.remainingMs(),
          );
          try {
            await sleepWithAbort(delayMs, deadline.signal);
          } catch (sleepError) {
            if (isAbortError(sleepError) && !options.signal?.aborted && deadline.hasExpired()) {
              throw timeoutWaitingForReady(timeoutMs);
            }

            throw sleepError;
          }
          backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
        }
      }
    } finally {
      deadline.dispose();
    }
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
    connectWhenReady,
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
  } satisfies Client<TConfig>;
}
