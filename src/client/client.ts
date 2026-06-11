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

export type Client = ReturnType<typeof createClient>;

type ManagedConnection = Connection & {
  waitUntilReady?: (signal?: AbortSignal, waitTimeoutMs?: number) => Promise<void>;
  shouldWaitForReconnect?: () => boolean;
};

const abortError = (): Error => {
  const error = new Error("The operation was aborted");
  error.name = "AbortError";
  return error;
};

const throwIfAborted = (signal?: AbortSignal): void => {
  if (signal?.aborted) {
    throw abortError();
  }
};

const waitForSharedPromise = async <T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> => {
  if (!signal) {
    return promise;
  }

  if (signal.aborted) {
    throw abortError();
  }

  return await new Promise<T>((resolve, reject) => {
    let settled = false;

    const cleanup = () => {
      signal.removeEventListener("abort", onAbort);
    };

    const settle = (callback: () => void) => {
      if (settled) {
        return;
      }

      settled = true;
      cleanup();
      callback();
    };

    const onAbort = () => {
      settle(() => reject(abortError()));
    };

    signal.addEventListener("abort", onAbort, { once: true });

    void promise.then(
      (value) => {
        settle(() => resolve(value));
      },
      (error) => {
        settle(() => reject(error));
      },
    );
  });
};

export function createClient(config: ClientConfig) {
  const observability = config.observability;
  const resolvedConfig: Required<
    Omit<ClientConfig, "tokenProvider" | "reconnect" | "asyncHandlers" | "retry" | "heartbeat">
  > &
    Pick<ClientConfig, "tokenProvider" | "reconnect" | "asyncHandlers" | "retry" | "heartbeat"> = {
    timeout: 30000,
    transport: "auto",
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
  let kvClient: KvClient | null = null;
  let queueClient: QueueClient | null = null;
  let rpcClient: RpcClient | null = null;
  let leaseClient: LeaseClient | null = null;
  let noticeClient: NoticeClient | null = null;
  let streamClient: StreamClient | null = null;
  let scheduleClient: ScheduleClient | null = null;
  let clientClosed = false;
  let pendingConnectPromise: Promise<void> | null = null;

  const resolveTokenProvider = (): TokenProvider => {
    if (resolvedConfig.tokenProvider) {
      return resolvedConfig.tokenProvider;
    }

    return () => "";
  };

  const ensureConnection = (): Connection => {
    if (clientClosed) {
      throw new ConnectionError("Client is closed", {
        state: ConnectionState.Closed,
      });
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
      throw new ConnectionError("Client is closed", {
        state: ConnectionState.Closed,
      });
    }

    throwIfAborted(options.signal);

    const activeConnection = createOwnedConnection();

    if (activeConnection.isConnected()) {
      return;
    }

    if (pendingConnectPromise) {
      await waitForSharedPromise(pendingConnectPromise, options.signal);
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
  };

  const close = async (): Promise<void> => {
    if (clientClosed && !connection) {
      kvClient = null;
      queueClient = null;
      rpcClient = null;
      leaseClient = null;
      noticeClient = null;
      streamClient = null;
      scheduleClient = null;
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
    kvClient = null;
    queueClient = null;
    rpcClient = null;
    leaseClient = null;
    noticeClient = null;
    streamClient = null;
    scheduleClient = null;
  };

  const isConnected = (): boolean => {
    return !clientClosed && (connection?.isConnected() ?? false);
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
