/**
 * Main Fitz client facade.
 */

import { ClientConfig, ConnectionState, FitzObservability, TokenProvider } from "../core/types";
import { Connection } from "./connection";
import { createTransport } from "../transport/factory";
import { ConnectionError } from "../core/errors";
import { KvClient } from "../domains/kv/client";
import { QueueClient } from "../domains/queue/client";
import { RpcClient } from "../domains/rpc/client";
import { LeaseClient } from "../domains/lease/client";
import { NoticeClient } from "../domains/notice/client";
import { StreamClient } from "../domains/stream/client";
import { ScheduleClient } from "../domains/schedule/client";

export class Client {
  private readonly config: Required<
    Omit<ClientConfig, "tokenProvider" | "reconnect" | "asyncHandlers">
  > &
    Pick<ClientConfig, "tokenProvider" | "reconnect" | "asyncHandlers">;
  private readonly observability: FitzObservability | undefined;
  private connection: Connection | null = null;
  private kvClient: KvClient | null = null;
  private queueClient: QueueClient | null = null;
  private rpcClient: RpcClient | null = null;
  private leaseClient: LeaseClient | null = null;
  private noticeClient: NoticeClient | null = null;
  private streamClient: StreamClient | null = null;
  private scheduleClient: ScheduleClient | null = null;

  constructor(config: ClientConfig) {
    this.observability = config.observability;
    this.config = {
      timeout: 30000,
      transport: "auto",
      maxFrameSize: 65535,
      authSettleDelayMs: 100,
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

    if (!this.config.url) {
      throw new Error("URL is required");
    }
  }

  async connect(options: { signal?: AbortSignal } = {}): Promise<void> {
    if (this.connection?.isConnected()) {
      return;
    }

    const tokenProvider = this.resolveTokenProvider();

    this.connection = new Connection(
      () =>
        createTransport(this.config.url, this.config.transport, {
          timeout: this.config.timeout,
          maxFrameSize: this.config.maxFrameSize,
        }),
      tokenProvider,
      {
        timeout: this.config.timeout,
        authSettleDelayMs: this.config.authSettleDelayMs,
        reconnect: this.config.reconnect,
        observability: this.observability,
        asyncHandlers: this.config.asyncHandlers,
      },
    );

    await this.connection.connect(options);
  }

  async close(): Promise<void> {
    if (this.connection) {
      await this.connection.close();
      this.connection = null;
    }
    this.kvClient = null;
    this.queueClient = null;
    this.rpcClient = null;
    this.leaseClient = null;
    this.noticeClient = null;
    this.streamClient = null;
    this.scheduleClient = null;
  }

  isConnected(): boolean {
    return this.connection?.isConnected() ?? false;
  }

  kv(): KvClient {
    const connection = this.ensureConnection();
    if (!this.kvClient) {
      this.kvClient = new KvClient(connection);
    }
    return this.kvClient;
  }

  queue(): QueueClient {
    const connection = this.ensureConnection();
    if (!this.queueClient) {
      this.queueClient = new QueueClient(connection);
    }
    return this.queueClient;
  }

  rpc(): RpcClient {
    const connection = this.ensureConnection();
    if (!this.rpcClient) {
      this.rpcClient = new RpcClient(connection);
    }
    return this.rpcClient;
  }

  lease(): LeaseClient {
    const connection = this.ensureConnection();
    if (!this.leaseClient) {
      this.leaseClient = new LeaseClient(connection);
    }
    return this.leaseClient;
  }

  notice(): NoticeClient {
    const connection = this.ensureConnection();
    if (!this.noticeClient) {
      this.noticeClient = new NoticeClient(connection);
    }
    return this.noticeClient;
  }

  stream(): StreamClient {
    const connection = this.ensureConnection();
    if (!this.streamClient) {
      this.streamClient = new StreamClient(connection);
    }
    return this.streamClient;
  }

  schedule(): ScheduleClient {
    const connection = this.ensureConnection();
    if (!this.scheduleClient) {
      this.scheduleClient = new ScheduleClient(connection);
    }
    return this.scheduleClient;
  }

  getUrl(): string {
    return this.ensureConnection().getUrl();
  }

  getState(): ConnectionState {
    return this.connection?.getState() ?? ConnectionState.Disconnected;
  }

  private resolveTokenProvider(): TokenProvider {
    if (this.config.tokenProvider) {
      return this.config.tokenProvider;
    }

    return () => "";
  }

  private ensureConnection(): Connection {
    if (!this.connection) {
      throw new ConnectionError("Not connected to Fitz server. Call connect() first.", {
        state: this.getState(),
      });
    }

    return this.connection;
  }
}
