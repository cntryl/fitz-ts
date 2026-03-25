/**
 * KV transaction wrapper.
 */

import { Connection } from "../../client/connection";
import { KvCodec } from "./codec";
import { KvGetResult, KvScanOptions, KvStatus } from "./types";
import {
  MSG_KV_PUT,
  MSG_KV_INSERT,
  MSG_KV_GET,
  MSG_KV_DELETE,
  MSG_KV_DELETE_RANGE,
  MSG_KV_COMMIT,
  MSG_KV_ROLLBACK,
  MSG_KV_SCAN,
} from "../../frame/types";
import { KvError } from "../../core/errors";
import { SliceIterator, AsyncIterableIterator } from "../../core/iterator";

export class KvTransaction {
  private closed = false;
  private readonly unsubscribeDisconnect: () => void;

  constructor(
    private readonly connection: Connection,
    private readonly route: string,
    private readonly txId: bigint,
  ) {
    this.unsubscribeDisconnect = this.connection.onDisconnect(() => {
      this.closed = true;
    });
  }

  async put(key: Uint8Array, value: Uint8Array): Promise<void> {
    this.ensureOpen();
    const payload = KvCodec.encodePut(this.txId, this.route, key, value);
    const response = await this.connection.request(MSG_KV_PUT, payload);
    this.checkStatus(KvCodec.decodeStatusResponse(response).status, "PUT");
  }

  async insert(key: Uint8Array, value: Uint8Array): Promise<void> {
    this.ensureOpen();
    const payload = KvCodec.encodeInsert(this.txId, this.route, key, value);
    const response = await this.connection.request(MSG_KV_INSERT, payload);
    this.checkStatus(KvCodec.decodeStatusResponse(response).status, "INSERT");
  }

  async get(key: Uint8Array): Promise<KvGetResult> {
    this.ensureOpen();
    const payload = KvCodec.encodeGet(this.txId, this.route, key);
    const response = await this.connection.request(MSG_KV_GET, payload);
    const decoded = KvCodec.decodeGetResponse(response);
    this.checkStatus(decoded.status, "GET");
    if (!decoded.found || !decoded.value) {
      return { type: "not-found" };
    }
    return { type: "found", value: decoded.value };
  }

  async delete(key: Uint8Array): Promise<void> {
    this.ensureOpen();
    const payload = KvCodec.encodeDelete(this.txId, this.route, key);
    const response = await this.connection.request(MSG_KV_DELETE, payload);
    this.checkStatus(KvCodec.decodeStatusResponse(response).status, "DELETE");
  }

  async deleteRange(startKey: Uint8Array, endKey: Uint8Array): Promise<void> {
    this.ensureOpen();
    const payload = KvCodec.encodeDeleteRange(
      this.txId,
      this.route,
      startKey,
      endKey,
    );
    const response = await this.connection.request(
      MSG_KV_DELETE_RANGE,
      payload,
    );
    this.checkStatus(
      KvCodec.decodeStatusResponse(response).status,
      "DELETE_RANGE",
    );
  }

  async scan(options: KvScanOptions = {}): Promise<AsyncIterable<Uint8Array>> {
    this.ensureOpen();
    const payload = KvCodec.encodeScan(this.txId, this.route, options);
    const response = await this.connection.request(MSG_KV_SCAN, payload);
    const decoded = KvCodec.decodeScanResponse(response);
    this.checkStatus(decoded.status, "SCAN");
    return new AsyncIterableIterator(new SliceIterator(decoded.keys));
  }

  async commit(): Promise<void> {
    this.ensureOpen();
    this.closed = true;
    this.unsubscribeDisconnect();
    const payload = KvCodec.encodeCommit(this.txId, this.route);
    const response = await this.connection.request(MSG_KV_COMMIT, payload);
    this.checkStatus(KvCodec.decodeStatusResponse(response).status, "COMMIT");
  }

  async rollback(): Promise<void> {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.unsubscribeDisconnect();
    const payload = KvCodec.encodeRollback(this.txId, this.route);
    try {
      const response = await this.connection.request(MSG_KV_ROLLBACK, payload);
      this.checkStatus(
        KvCodec.decodeStatusResponse(response).status,
        "ROLLBACK",
      );
    } catch {
      // Best-effort cleanup.
    }
  }

  getTxId(): bigint {
    return this.txId;
  }

  isOpen(): boolean {
    return !this.closed;
  }

  private ensureOpen(): void {
    if (this.closed) {
      throw new KvError("Transaction already closed", "TX_CLOSED");
    }
  }

  private checkStatus(status: number, operation: string): void {
    if (status === KvStatus.Ok) {
      return;
    }

    const statusNames: Record<number, string> = {
      [KvStatus.TransactionAborted]: "TransactionAborted",
      [KvStatus.LeaseExpired]: "LeaseExpired",
      [KvStatus.ConflictingWrite]: "ConflictingWrite",
      [KvStatus.KeyNotFound]: "KeyNotFound",
      [KvStatus.OperationNotAllowed]: "OperationNotAllowed",
    };

    throw new KvError(
      `${operation} failed: ${statusNames[status] ?? `Unknown(${status})`}`,
      operation,
      status,
    );
  }
}
