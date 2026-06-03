/**
 * KV domain codec.
 */

import { BufferWriter, BufferReader } from "../../core/buffer";
import { CodecError } from "../../core/errors";
import {
  KvBeginResponse,
  KvGetResponse,
  KvScanOptions,
  KvScanResponse,
  KvStatusResponse,
  TxMode,
  DurabilityMode,
} from "./types";

export const KvCodec = {
  encodeBegin(route: string, mode: TxMode, durability: DurabilityMode): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeRoute(route);
    writer.writeU8(mode === "ReadWrite" ? 1 : 0);
    writer.writeU8(this.encodeDurability(durability));
    return writer.getBuffer();
  },

  decodeBeginResponse(payload: Uint8Array): KvBeginResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    const txId = reader.isEOF() ? undefined : reader.readU64BE();
    return { status, txId };
  },

  encodePut(txId: bigint, route: string, key: Uint8Array, value: Uint8Array): Uint8Array {
    const writer = new BufferWriter(512);
    writer.writeU64BE(txId);
    writer.writeRoute(route);
    writer.writeU32BE(key.length);
    writer.writeBytes(key);
    writer.writeU32BE(value.length);
    writer.writeBytes(value);
    return writer.getBuffer();
  },

  encodeInsert(txId: bigint, route: string, key: Uint8Array, value: Uint8Array): Uint8Array {
    return this.encodePut(txId, route, key, value);
  },

  decodeStatusResponse(payload: Uint8Array): KvStatusResponse {
    const reader = new BufferReader(payload);
    return { status: reader.readU8() };
  },

  decodePutResponse(payload: Uint8Array): KvStatusResponse {
    return this.decodeStatusResponse(payload);
  },

  encodeGet(txId: bigint, route: string, key: Uint8Array): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeU64BE(txId);
    writer.writeRoute(route);
    writer.writeU32BE(key.length);
    writer.writeBytes(key);
    return writer.getBuffer();
  },

  decodeGetResponse(payload: Uint8Array): KvGetResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();
    const found = !reader.isEOF() && reader.readU8() === 1;

    if (!found || reader.isEOF()) {
      return { status, found: false };
    }

    const valueLen = reader.readU32BE();

    return {
      status,
      found: true,
      value: reader.readBytes(valueLen),
    };
  },

  encodeDelete(txId: bigint, route: string, key: Uint8Array): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeU64BE(txId);
    writer.writeRoute(route);
    writer.writeU32BE(key.length);
    writer.writeBytes(key);
    return writer.getBuffer();
  },

  encodeDeleteRange(
    txId: bigint,
    route: string,
    startKey: Uint8Array,
    endKey: Uint8Array,
  ): Uint8Array {
    const writer = new BufferWriter(256);
    writer.writeU64BE(txId);
    writer.writeRoute(route);
    writer.writeU32BE(startKey.length);
    writer.writeBytes(startKey);
    writer.writeU32BE(endKey.length);
    writer.writeBytes(endKey);
    return writer.getBuffer();
  },

  encodeCommit(txId: bigint, route: string): Uint8Array {
    const writer = new BufferWriter(128);
    writer.writeU64BE(txId);
    writer.writeRoute(route);
    return writer.getBuffer();
  },

  encodeRollback(txId: bigint, route: string): Uint8Array {
    return this.encodeCommit(txId, route);
  },

  encodeScan(txId: bigint, route: string, options: KvScanOptions = {}): Uint8Array {
    const writer = new BufferWriter(512);
    writer.writeU64BE(txId);
    writer.writeRoute(route);

    if (options.startKey) {
      writer.writeU8(1);
      writer.writeU32BE(options.startKey.length);
      writer.writeBytes(options.startKey);
    } else {
      writer.writeU8(0);
    }

    if (options.endKey) {
      writer.writeU8(1);
      writer.writeU32BE(options.endKey.length);
      writer.writeBytes(options.endKey);
    } else {
      writer.writeU8(0);
    }

    if (typeof options.limit === "number" && options.limit > 0) {
      writer.writeU8(1);
      writer.writeU32BE(options.limit);
    } else {
      writer.writeU8(0);
    }

    writer.writeU8(options.reverse ? 1 : 0);
    return writer.getBuffer();
  },

  decodeScanResponse(payload: Uint8Array): KvScanResponse {
    const reader = new BufferReader(payload);
    const status = reader.readU8();

    if (reader.isEOF()) {
      return { status, keys: [] };
    }

    const count = reader.readU32BE();
    const keys: Uint8Array[] = [];
    for (let i = 0; i < count; i += 1) {
      const keyLen = reader.readU32BE();
      keys.push(reader.readBytes(keyLen));

      const valueLen = reader.readU32BE();
      reader.readBytes(valueLen);
    }

    const hasMore = !reader.isEOF() ? reader.readU8() : 0;
    const nextCursor = hasMore === 1 ? new Uint8Array(0) : undefined;

    return { status, keys, nextCursor };
  },

  encodeDurability(durability: DurabilityMode): number {
    switch (durability) {
      case "Buffered":
        return 0;
      case "Sync":
        return 1;
      default:
        throw new CodecError("Unknown durability mode");
    }
  },
};
