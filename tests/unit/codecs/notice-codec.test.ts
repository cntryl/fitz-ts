/**
 * Notice Codec unit tests
 */

import { describe, it, expect } from "vite-plus/test";
import { NoticeCodec } from "../../../src/domains/notice/codec";
import { createBufferWriter } from "../../../src/core/buffer";
import { testData } from "../helpers/test-utils";

describe("NoticeCodec", () => {
  describe("PUBLISH encoding", () => {
    it("should_encode_publish_with_route_and_body", () => {
      // Arrange
      const route = "notice://acme/alerts/system";
      const body = testData('{"level": "warning", "msg": "high cpu"}');

      // Act
      const encoded = NoticeCodec.encodePublish(route, body);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_publish_with_empty_body", () => {
      // Arrange/Act
      const encoded = NoticeCodec.encodePublish("notice://test/events", new Uint8Array(0));

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("SUBSCRIBE encoding", () => {
    it("should_encode_subscribe_with_pattern", () => {
      // Arrange
      const pattern = "notice://acme/alerts/*";

      // Act
      const encoded = NoticeCodec.encodeSubscribe(pattern);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it("should_encode_subscribe_with_exact_route", () => {
      // Arrange/Act
      const encoded = NoticeCodec.encodeSubscribe("notice://acme/alerts/critical");

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("UNSUBSCRIBE encoding", () => {
    it("should_encode_unsubscribe_with_sub_id", () => {
      // Arrange
      const subId = 42n;

      // Act
      const encoded = NoticeCodec.encodeUnsubscribe(subId);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("NOTIFY decoding", () => {
    it("should_decode_notification_with_route_and_body", () => {
      // Arrange
      const writer = createBufferWriter(256);
      writer.writeU64BE(333n); // subId
      writer.writeString("notice://acme/alerts/cpu");
      writer.writeU32BE(testData('{"cpu": 95.5}').length);
      writer.writeBytes(testData('{"cpu": 95.5}'));
      const payload = writer.getBuffer();

      // Act
      const decoded = NoticeCodec.decodeNotification(payload);

      // Assert
      expect(decoded.subId).toBe(333n);
      expect(decoded.route).toBe("notice://acme/alerts/cpu");
      expect(decoded.body).toEqual(testData('{"cpu": 95.5}'));
    });

    it("should_decode_notification_with_empty_body", () => {
      // Arrange
      const writer = createBufferWriter(64);
      writer.writeU64BE(333n);
      writer.writeString("notice://test/event");
      writer.writeU32BE(0);
      writer.writeBytes(new Uint8Array(0));
      const payload = writer.getBuffer();

      // Act
      const decoded = NoticeCodec.decodeNotification(payload);

      // Assert
      expect(decoded.subId).toBe(333n);
      expect(decoded.body).toEqual(new Uint8Array(0));
    });

    it("should_reject_notification_with_trailing_bytes", () => {
      const writer = createBufferWriter(64);
      writer.writeU64BE(333n);
      writer.writeString("notice://test/event");
      writer.writeU32BE(0);
      writer.writeU8(0xff);

      expect(() => NoticeCodec.decodeNotification(writer.getBuffer())).toThrow("trailing bytes");
    });
  });
});
