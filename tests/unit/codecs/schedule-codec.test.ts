/**
 * Schedule Codec unit tests
 */

import { describe, it, expect } from "vite-plus/test";
import { ScheduleCodec } from "../../../src/domains/schedule/codec";
import { createBufferReader, createBufferWriter } from "../../../src/core/buffer";
import { testData } from "../helpers/test-utils";

describe("ScheduleCodec", () => {
  describe("CREATE encoding", () => {
    it("should_encode_create_with_route_cron_payload", () => {
      // Arrange
      const route = "schedule://acme/tasks/nightly_backup/run";
      const cronExpr = "0 0 * * *"; // midnight daily
      const payload = testData('{"bucket": "s3://backups"}');

      // Act
      const encoded = ScheduleCodec.encodeCreate(route, cronExpr, "broadcast", payload);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
      expect(encoded.length).toBeGreaterThan(0);
    });

    it("should_encode_create_with_empty_payload", () => {
      // Arrange/Act
      const encoded = ScheduleCodec.encodeCreate(
        "schedule://test/jobs/job/run",
        "*/5 * * * *",
        "single",
        new Uint8Array(0),
      );

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it.each([
      ["broadcast", 0],
      ["single", 1],
    ] as const)("should_encode_%s_delivery_mode_as_%i", (mode, expected) => {
      const encoded = ScheduleCodec.encodeCreate(
        "schedule://test/jobs/job/run",
        "* * * * *",
        mode,
        testData("body"),
      );
      const reader = createBufferReader(encoded);
      reader.readString();
      reader.readString();
      expect(reader.readU8()).toBe(expected);
      expect(reader.readBytes(reader.readU32BE())).toEqual(testData("body"));
    });

    it("should_reject_unknown_delivery_mode", () => {
      expect(() =>
        ScheduleCodec.encodeCreate(
          "schedule://test/jobs/job/run",
          "* * * * *",
          "unknown" as never,
          new Uint8Array(),
        ),
      ).toThrow("Invalid schedule delivery mode");
    });
  });

  describe("CREATE decoding", () => {
    it("should_decode_create_response_with_schedule_id", () => {
      // Arrange
      const writer = createBufferWriter(64);
      writer.writeU8(1); // has_schedule_id
      writer.writeString("schedule_id_12345");
      const response = writer.getBuffer();

      // Act
      const decoded = ScheduleCodec.decodeCreateResponse(response);

      // Assert
      expect(decoded.scheduleId).toBe("schedule_id_12345");
    });

    it("should_decode_create_response_success_without_id", () => {
      // Arrange
      const response = new Uint8Array(0);

      // Act
      const decoded = ScheduleCodec.decodeCreateResponse(response);

      // Assert
      expect(decoded.scheduleId).toBeUndefined();
    });
  });

  describe("CANCEL encoding", () => {
    it("should_encode_cancel_with_route", () => {
      // Arrange
      const route = "schedule://acme/tasks/nightly_backup/run";

      // Act
      const encoded = ScheduleCodec.encodeCancel(route);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("LIST encoding", () => {
    it("should_encode_list_with_offset_and_limit", () => {
      // Arrange/Act
      const encoded = ScheduleCodec.encodeList(10n, 50n);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });

    it("should_encode_list_with_defaults", () => {
      // Arrange/Act
      const encoded = ScheduleCodec.encodeList();

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("LIST decoding", () => {
    it("should_decode_list_response_with_single_entry", () => {
      // Arrange
      const writer = createBufferWriter(128);
      writer.writeU64BE(1n); // totalCount = 1
      // Entry 1
      writer.writeU8(1); // hasEntry = 1
      writer.writeString("schedule://acme/jobs/job1/run");
      writer.writeString("0 0 * * *");
      writer.writeU8(1);
      writer.writeU32BE(testData("payload1").length);
      writer.writeBytes(testData("payload1"));
      // End marker
      writer.writeU8(0); // hasEntry = 0

      const response = writer.getBuffer();

      // Act
      const decoded = ScheduleCodec.decodeListResponse(response);

      // Assert
      expect(decoded.totalCount).toBe(1n);
      expect(decoded.entries).toHaveLength(1);
      expect(decoded.entries[0].route).toBe("schedule://acme/jobs/job1/run");
      expect(decoded.entries[0].cron).toBe("0 0 * * *");
      expect(decoded.entries[0].deliveryMode).toBe("single");
      expect(decoded.entries[0].payload).toEqual(testData("payload1"));
    });

    it("should_decode_list_response_empty", () => {
      // Arrange
      const writer = createBufferWriter(16);
      writer.writeU64BE(0n); // totalCount
      writer.writeU8(0); // hasEntry = 0 (no entries)
      const response = writer.getBuffer();

      // Act
      const decoded = ScheduleCodec.decodeListResponse(response);

      // Assert
      expect(decoded.entries).toHaveLength(0);
      expect(decoded.totalCount).toBe(0n);
    });

    it("should_reject_unknown_list_delivery_mode", () => {
      const writer = createBufferWriter(128);
      writer.writeU64BE(1n);
      writer.writeU8(1);
      writer.writeString("schedule://acme/jobs/job1/run");
      writer.writeString("0 0 * * *");
      writer.writeU8(2);
      writer.writeU32BE(0);
      writer.writeU8(0);

      expect(() => ScheduleCodec.decodeListResponse(writer.getBuffer())).toThrow(
        "Invalid schedule delivery mode byte: 2",
      );
    });
  });

  describe("SUBSCRIBE encoding", () => {
    it("should_encode_subscribe_with_pattern", () => {
      // Arrange
      const pattern = "schedule://acme/tasks/nightly_backup/run";

      // Act
      const encoded = ScheduleCodec.encodeSubscribe(pattern);

      // Assert
      expect(encoded).toBeInstanceOf(Uint8Array);
    });
  });

  describe("SUBSCRIBE decoding", () => {
    it("should_decode_subscribe_response_with_sub_id", () => {
      // Arrange
      const writer = createBufferWriter(16);
      writer.writeU8(1); // has_sub_id
      writer.writeU64BE(444n); // subId
      const response = writer.getBuffer();

      // Act
      const decoded = ScheduleCodec.decodeSubscribeResponse(response);

      // Assert
      expect(decoded.subId).toBe(444n);
    });

    it("should_reject_subscribe_response_without_sub_id", () => {
      expect(() => ScheduleCodec.decodeSubscribeResponse(new Uint8Array([0]))).toThrow(
        "missing subscription_id",
      );
    });
  });

  describe("NOTIFY decoding", () => {
    it("should_decode_schedule_fire_notification_with_sub_id", () => {
      // Arrange
      const writer = createBufferWriter(256);
      writer.writeU64BE(444n); // subId
      writer.writeU32BE(testData('{"execution_id": "exec_123"}').length);
      writer.writeBytes(testData('{"execution_id": "exec_123"}'));
      const payload = writer.getBuffer();

      // Act
      const decoded = ScheduleCodec.decodeNotification(payload);

      // Assert
      expect(decoded.subId).toBe(444n);
      expect(decoded.payload).toEqual(testData('{"execution_id": "exec_123"}'));
    });

    it("should_reject_schedule_fire_notification_without_sub_id", () => {
      const writer = createBufferWriter(256);
      writer.writeU32BE(testData('{"execution_id": "exec_456"}').length);
      writer.writeBytes(testData('{"execution_id": "exec_456"}'));

      expect(() => ScheduleCodec.decodeNotification(writer.getBuffer())).toThrow(
        "payload truncated",
      );
    });

    it("should_reject_schedule_fire_notification_with_trailing_bytes", () => {
      const writer = createBufferWriter(256);
      writer.writeU64BE(444n);
      writer.writeU32BE(0);
      writer.writeU8(0xff);

      expect(() => ScheduleCodec.decodeNotification(writer.getBuffer())).toThrow("trailing bytes");
    });
  });

  describe("Cron expression handling", () => {
    it("should_encode_various_cron_expressions", () => {
      const cronExpressions = [
        "0 0 * * *", // Daily
        "*/5 * * * *", // Every 5 minutes
        "0 */2 * * *", // Every 2 hours
        "0 0 * * 0", // Weekly
        "0 0 1 * *", // Monthly
      ];

      for (const cron of cronExpressions) {
        const encoded = ScheduleCodec.encodeCreate(
          "schedule://test/jobs/job/run",
          cron,
          "broadcast",
          new Uint8Array(0),
        );
        expect(encoded).toBeInstanceOf(Uint8Array);
      }
    });
  });
});
