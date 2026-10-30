/** Fitz wire message identifiers. The header is the fixed wire format 2 header. */

// Control messages (0-99)
export const MSG_CONNECT = 1;
export const MSG_HELLO = 2;
export const MSG_CONNECT_REJECTED = 3;
export const MSG_GOAWAY = 4;

// No optional capability bits are defined by wire version 2.
export const CAP_CORRELATION = 0;
export const CAP_SESSION_METADATA = 0;
export const CAP_KV_SCAN_EXCLUSIVE = 0;
export const CAP_RPC_CANCELLATION = 0;

// KV Domain (100-199)
export const MSG_KV_BEGIN = 100;
export const MSG_KV_COMMIT = 101;
export const MSG_KV_ROLLBACK = 102;
export const MSG_KV_GET = 103;
export const MSG_KV_PUT = 104;
export const MSG_KV_INSERT = 105;
export const MSG_KV_DELETE = 106;
export const MSG_KV_DELETE_RANGE = 107;
export const MSG_KV_SCAN = 108;
export const MSG_KV_SUBSCRIBE = 109;
export const MSG_KV_UNSUBSCRIBE = 110;
export const MSG_KV_NOTIFY = 111;

// Queue Domain (200-299); 201 is retired and must not be reused.
export const MSG_QUEUE_ENQUEUE = 200;
export const MSG_QUEUE_RESERVE = 202;
export const MSG_QUEUE_EXTEND = 203;
export const MSG_QUEUE_COMPLETE = 204;
export const MSG_QUEUE_SUBSCRIBE = 207;
export const MSG_QUEUE_UNSUBSCRIBE = 208;
export const MSG_QUEUE_NOTIFY = 209;

// RPC Domain (300-399)
export const MSG_RPC_SUBSCRIBE_WORKER = 300;
export const MSG_RPC_UNSUBSCRIBE_WORKER = 301;
export const MSG_RPC_REQUEST = 302;
export const MSG_RPC_RESPONSE = 303;
export const MSG_RPC_CANCELLATION = 304;
export const MSG_RPC_LIFECYCLE = 305;

// Lease Domain (400-499)
export const MSG_LEASE_ACQUIRE = 400;
export const MSG_LEASE_RENEW = 401;
export const MSG_LEASE_RELEASE = 402;
export const MSG_LEASE_QUERY = 403;
export const MSG_LEASE_SUBSCRIBE = 407;
export const MSG_LEASE_UNSUBSCRIBE = 408;
export const MSG_LEASE_NOTIFY = 409;
export const MSG_LEASE_LIST = 410;

// Notice Domain (500-599)
export const MSG_NOTICE_PUBLISH = 500;
export const MSG_NOTICE_SUBSCRIBE = 501;
export const MSG_NOTICE_UNSUBSCRIBE = 502;
export const MSG_NOTICE_UNSUBSCRIBE_ALL = 503;
export const MSG_NOTICE_NOTIFY = 504;

// Stream Domain (600-699)
export const MSG_STREAM_BEGIN = 600;
export const MSG_STREAM_APPEND = 601;
export const MSG_STREAM_COMMIT = 602;
export const MSG_STREAM_ROLLBACK = 603;
export const MSG_STREAM_READ = 604;
export const MSG_STREAM_LAST = 605;
export const MSG_STREAM_GET_METADATA = 606;
export const MSG_STREAM_SUBSCRIBE = 607;
export const MSG_STREAM_UNSUBSCRIBE = 608;
export const MSG_STREAM_NOTIFY = 609;

// Schedule Domain (700-799). LIST (702) is the cursor form; 707 is retired.
export const MSG_SCHEDULE_CREATE = 700;
export const MSG_SCHEDULE_CANCEL = 701;
export const MSG_SCHEDULE_LIST = 702;
export const MSG_SCHEDULE_LIST_PAGE = MSG_SCHEDULE_LIST;
export const MSG_SCHEDULE_SUBSCRIBE = 703;
export const MSG_SCHEDULE_UNSUBSCRIBE = 704;
export const MSG_SCHEDULE_NOTIFY = 705;
export const MSG_SCHEDULE_CREATE_BATCH = 706;

export function routeDomain(msgType: number): string {
  if (msgType >= 100 && msgType <= 199) return "kv";
  if (msgType >= 200 && msgType <= 299) return "queue";
  if (msgType >= 300 && msgType <= 399) return "rpc";
  if (msgType >= 400 && msgType <= 499) return "lease";
  if (msgType >= 500 && msgType <= 599) return "notice";
  if (msgType >= 600 && msgType <= 699) return "stream";
  if (msgType >= 700 && msgType <= 799) return "schedule";
  return "unknown";
}

export const NOTIFICATION_TYPES = new Set([
  MSG_KV_NOTIFY,
  MSG_QUEUE_NOTIFY,
  MSG_LEASE_NOTIFY,
  MSG_NOTICE_NOTIFY,
  MSG_STREAM_NOTIFY,
  MSG_SCHEDULE_NOTIFY,
]);
