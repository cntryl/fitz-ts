/// <reference types="node" />

import { createHmac } from "node:crypto";

const DEFAULT_PERMISSIONS = [
  "kv://**#*",
  "queue://**#*",
  "notice://**#*",
  "stream://**#*",
  "rpc://**#*",
  "lease://**#*",
  "schedule://**#*",
];

function encodeBase64Url(value: string): string {
  return Buffer.from(value, "utf8").toString("base64url");
}

function sign(input: string, secret: string): string {
  return createHmac("sha256", secret).update(input).digest("base64url");
}

function generateTestJwt(
  secret: string,
  audience: string,
  expiresAtSeconds: number,
  tenant: string,
): string {
  const now = Math.floor(Date.now() / 1000);
  const header = encodeBase64Url(
    JSON.stringify({
      alg: "HS256",
      typ: "JWT",
    }),
  );
  const payload = encodeBase64Url(
    JSON.stringify({
      iss: "",
      aud: audience,
      sub: tenant,
      tid: tenant,
      exp: expiresAtSeconds,
      iat: now,
      permissions: DEFAULT_PERMISSIONS,
    }),
  );
  const signature = sign(`${header}.${payload}`, secret);
  return `${header}.${payload}.${signature}`;
}

export function generateValidTestJwt(secret: string, audience: string, tenant: string): string {
  return generateTestJwt(secret, audience, Math.floor(Date.now() / 1000) + 3600, tenant);
}

export function generateExpiredTestJwt(secret: string, audience: string, tenant: string): string {
  return generateTestJwt(secret, audience, Math.floor(Date.now() / 1000) - 3600, tenant);
}

export function generateInvalidSignatureTestJwt(
  secret: string,
  audience: string,
  tenant: string,
): string {
  return generateTestJwt(
    `${secret}-invalid`,
    audience,
    Math.floor(Date.now() / 1000) + 3600,
    tenant,
  );
}
