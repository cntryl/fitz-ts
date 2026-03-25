import { describe } from "vite-plus/test";

import type { AuthMode } from "./fixture";

export type TransportType = "tcp" | "ws";

const BOTH_TRANSPORTS: TransportType[] = ["tcp", "ws"];
const BOTH_AUTH_MODES: AuthMode[] = ["anonymous", "valid_jwt"];

export function runWithBothTransports(
  register: (ctx: { transport: TransportType; authMode: AuthMode }) => void,
): void {
  for (const authMode of BOTH_AUTH_MODES) {
    describe(authMode, () => {
      for (const transport of BOTH_TRANSPORTS) {
        describe(transport, () => {
          register({ transport, authMode });
        });
      }
    });
  }
}

export function runWithTransportsOnly(
  register: (ctx: { transport: TransportType }) => void,
): void {
  for (const transport of BOTH_TRANSPORTS) {
    describe(transport, () => {
      register({ transport });
    });
  }
}
