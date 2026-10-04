import { ConnectionError } from "../../core/errors";
import { waitForSharedPromise } from "./async";

/** A capability advertisement must arrive before the first domain command. */
export async function waitForServerHello(
  hello: Promise<void>,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expired = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(
      () => reject(new ConnectionError("Timed out waiting for SERVER_HELLO negotiation")),
      timeoutMs,
    );
  });
  try {
    await waitForSharedPromise(Promise.race([hello, expired]), signal);
  } finally {
    clearTimeout(timer);
  }
}
