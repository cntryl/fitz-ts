import { ConnectionError } from "../../core/errors";
import { waitForSharedPromise } from "./async";

/** The negotiated HELLO must arrive before the first domain command. */
export async function waitForHello(
  hello: Promise<void>,
  timeoutMs: number,
  signal?: AbortSignal,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expired = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(
      () => reject(new ConnectionError("Timed out waiting for HELLO negotiation")),
      timeoutMs,
    );
  });
  try {
    await waitForSharedPromise(Promise.race([hello, expired]), signal);
  } finally {
    clearTimeout(timer);
  }
}
