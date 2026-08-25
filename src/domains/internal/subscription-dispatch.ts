import { AsyncHandlerOverflowError } from "../../core/errors";
import type { AsyncDispatchPort } from "../base";

export interface SubscriptionHandlerRegistration<TNotification> {
  readonly handler: (notification: TNotification) => void | Promise<void>;
  readonly fail: (error: unknown) => void;
}

export function dispatchSubscriptionHandler<TNotification>(
  connection: AsyncDispatchPort,
  registration: SubscriptionHandlerRegistration<TNotification>,
  notification: TNotification,
  domain: string,
  subscription: string,
): void {
  const accepted = connection.dispatchAsyncHandler(async () => {
    await registration.handler(notification);
  });
  if (accepted === false) {
    registration.fail(
      new AsyncHandlerOverflowError(
        `Async handler queue overflowed for ${domain} subscription '${subscription}'`,
        { domain, subscription },
      ),
    );
  }
}
