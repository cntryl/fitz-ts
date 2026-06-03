import { createDeferred, Deferred } from "./types";

export type Disposable = {
  dispose(): void;
};

export type AsyncDisposable = {
  dispose(): Promise<void>;
};

export type Subscription = {
  readonly name?: string;
  readonly closed: boolean;
  unsubscribe(): Promise<void>;
};

export interface SubscriptionGroup {
  readonly name: string;
  readonly size: number;
  readonly closed: boolean;
  add<T extends Subscription | Disposable | AsyncDisposable | (() => void | Promise<void>)>(
    resource: T,
  ): T;
  unsubscribe(): Promise<void>;
}

export interface Scope {
  readonly name: string;
  readonly signal: AbortSignal;
  readonly disposed: boolean;
  add<T extends Subscription | Disposable | AsyncDisposable | (() => void | Promise<void>)>(
    resource: T,
  ): T;
  createSubscriptionGroup(groupName: string): SubscriptionGroup;
  dispose(): Promise<void>;
}

export function createScope(name: string, parent?: Scope) {
  const controller = new AbortController();
  const signal = controller.signal;
  let disposed = false;
  let disposing = false;
  let completion: Deferred<void> | null = null;
  const resources: Array<
    Disposable | AsyncDisposable | Subscription | (() => void | Promise<void>)
  > = [];

  const ensureCompletion = (): Deferred<void> => {
    if (!completion) {
      completion = createDeferred<void>();
    }
    return completion;
  };

  const add = <
    T extends Subscription | Disposable | AsyncDisposable | (() => void | Promise<void>),
  >(
    resource: T,
  ): T => {
    if (disposed) {
      throw new Error(`Cannot add resource to disposed scope ${name}`);
    }
    resources.push(resource);
    return resource;
  };

  const disposeResource = async (
    resource: Disposable | AsyncDisposable | Subscription | (() => void | Promise<void>),
  ): Promise<void> => {
    if (typeof resource === "function") {
      await resource();
      return;
    }

    if (typeof (resource as Subscription).unsubscribe === "function") {
      await (resource as Subscription).unsubscribe();
      return;
    }

    if (typeof (resource as AsyncDisposable).dispose === "function") {
      await (resource as AsyncDisposable).dispose();
      return;
    }

    if (typeof (resource as Disposable).dispose === "function") {
      (resource as Disposable).dispose();
      return;
    }

    throw new Error("Unsupported resource type");
  };

  const dispose = async (): Promise<void> => {
    if (disposed) {
      return ensureCompletion().promise;
    }
    if (disposing) {
      return ensureCompletion().promise;
    }

    disposing = true;
    controller.abort();
    const completionDeferred = ensureCompletion();
    const snapshot = resources.slice().reverse();
    resources.length = 0;

    const errors: unknown[] = [];
    for (const resource of snapshot) {
      try {
        await disposeResource(resource);
      } catch (error) {
        errors.push(error);
      }
    }

    disposed = true;
    disposing = false;

    if (errors.length > 0) {
      completionDeferred.reject(errors[0]);
      return;
    }

    completionDeferred.resolve();
  };

  const createSubscriptionGroup = (groupName: string) => {
    const group = createSubscriptionGroupInternal(groupName);
    add(group);
    return group;
  };

  if (parent) {
    parent.add({ dispose });
  }

  return {
    get name() {
      return name;
    },
    get signal() {
      return signal;
    },
    get disposed() {
      return disposed;
    },
    add,
    createSubscriptionGroup,
    dispose,
  };
}

function createSubscriptionGroupInternal(name: string) {
  let closed = false;
  const subscriptions: Array<
    Subscription | Disposable | AsyncDisposable | (() => void | Promise<void>)
  > = [];

  const add = <
    T extends Subscription | Disposable | AsyncDisposable | (() => void | Promise<void>),
  >(
    subscription: T,
  ): T => {
    if (closed) {
      throw new Error(`Cannot add subscription to closed group ${name}`);
    }

    subscriptions.push(subscription);
    return subscription;
  };

  const unsubscribe = async (): Promise<void> => {
    if (closed) {
      return;
    }

    closed = true;
    const snapshot = subscriptions.slice().reverse();
    subscriptions.length = 0;
    const errors: unknown[] = [];

    for (const subscription of snapshot) {
      try {
        if (typeof subscription === "function") {
          await subscription();
          continue;
        }

        if (typeof (subscription as Subscription).unsubscribe === "function") {
          await (subscription as Subscription).unsubscribe();
          continue;
        }

        if (typeof (subscription as AsyncDisposable).dispose === "function") {
          await (subscription as AsyncDisposable).dispose();
          continue;
        }

        if (typeof (subscription as Disposable).dispose === "function") {
          (subscription as Disposable).dispose();
          continue;
        }
      } catch (error) {
        errors.push(error);
      }
    }

    if (errors.length > 0) {
      throw errors[0];
    }
  };

  return {
    name,
    get size() {
      return subscriptions.length;
    },
    get closed() {
      return closed;
    },
    add,
    unsubscribe,
  };
}
