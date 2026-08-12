import { expect, test } from "@playwright/test";

test("publishes and receives a notice through the real WebSocket broker", async ({ page }) => {
  await page.goto("/tests/browser/index.html");
  const result = await page.evaluate(async () => {
    type BrowserGlobal = typeof globalThis & {
      fitzCreateClient: (config: object) => {
        connect(): Promise<void>;
        close(): Promise<void>;
        notice: {
          subscribe(
            route: string,
            handler: (message: { body: Uint8Array }) => void,
          ): Promise<{ unsubscribe(): Promise<void> }>;
          publish(route: string, options: { body: Uint8Array }): Promise<void>;
        };
      };
    };
    const createClient = (globalThis as BrowserGlobal).fitzCreateClient;
    const client = createClient({
      url: "ws://localhost:4290/ws",
      transport: "ws",
      tokenProvider: () => "",
    });
    await client.connect();
    const route = `notice://browser/${Date.now()}-${crypto.randomUUID()}/event`;
    let resolve!: (value: string) => void;
    const received = new Promise<string>((done) => (resolve = done));
    const subscription = await client.notice.subscribe(route, (message) => {
      resolve(new TextDecoder().decode(message.body));
    });
    try {
      await client.notice.publish(route, { body: new TextEncoder().encode("browser-ok") });
      return await Promise.race([
        received,
        new Promise<string>((_, reject) =>
          setTimeout(() => reject(new Error("notice timed out")), 5_000),
        ),
      ]);
    } finally {
      await subscription.unsubscribe();
      await client.close();
    }
  });
  expect(result).toBe("browser-ok");
});
