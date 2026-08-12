import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "tests/browser",
  fullyParallel: false,
  workers: process.env.CI ? undefined : 1,
  timeout: 30_000,
  use: { baseURL: "http://127.0.0.1:4173" },
  webServer: {
    command: "npx vite --host 127.0.0.1 --port 4173",
    port: 4173,
    reuseExistingServer: !process.env.CI,
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "firefox", use: { ...devices["Desktop Firefox"] } },
    { name: "webkit", use: { ...devices["Desktop Safari"] } },
  ],
});
