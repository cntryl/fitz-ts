import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

const fixtureUrl = new URL("../../docs/clients/spec/stream-read-conformance.json", import.meta.url);
const vendorUrl = new URL(
  "../../docs/clients/spec/stream-read-conformance.vendor.json",
  import.meta.url,
);
const fixtureBytes = await readFile(fixtureUrl);
const vendor = JSON.parse(await readFile(vendorUrl, "utf8"));
const fixture = JSON.parse(fixtureBytes.toString("utf8"));
const checksum = createHash("sha256").update(fixtureBytes).digest("hex");

if (checksum !== vendor.sha256) throw new Error("vendored Stream fixture checksum mismatch");
if (fixture.fixture !== "fitz.stream-read-conformance" || fixture.version !== 1)
  throw new Error("unsupported Stream fixture identity or version");
if (fixture.selectors.length !== 10) throw new Error("Stream fixture must contain ten selectors");

console.log(`Stream fixture ${fixture.fixture}@${fixture.version} verified at ${vendor.commit}`);

if (process.argv.includes("--upstream")) {
  const upstreamUrl = `https://raw.githubusercontent.com/cntryl/fitz/main/${vendor.path}`;
  const response = await fetch(upstreamUrl);
  if (!response.ok) throw new Error(`failed to fetch upstream fixture: ${response.status}`);
  const upstream = Buffer.from(await response.arrayBuffer());
  const upstreamChecksum = createHash("sha256").update(upstream).digest("hex");
  if (upstreamChecksum !== checksum) {
    throw new Error(
      `upstream Stream fixture drifted: vendored=${checksum} upstream=${upstreamChecksum}`,
    );
  }
  console.log("Upstream main Stream fixture matches the vendored bytes");
}
