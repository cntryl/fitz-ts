import { describe, expect, it } from "vite-plus/test";
import { ResultCollector, type ScenarioResult } from "../conformance/result.js";

const opts = { client: "fitz-ts", transport: "tcp", auth_mode: "anonymous" };

function passing(id: number): ScenarioResult {
  return {
    scenario_id: `CS-${String(id).padStart(3, "0")}`,
    title: "scenario",
    priority: id <= 8 || [19, 20, 24].includes(id) ? "P0" : "P1",
    ...opts,
    verdict: "pass",
    evidence: [],
    latency_ms: 0,
  };
}

describe("conformance result accounting", () => {
  it("fails the P0 gate when a required scenario is missing", () => {
    const collector = new ResultCollector();
    for (let id = 1; id <= 24; id++) if (id !== 20) collector.record(passing(id));
    collector.record({ ...passing(20), scenario_id: "TS-EXTRA" });

    const aggregate = collector.aggregate(opts);

    expect(aggregate.overall_status).toBe("fail");
    expect(aggregate.p0_pass_rate).toBe(10 / 11);
    expect(aggregate.missing_scenarios).toEqual(["CS-020"]);
  });

  it("reports partial when only a required P1 scenario is missing", () => {
    const collector = new ResultCollector();
    for (let id = 1; id <= 24; id++) if (id !== 18) collector.record(passing(id));

    const aggregate = collector.aggregate(opts);

    expect(aggregate.overall_status).toBe("partial");
    expect(aggregate.p1_pass_rate).toBe(12 / 13);
    expect(aggregate.missing_scenarios).toEqual(["CS-018"]);
  });
});
