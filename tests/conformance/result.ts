/**
 * Result types for the Fitz cross-language conformance harness.
 * Shape must match the contract defined in:
 *   fitz/docs/clients/cross-language-conformance-runner.md
 */

export type Verdict = "pass" | "partial" | "fail" | "not_implemented" | "unclear";

export interface ScenarioResult {
  scenario_id: string;
  title: string;
  priority: "P0" | "P1";
  client: string;
  transport: string;
  auth_mode: string;
  verdict: Verdict;
  evidence: string[];
  latency_ms: number;
  error?: string;
}

export interface AggregateResult {
  suite: string;
  version: string;
  generated_at: string;
  client: string;
  transport: string;
  auth_mode: string;
  p0_pass: number;
  p0_total: number;
  p1_pass: number;
  p1_total: number;
  p0_pass_rate: number;
  p1_pass_rate: number;
  overall_status: "pass" | "fail" | "partial";
  missing_scenarios: string[];
  scenarios: ScenarioResult[];
}

export const REQUIRED_SCENARIO_IDS = Array.from(
  { length: 24 },
  (_, index) => `CS-${String(index + 1).padStart(3, "0")}`,
);
const p0Ids = new Set(
  [1, 2, 3, 4, 5, 6, 7, 8, 19, 20, 24].map((id) => `CS-${String(id).padStart(3, "0")}`),
);

export class ResultCollector {
  private results: ScenarioResult[] = [];

  record(result: ScenarioResult): void {
    this.results.push(result);
  }

  aggregate(opts: { client: string; transport: string; auth_mode: string }): AggregateResult {
    const byId = new Map(this.results.map((result) => [result.scenario_id, result]));
    const p0 = REQUIRED_SCENARIO_IDS.filter((id) => p0Ids.has(id));
    const p1 = REQUIRED_SCENARIO_IDS.filter((id) => !p0Ids.has(id));
    const passed = (ids: string[]) => ids.filter((id) => byId.get(id)?.verdict === "pass").length;
    const p0Pass = passed(p0);
    const p1Pass = passed(p1);

    const p0Rate = p0Pass / p0.length;
    const p1Rate = p1Pass / p1.length;
    const anyP0Fail = p0Rate < 1;
    const anyP1Warn = p1Rate < 1;

    let overall_status: "pass" | "fail" | "partial";
    if (anyP0Fail) {
      overall_status = "fail";
    } else if (anyP1Warn) {
      overall_status = "partial";
    } else {
      overall_status = "pass";
    }

    return {
      suite: "fitz-cross-language-client-conformance",
      version: "1.0",
      generated_at: new Date().toISOString(),
      client: opts.client,
      transport: opts.transport,
      auth_mode: opts.auth_mode,
      p0_pass: p0Pass,
      p0_total: p0.length,
      p1_pass: p1Pass,
      p1_total: p1.length,
      p0_pass_rate: p0Rate,
      p1_pass_rate: p1Rate,
      overall_status,
      missing_scenarios: REQUIRED_SCENARIO_IDS.filter((id) => !byId.has(id)),
      scenarios: this.results,
    };
  }
}
