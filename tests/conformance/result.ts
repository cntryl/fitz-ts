/**
 * Result types for the Fitz cross-language conformance harness.
 * Shape must match the contract defined in:
 *   fitz/docs/clients/cross-language-conformance-runner.md
 */

export type Verdict =
  | "pass"
  | "partial"
  | "fail"
  | "not_implemented"
  | "unclear";

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
  p0_pass_rate: number;
  p1_pass_rate: number;
  overall_status: "pass" | "fail" | "partial";
  scenarios: ScenarioResult[];
}

export class ResultCollector {
  private results: ScenarioResult[] = [];

  record(result: ScenarioResult): void {
    this.results.push(result);
  }

  aggregate(opts: {
    client: string;
    transport: string;
    auth_mode: string;
  }): AggregateResult {
    const p0 = this.results.filter((r) => r.priority === "P0");
    const p1 = this.results.filter((r) => r.priority === "P1");
    const rate = (arr: ScenarioResult[]) =>
      arr.length === 0
        ? 1
        : arr.filter((r) => r.verdict === "pass").length / arr.length;

    const p0Rate = rate(p0);
    const p1Rate = rate(p1);
    const anyP0Fail = p0.some((r) => r.verdict !== "pass");
    const anyP1Warn = p1.some(
      (r) => r.verdict === "fail" || r.verdict === "partial",
    );

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
      p0_pass_rate: p0Rate,
      p1_pass_rate: p1Rate,
      overall_status,
      scenarios: this.results,
    };
  }
}
