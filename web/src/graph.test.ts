import { describe, expect, it } from "vitest";
import {
  deriveDashboardMetrics,
  findAccountByQuery,
  formatTransferAmount,
  highestRiskNode,
  riskBand,
} from "./graph";
import type { DashboardNode, GraphSnapshot } from "./types";

function node(
  id: string,
  risk: number,
  pagerank: number,
  community: number | null,
): DashboardNode {
  return {
    id,
    label: id,
    country: "US",
    account_type: "checking",
    risk_tier: "medium",
    graph_risk_score: risk,
    pagerank_score: pagerank,
    community_id: community,
  };
}

describe("dashboard graph summaries", () => {
  const nodes = [
    node("account-a", 72, 0.5, 4),
    node("account-b", 35, 0.9, 4),
    node("account-c", 88, 0.4, 9),
  ];
  const snapshot: GraphSnapshot = {
    generated_at: "2026-08-22T12:00:00Z",
    nodes,
    edges: [
      {
        id: "transfer-a",
        source: "account-a",
        target: "account-b",
        amount: 9900,
        currency: "USD",
        occurred_at: "2026-08-22T11:59:59Z",
        channel: "wire",
        syndicate_id: "syndicate-1",
        risk_indicators: ["below_reporting_threshold"],
      },
      {
        id: "transfer-b",
        source: "account-b",
        target: "account-c",
        amount: 100,
        currency: "USD",
        occurred_at: "2026-08-22T12:00:00Z",
        channel: "ach",
        syndicate_id: null,
        risk_indicators: [],
      },
    ],
    filters: {
      edge_limit: 200,
      minimum_risk_score: 0,
      minimum_pagerank_score: 0,
      community_id: null,
    },
  };

  it("counts unique communities, high-risk nodes, and flagged transfers", () => {
    expect(deriveDashboardMetrics(snapshot)).toEqual({
      accountCount: 3,
      transferCount: 2,
      highRiskCount: 2,
      communityCount: 2,
      flaggedTransferCount: 1,
    });
  });

  it("selects the highest graph-risk account before centrality", () => {
    expect(highestRiskNode(nodes)?.id).toBe("account-c");
    expect(highestRiskNode([])).toBeNull();
  });

  it("maps explainable score thresholds to analyst risk bands", () => {
    expect(riskBand(70)).toBe("critical");
    expect(riskBand(40)).toBe("watch");
    expect(riskBand(39.9)).toBe("normal");
  });

  it("finds exact, prefix, and partial account matches without case sensitivity", () => {
    expect(findAccountByQuery(nodes, "ACCOUNT-B")?.id).toBe("account-b");
    expect(findAccountByQuery(nodes, "account-")?.id).toBe("account-c");
    expect(findAccountByQuery(nodes, "count-a")?.id).toBe("account-a");
    expect(findAccountByQuery(nodes, "missing")).toBeNull();
    expect(findAccountByQuery(nodes, "  ")).toBeNull();
  });

  it("formats transaction values with their own currency", () => {
    expect(formatTransferAmount(9900, "USD")).toBe("$9,900.00");
    expect(formatTransferAmount(1200.5, null)).toBe("1,200.5");
  });
});
