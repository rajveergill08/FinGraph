import type {
  DashboardMetrics,
  DashboardNode,
  GraphSnapshot,
} from "./types";

export const HIGH_RISK_THRESHOLD = 70;

export function deriveDashboardMetrics(snapshot: GraphSnapshot): DashboardMetrics {
  const communities = new Set(
    snapshot.nodes
      .map((node) => node.community_id)
      .filter((community): community is number => community !== null),
  );
  return {
    accountCount: snapshot.nodes.length,
    transferCount: snapshot.edges.length,
    highRiskCount: snapshot.nodes.filter(
      (node) => node.graph_risk_score >= HIGH_RISK_THRESHOLD,
    ).length,
    communityCount: communities.size,
    flaggedTransferCount: snapshot.edges.filter(
      (edge) => edge.risk_indicators.length > 0,
    ).length,
  };
}

export function highestRiskNode(nodes: DashboardNode[]): DashboardNode | null {
  return [...nodes].sort(
    (left, right) =>
      right.graph_risk_score - left.graph_risk_score ||
      right.pagerank_score - left.pagerank_score ||
      left.id.localeCompare(right.id),
  )[0] ?? null;
}

export function riskBand(score: number): "critical" | "watch" | "normal" {
  if (score >= HIGH_RISK_THRESHOLD) return "critical";
  if (score >= 40) return "watch";
  return "normal";
}
