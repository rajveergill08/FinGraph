export interface DashboardNode {
  id: string;
  label: string;
  country: string | null;
  account_type: string | null;
  risk_tier: string | null;
  graph_risk_score: number;
  pagerank_score: number;
  community_id: number | null;
}

export interface DashboardEdge {
  id: string;
  source: string;
  target: string;
  amount: number;
  currency: string | null;
  occurred_at: string | null;
  channel: string | null;
  syndicate_id: string | null;
  risk_indicators: string[];
}

export interface GraphFilters {
  edge_limit: number;
  minimum_risk_score: number;
  minimum_pagerank_score: number;
  community_id: number | null;
}

export interface GraphSnapshot {
  generated_at: string;
  nodes: DashboardNode[];
  edges: DashboardEdge[];
  filters: GraphFilters;
}

export interface GraphViewNode extends DashboardNode {
  kind: "account" | "community";
  member_count: number;
}

export interface GraphView {
  nodes: GraphViewNode[];
  edges: DashboardEdge[];
  hiddenInternalTransferCount: number;
}

export interface CommunitySummary {
  id: number;
  accountCount: number;
  highRiskCount: number;
  maximumRiskScore: number;
}

export interface DashboardMetrics {
  accountCount: number;
  transferCount: number;
  highRiskCount: number;
  communityCount: number;
  flaggedTransferCount: number;
}
