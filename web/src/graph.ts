import type {
  CommunitySummary,
  DashboardMetrics,
  DashboardNode,
  GraphView,
  GraphViewNode,
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

export function findAccountByQuery(
  nodes: DashboardNode[],
  query: string,
): DashboardNode | null {
  const normalizedQuery = query.trim().toLocaleLowerCase();
  if (!normalizedQuery) return null;

  const rankedNodes = [...nodes].sort(
    (left, right) =>
      right.graph_risk_score - left.graph_risk_score ||
      right.pagerank_score - left.pagerank_score ||
      left.id.localeCompare(right.id),
  );
  return (
    rankedNodes.find(
      (node) =>
        node.id.toLocaleLowerCase() === normalizedQuery ||
        node.label.toLocaleLowerCase() === normalizedQuery,
    ) ??
    rankedNodes.find(
      (node) =>
        node.id.toLocaleLowerCase().startsWith(normalizedQuery) ||
        node.label.toLocaleLowerCase().startsWith(normalizedQuery),
    ) ??
    rankedNodes.find(
      (node) =>
        node.id.toLocaleLowerCase().includes(normalizedQuery) ||
        node.label.toLocaleLowerCase().includes(normalizedQuery),
    ) ??
    null
  );
}

export function formatTransferAmount(
  amount: number,
  currency: string | null,
): string {
  const currencyCode = currency?.trim().toUpperCase() ?? "";
  if (/^[A-Z]{3}$/.test(currencyCode)) {
    try {
      return new Intl.NumberFormat("en-US", {
        style: "currency",
        currency: currencyCode,
        maximumFractionDigits: 2,
      }).format(amount);
    } catch {
      // Fall through when the runtime does not recognize the ISO code.
    }
  }
  const formattedAmount = new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 2,
  }).format(amount);
  return currencyCode ? `${formattedAmount} ${currencyCode}` : formattedAmount;
}

export function deriveCommunitySummaries(
  nodes: DashboardNode[],
): CommunitySummary[] {
  const communities = new Map<number, DashboardNode[]>();
  for (const node of nodes) {
    if (node.community_id === null) continue;
    const members = communities.get(node.community_id) ?? [];
    members.push(node);
    communities.set(node.community_id, members);
  }
  return [...communities.entries()]
    .map(([id, members]) => ({
      id,
      accountCount: members.length,
      highRiskCount: members.filter(
        (member) => member.graph_risk_score >= HIGH_RISK_THRESHOLD,
      ).length,
      maximumRiskScore: Math.max(
        ...members.map((member) => member.graph_risk_score),
      ),
    }))
    .sort(
      (left, right) =>
        right.maximumRiskScore - left.maximumRiskScore || left.id - right.id,
    );
}

export function buildGraphView(
  snapshot: GraphSnapshot,
  collapsedCommunityIds: ReadonlySet<number>,
): GraphView {
  const collapsibleCommunities = new Map<number, DashboardNode[]>();
  for (const node of snapshot.nodes) {
    if (
      node.community_id !== null &&
      collapsedCommunityIds.has(node.community_id)
    ) {
      const members = collapsibleCommunities.get(node.community_id) ?? [];
      members.push(node);
      collapsibleCommunities.set(node.community_id, members);
    }
  }

  const endpointIds = new Map<string, string>();
  const accountNodes: GraphViewNode[] = [];
  for (const node of snapshot.nodes) {
    if (
      node.community_id !== null &&
      collapsibleCommunities.has(node.community_id)
    ) {
      endpointIds.set(node.id, `community:${node.community_id}`);
      continue;
    }
    endpointIds.set(node.id, node.id);
    accountNodes.push({ ...node, kind: "account", member_count: 1 });
  }

  const communityNodes: GraphViewNode[] = [
    ...collapsibleCommunities.entries(),
  ].map(([communityId, members]) => ({
    id: `community:${communityId}`,
    label: `Community ${communityId}`,
    country: null,
    account_type: "community",
    risk_tier: "cluster",
    graph_risk_score: Math.max(
      ...members.map((member) => member.graph_risk_score),
    ),
    pagerank_score: Math.max(
      ...members.map((member) => member.pagerank_score),
    ),
    community_id: communityId,
    kind: "community",
    member_count: members.length,
  }));

  let hiddenInternalTransferCount = 0;
  const edges = snapshot.edges.flatMap((edge) => {
    const source = endpointIds.get(edge.source);
    const target = endpointIds.get(edge.target);
    if (!source || !target) return [];
    if (source === target) {
      hiddenInternalTransferCount += 1;
      return [];
    }
    return [{ ...edge, source, target }];
  });

  return {
    nodes: [...accountNodes, ...communityNodes],
    edges,
    hiddenInternalTransferCount,
  };
}
