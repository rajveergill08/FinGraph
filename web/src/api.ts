import type {
  GraphFilters,
  GraphSnapshot,
  StarburstSnapshot,
} from "./types";

const apiBaseUrl = (
  import.meta.env.VITE_API_BASE_URL || "http://localhost:8000"
).replace(/\/$/, "");

function graphQuery(filters: GraphFilters): string {
  const query = new URLSearchParams({
    edge_limit: String(filters.edge_limit),
    minimum_risk_score: String(filters.minimum_risk_score),
    minimum_pagerank_score: String(filters.minimum_pagerank_score),
  });
  if (filters.community_id !== null) {
    query.set("community_id", String(filters.community_id));
  }
  return query.toString();
}

export async function fetchGraphSnapshot(
  filters: GraphFilters,
  signal?: AbortSignal,
): Promise<GraphSnapshot> {
  const response = await fetch(`${apiBaseUrl}/api/graph?${graphQuery(filters)}`, {
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    throw new Error(`Graph API returned ${response.status}.`);
  }
  return (await response.json()) as GraphSnapshot;
}

export async function fetchStarburstPatterns(
  signal?: AbortSignal,
): Promise<StarburstSnapshot> {
  const response = await fetch(`${apiBaseUrl}/api/patterns/starbursts`, {
    headers: { Accept: "application/json" },
    signal,
  });
  if (!response.ok) {
    throw new Error(`Starburst API returned ${response.status}.`);
  }
  return (await response.json()) as StarburstSnapshot;
}
