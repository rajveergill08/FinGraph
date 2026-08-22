import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchGraphSnapshot } from "./api";
import type { GraphFilters, GraphSnapshot } from "./types";

const filters: GraphFilters = {
  edge_limit: 100,
  minimum_risk_score: 45,
  minimum_pagerank_score: 0.2,
  community_id: 7,
};

const snapshot: GraphSnapshot = {
  generated_at: "2026-08-22T12:00:00Z",
  nodes: [],
  edges: [],
  filters,
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("fetchGraphSnapshot", () => {
  it("routes bounded filters to the analyst API", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => snapshot,
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(fetchGraphSnapshot(filters)).resolves.toEqual(snapshot);

    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe(
      "http://localhost:8000/api/graph?edge_limit=100&minimum_risk_score=45&minimum_pagerank_score=0.2&community_id=7",
    );
    expect(options.headers).toEqual({ Accept: "application/json" });
  });

  it("reports a non-success API response", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status: 503 }));

    await expect(fetchGraphSnapshot(filters)).rejects.toThrow(
      "Graph API returned 503.",
    );
  });
});
