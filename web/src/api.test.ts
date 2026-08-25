import { afterEach, describe, expect, it, vi } from "vitest";
import { fetchGraphSnapshot, fetchStarburstPatterns } from "./api";
import type {
  GraphFilters,
  GraphSnapshot,
  StarburstSnapshot,
} from "./types";

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

const starburstSnapshot: StarburstSnapshot = {
  generated_at: "2026-08-25T12:00:00Z",
  patterns: [
    {
      id: "starburst:account-shell-001",
      sink_account_id: "account-shell-001",
      source_account_ids: ["account-source-001", "account-source-002"],
      intermediary_account_ids: [
        "account-intermediary-001",
        "account-intermediary-002",
      ],
      source_count: 50,
      intermediary_count: 5,
      inbound_transfer_count: 50,
      outbound_transfer_count: 5,
      latest_transfer_at: "2026-08-25T11:59:59Z",
    },
  ],
  filters: {
    lookback_hours: 24,
    minimum_source_accounts: 10,
    minimum_intermediaries: 2,
    limit: 20,
  },
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

describe("fetchStarburstPatterns", () => {
  it("loads topology alerts from the analyst API", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => starburstSnapshot,
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(fetchStarburstPatterns()).resolves.toEqual(starburstSnapshot);

    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe("http://localhost:8000/api/patterns/starbursts");
    expect(options.headers).toEqual({ Accept: "application/json" });
  });

  it("reports a non-success pattern response", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status: 503 }));

    await expect(fetchStarburstPatterns()).rejects.toThrow(
      "Starburst API returned 503.",
    );
  });
});
