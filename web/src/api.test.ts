import { afterEach, describe, expect, it, vi } from "vitest";
import {
  fetchAlertStatus,
  fetchGraphSnapshot,
  fetchStarburstPatterns,
  freezeAccounts,
} from "./api";
import type {
  AlertStatusSnapshot,
  FreezeCase,
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

const alertStatusSnapshot: AlertStatusSnapshot = {
  generated_at: "2026-08-31T12:00:00Z",
  candidates: [
    {
      account_id: "account-shell-001",
      graph_risk_score: 92.5,
      risk_tier: "critical",
      country: "KY",
      pagerank_score: 0.91,
      community_id: 7,
      transaction_count: 55,
      counterparty_count: 50,
      latest_transfer_at: "2026-08-31T11:59:00Z",
      deliveries: [
        {
          channel: "slack",
          graph_risk_score: 91,
          delivered_at: "2026-08-31T11:58:00Z",
        },
      ],
    },
  ],
  filters: {
    minimum_risk_score: 70,
    limit: 100,
  },
};

const freezeCase: FreezeCase = {
  case_id: "containment-case-001",
  status: "frozen",
  reason: "Confirmed starburst network containment.",
  pattern_id: "starburst:account-shell-001",
  frozen_at: "2026-09-02T12:00:00Z",
  account_ids: ["account-source-001", "account-shell-001"],
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

describe("fetchAlertStatus", () => {
  it("loads automated alert delivery status from the analyst API", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => alertStatusSnapshot,
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(fetchAlertStatus()).resolves.toEqual(alertStatusSnapshot);

    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe("http://localhost:8000/api/alerts");
    expect(options.headers).toEqual({ Accept: "application/json" });
  });

  it("reports a non-success alert response", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status: 503 }));

    await expect(fetchAlertStatus()).rejects.toThrow(
      "Alert API returned 503.",
    );
  });
});

describe("freezeAccounts", () => {
  it("posts a bounded containment request to the analyst API", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => freezeCase,
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      freezeAccounts(
        ["account-source-001", "account-shell-001"],
        "Confirmed starburst network containment.",
        "starburst:account-shell-001",
      ),
    ).resolves.toEqual(freezeCase);

    const [url, options] = fetchMock.mock.calls[0];
    expect(url).toBe("http://localhost:8000/api/actions/freeze");
    expect(options).toEqual({
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        account_ids: ["account-source-001", "account-shell-001"],
        reason: "Confirmed starburst network containment.",
        pattern_id: "starburst:account-shell-001",
      }),
    });
  });

  it("reports a failed containment response", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue({ ok: false, status: 503 }));

    await expect(
      freezeAccounts(["account-a"], "Confirmed analyst containment."),
    ).rejects.toThrow("Containment API returned 503.");
  });
});
