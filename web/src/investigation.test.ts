import { describe, expect, it } from "vitest";
import { buildInvestigationExport } from "./investigation";
import type {
  AlertStatusSnapshot,
  DashboardNode,
  GraphSnapshot,
  StarburstSnapshot,
} from "./types";

const account: DashboardNode = {
  id: "account:shell/001",
  label: "Shell 001",
  country: "PA",
  account_type: "business",
  risk_tier: "critical",
  graph_risk_score: 92,
  pagerank_score: 1,
  community_id: 9,
};

const graphSnapshot: GraphSnapshot = {
  generated_at: "2026-09-01T11:55:00Z",
  nodes: [
    account,
    { ...account, id: "source-1", label: "Source 1" },
    { ...account, id: "target-1", label: "Target 1" },
  ],
  edges: [
    {
      id: "incoming-usd",
      source: "source-1",
      target: account.id,
      amount: 9900,
      currency: "USD",
      occurred_at: "2026-09-01T11:59:00Z",
      channel: "wire",
      syndicate_id: "syndicate-1",
      risk_indicators: ["below_reporting_threshold"],
    },
    {
      id: "outgoing-eur",
      source: account.id,
      target: "target-1",
      amount: 7500,
      currency: "EUR",
      occurred_at: "2026-09-01T12:00:00Z",
      channel: "wire",
      syndicate_id: "syndicate-1",
      risk_indicators: [],
    },
    {
      id: "unrelated",
      source: "source-1",
      target: "target-1",
      amount: 10,
      currency: "USD",
      occurred_at: "2026-09-01T12:01:00Z",
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

const alertSnapshot: AlertStatusSnapshot = {
  generated_at: "2026-09-01T12:00:00Z",
  candidates: [
    {
      account_id: account.id,
      graph_risk_score: 92,
      risk_tier: "critical",
      country: "PA",
      pagerank_score: 1,
      community_id: 9,
      transaction_count: 2,
      counterparty_count: 2,
      latest_transfer_at: "2026-09-01T12:00:00Z",
      deliveries: [],
    },
  ],
  filters: { minimum_risk_score: 70, limit: 100 },
};

const starburstSnapshot: StarburstSnapshot = {
  generated_at: "2026-09-01T12:00:00Z",
  patterns: [
    {
      id: "starburst:account:shell/001",
      sink_account_id: account.id,
      source_account_ids: ["source-1"],
      intermediary_account_ids: ["target-1"],
      source_count: 10,
      intermediary_count: 2,
      inbound_transfer_count: 10,
      outbound_transfer_count: 2,
      latest_transfer_at: "2026-09-01T12:00:00Z",
    },
  ],
  filters: {
    lookback_hours: 24,
    minimum_source_accounts: 10,
    minimum_intermediaries: 2,
    limit: 20,
  },
};

describe("investigation evidence export", () => {
  it("builds a deterministic dossier without mixing currency totals", () => {
    const file = buildInvestigationExport({
      account,
      graphSnapshot,
      alertSnapshot,
      starburstSnapshot,
      generatedAt: "2026-09-01T12:05:06.000Z",
    });

    expect(file.filename).toBe(
      "fingraph-account-shell-001-20260901120506.json",
    );
    expect(file.mime_type).toBe("application/json");
    expect(file.report.transfer_summary).toEqual({
      connected_transfer_count: 2,
      incoming_transfer_count: 1,
      outgoing_transfer_count: 1,
      flagged_transfer_count: 1,
      latest_activity_at: "2026-09-01T12:00:00Z",
      totals_by_currency: [
        { currency: "EUR", incoming: 0, outgoing: 7500 },
        { currency: "USD", incoming: 9900, outgoing: 0 },
      ],
    });
    expect(file.report.counterparties).toEqual([
      { id: "source-1", label: "Source 1" },
      { id: "target-1", label: "Target 1" },
    ]);
    expect(file.report.automated_alert.candidate?.account_id).toBe(account.id);
    expect(file.report.matched_starburst_patterns).toHaveLength(1);
    expect(file.report.transfers.map((transfer) => transfer.id)).toEqual([
      "outgoing-eur",
      "incoming-usd",
    ]);
    expect(JSON.parse(file.contents)).toEqual(file.report);
  });

  it("records an explicit bounded scope when no alert or topology match exists", () => {
    const file = buildInvestigationExport({
      account,
      graphSnapshot,
      alertSnapshot: null,
      starburstSnapshot: null,
      generatedAt: "2026-09-01T12:05:06.000Z",
    });

    expect(file.report.data_scope).toBe("bounded_dashboard_snapshot");
    expect(file.report.automated_alert).toEqual({
      threshold: null,
      candidate: null,
    });
    expect(file.report.matched_starburst_patterns).toEqual([]);
  });
});
