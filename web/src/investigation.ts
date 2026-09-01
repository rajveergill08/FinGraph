import type {
  AlertCandidateStatus,
  AlertStatusSnapshot,
  DashboardEdge,
  DashboardNode,
  GraphFilters,
  GraphSnapshot,
  StarburstPattern,
  StarburstSnapshot,
} from "./types";

export interface CurrencyFlowTotal {
  currency: string;
  incoming: number;
  outgoing: number;
}

export interface InvestigationReport {
  schema_version: 1;
  generated_at: string;
  data_scope: "bounded_dashboard_snapshot";
  graph_snapshot_generated_at: string;
  graph_filters: GraphFilters;
  account: DashboardNode;
  counterparties: Array<{ id: string; label: string }>;
  transfer_summary: {
    connected_transfer_count: number;
    incoming_transfer_count: number;
    outgoing_transfer_count: number;
    flagged_transfer_count: number;
    latest_activity_at: string | null;
    totals_by_currency: CurrencyFlowTotal[];
  };
  automated_alert: {
    threshold: number | null;
    candidate: AlertCandidateStatus | null;
  };
  matched_starburst_patterns: StarburstPattern[];
  transfers: DashboardEdge[];
}

export interface InvestigationExport {
  filename: string;
  mime_type: "application/json";
  contents: string;
  report: InvestigationReport;
}

function matchedStarbursts(
  accountId: string,
  snapshot: StarburstSnapshot | null,
): StarburstPattern[] {
  return (snapshot?.patterns ?? []).filter(
    (pattern) =>
      pattern.sink_account_id === accountId ||
      pattern.source_account_ids.includes(accountId) ||
      pattern.intermediary_account_ids.includes(accountId),
  );
}

function exportFilename(accountId: string, generatedAt: string): string {
  const safeAccountId = accountId.replace(/[^a-zA-Z0-9_-]/g, "-");
  const timestamp = generatedAt.replace(/\D/g, "").slice(0, 14);
  return `fingraph-${safeAccountId}-${timestamp || "snapshot"}.json`;
}

export function buildInvestigationExport({
  account,
  graphSnapshot,
  alertSnapshot,
  starburstSnapshot,
  generatedAt = new Date().toISOString(),
}: {
  account: DashboardNode;
  graphSnapshot: GraphSnapshot;
  alertSnapshot: AlertStatusSnapshot | null;
  starburstSnapshot: StarburstSnapshot | null;
  generatedAt?: string;
}): InvestigationExport {
  const transfers = graphSnapshot.edges
    .filter((edge) => edge.source === account.id || edge.target === account.id)
    .sort(
      (left, right) =>
        Date.parse(right.occurred_at ?? "") -
        Date.parse(left.occurred_at ?? ""),
    );
  const nodesById = new Map(
    graphSnapshot.nodes.map((node) => [node.id, node] as const),
  );
  const counterpartyIds = new Set<string>();
  const currencyTotals = new Map<string, CurrencyFlowTotal>();
  let incomingTransferCount = 0;
  let outgoingTransferCount = 0;

  for (const transfer of transfers) {
    const outgoing = transfer.source === account.id;
    const counterpartyId = outgoing ? transfer.target : transfer.source;
    counterpartyIds.add(counterpartyId);
    if (outgoing) outgoingTransferCount += 1;
    else incomingTransferCount += 1;

    const currency = transfer.currency ?? "UNSPECIFIED";
    const total = currencyTotals.get(currency) ?? {
      currency,
      incoming: 0,
      outgoing: 0,
    };
    if (outgoing) total.outgoing += transfer.amount;
    else total.incoming += transfer.amount;
    currencyTotals.set(currency, total);
  }

  const report: InvestigationReport = {
    schema_version: 1,
    generated_at: generatedAt,
    data_scope: "bounded_dashboard_snapshot",
    graph_snapshot_generated_at: graphSnapshot.generated_at,
    graph_filters: graphSnapshot.filters,
    account,
    counterparties: [...counterpartyIds]
      .sort()
      .map((id) => ({ id, label: nodesById.get(id)?.label ?? id })),
    transfer_summary: {
      connected_transfer_count: transfers.length,
      incoming_transfer_count: incomingTransferCount,
      outgoing_transfer_count: outgoingTransferCount,
      flagged_transfer_count: transfers.filter(
        (transfer) => transfer.risk_indicators.length > 0,
      ).length,
      latest_activity_at: transfers[0]?.occurred_at ?? null,
      totals_by_currency: [...currencyTotals.values()].sort((left, right) =>
        left.currency.localeCompare(right.currency),
      ),
    },
    automated_alert: {
      threshold: alertSnapshot?.filters.minimum_risk_score ?? null,
      candidate:
        alertSnapshot?.candidates.find(
          (candidate) => candidate.account_id === account.id,
        ) ?? null,
    },
    matched_starburst_patterns: matchedStarbursts(
      account.id,
      starburstSnapshot,
    ),
    transfers,
  };

  return {
    filename: exportFilename(account.id, generatedAt),
    mime_type: "application/json",
    contents: `${JSON.stringify(report, null, 2)}\n`,
    report,
  };
}

export function downloadInvestigationExport(file: InvestigationExport): void {
  const objectUrl = URL.createObjectURL(
    new Blob([file.contents], { type: file.mime_type }),
  );
  const link = document.createElement("a");
  link.href = objectUrl;
  link.download = file.filename;
  link.style.display = "none";
  document.body.append(link);
  try {
    link.click();
  } finally {
    link.remove();
    URL.revokeObjectURL(objectUrl);
  }
}
