import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  type FormEvent,
} from "react";
import { fetchGraphSnapshot } from "./api";
import {
  deriveDashboardMetrics,
  findAccountByQuery,
  formatTransferAmount,
  highestRiskNode,
  riskBand,
} from "./graph";
import NetworkGraph from "./NetworkGraph";
import type {
  DashboardEdge,
  DashboardNode,
  GraphFilters,
  GraphSnapshot,
} from "./types";

const DEFAULT_FILTERS: GraphFilters = {
  edge_limit: 200,
  minimum_risk_score: 0,
  minimum_pagerank_score: 0,
  community_id: null,
};

type LoadStatus = "loading" | "ready" | "error";

function metricValue(value: number) {
  return value.toLocaleString();
}

function formatSnapshotTime(value: string | undefined): string {
  if (!value) return "Awaiting snapshot";
  return new Intl.DateTimeFormat("en-IN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function formatTransactionTime(value: string | null): string {
  if (!value) return "Time unavailable";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.valueOf())) return "Time unavailable";
  return new Intl.DateTimeFormat("en-IN", {
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
  }).format(parsed);
}

function selectedTransferCounterparty(
  edge: DashboardEdge,
  node: DashboardNode,
  nodes: DashboardNode[],
): { direction: "Incoming" | "Outgoing"; account: DashboardNode | null } {
  const outgoing = edge.source === node.id;
  const counterpartyId = outgoing ? edge.target : edge.source;
  return {
    direction: outgoing ? "Outgoing" : "Incoming",
    account: nodes.find((candidate) => candidate.id === counterpartyId) ?? null,
  };
}

function TransferDetail({ edge, onClose }: { edge: DashboardEdge; onClose: () => void }) {
  return (
    <section className="selected-transfer" aria-label="Selected transfer details">
      <header>
        <span className="section-label">Selected transfer</span>
        <button type="button" onClick={onClose} aria-label="Close transfer details">×</button>
      </header>
      <strong>{formatTransferAmount(edge.amount, edge.currency)}</strong>
      <p className="transfer-route"><span>{edge.source}</span><b aria-label="transferred to">→</b><span>{edge.target}</span></p>
      <dl className="transfer-grid">
        <div><dt>Occurred</dt><dd>{formatTransactionTime(edge.occurred_at)}</dd></div>
        <div><dt>Channel</dt><dd>{edge.channel ?? "Unknown"}</dd></div>
        <div><dt>Syndicate</dt><dd>{edge.syndicate_id ?? "Not linked"}</dd></div>
        <div><dt>Transaction ID</dt><dd title={edge.id}>{edge.id}</dd></div>
      </dl>
      <div className="transfer-signals">
        {edge.risk_indicators.length > 0 ? (
          edge.risk_indicators.map((indicator) => (
            <span key={indicator}>{indicator.replaceAll("_", " ")}</span>
          ))
        ) : (
          <small>No explicit risk indicators</small>
        )}
      </div>
    </section>
  );
}

function AccountDetails({
  node,
  nodes,
  edges,
  selectedEdgeId,
  onSelectEdge,
  onClearEdge,
}: {
  node: DashboardNode | null;
  nodes: DashboardNode[];
  edges: DashboardEdge[];
  selectedEdgeId: string | null;
  onSelectEdge: (edgeId: string) => void;
  onClearEdge: () => void;
}) {
  if (!node) {
    return (
      <div className="detail-empty">
        <span className="detail-empty-mark" aria-hidden="true">◎</span>
        <h3>Select an account</h3>
        <p>Choose a node to inspect its graph risk, centrality, and transfer trail.</p>
      </div>
    );
  }

  const band = riskBand(node.graph_risk_score);
  const sortedEdges = [...edges].sort(
    (left, right) =>
      Date.parse(right.occurred_at ?? "") - Date.parse(left.occurred_at ?? ""),
  );
  const selectedEdge = sortedEdges.find((edge) => edge.id === selectedEdgeId) ?? null;

  return (
    <div className="account-detail">
      <div className="detail-heading">
        <div>
          <span className={`risk-label risk-${band}`}>{band} risk</span>
          <h3>{node.label}</h3>
        </div>
        <strong>{node.graph_risk_score.toFixed(1)}</strong>
      </div>

      <dl className="detail-grid">
        <div><dt>PageRank</dt><dd>{node.pagerank_score.toFixed(3)}</dd></div>
        <div><dt>Community</dt><dd>{node.community_id ?? "Unassigned"}</dd></div>
        <div><dt>Connections</dt><dd>{edges.length}</dd></div>
        <div><dt>Risk tier</dt><dd>{node.risk_tier ?? "Unknown"}</dd></div>
        <div><dt>Account type</dt><dd>{node.account_type ?? "Unknown"}</dd></div>
        <div><dt>Country</dt><dd>{node.country ?? "Unknown"}</dd></div>
      </dl>

      {selectedEdge ? (
        <TransferDetail edge={selectedEdge} onClose={onClearEdge} />
      ) : (
        <p className="transfer-prompt">Select a transfer below or choose an arrow in the graph to inspect the money trail.</p>
      )}

      <section className="transfer-history" aria-label={`Transfers connected to ${node.label}`}>
        <header>
          <span className="section-label">Connected transfers</span>
          <small>{edges.length} in snapshot</small>
        </header>
        {sortedEdges.length > 0 ? (
          <ul>
            {sortedEdges.slice(0, 8).map((edge) => {
              const counterparty = selectedTransferCounterparty(edge, node, nodes);
              return (
                <li key={edge.id}>
                  <button
                    type="button"
                    className={edge.id === selectedEdgeId ? "transfer-row selected" : "transfer-row"}
                    aria-pressed={edge.id === selectedEdgeId}
                    onClick={() => onSelectEdge(edge.id)}
                  >
                    <span className={`direction direction-${counterparty.direction.toLowerCase()}`} aria-hidden="true">
                      {counterparty.direction === "Outgoing" ? "→" : "←"}
                    </span>
                    <span className="transfer-summary">
                      <strong>{formatTransferAmount(edge.amount, edge.currency)}</strong>
                      <small>{counterparty.direction} · {counterparty.account?.label ?? (counterparty.direction === "Outgoing" ? edge.target : edge.source)}</small>
                    </span>
                    <time dateTime={edge.occurred_at ?? undefined}>{formatTransactionTime(edge.occurred_at)}</time>
                    {edge.risk_indicators.length > 0 && <i aria-label="Risk indicators present" />}
                  </button>
                </li>
              );
            })}
          </ul>
        ) : (
          <p>No connected transfers are present in this snapshot.</p>
        )}
      </section>
    </div>
  );
}

export default function App() {
  const [snapshot, setSnapshot] = useState<GraphSnapshot | null>(null);
  const [draftFilters, setDraftFilters] = useState(DEFAULT_FILTERS);
  const [appliedFilters, setAppliedFilters] = useState(DEFAULT_FILTERS);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const [accountQuery, setAccountQuery] = useState("");
  const [searchMessage, setSearchMessage] = useState("");
  const [status, setStatus] = useState<LoadStatus>("loading");
  const [error, setError] = useState("");
  const [refreshVersion, setRefreshVersion] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setStatus("loading");
    setError("");
    fetchGraphSnapshot(appliedFilters, controller.signal)
      .then((nextSnapshot) => {
        setSnapshot(nextSnapshot);
        setSelectedNodeId((current) => {
          if (current && nextSnapshot.nodes.some((node) => node.id === current)) return current;
          return highestRiskNode(nextSnapshot.nodes)?.id ?? null;
        });
        setSelectedEdgeId((current) =>
          current && nextSnapshot.edges.some((edge) => edge.id === current) ? current : null,
        );
        setStatus("ready");
      })
      .catch((reason: unknown) => {
        if (reason instanceof DOMException && reason.name === "AbortError") return;
        setError(reason instanceof Error ? reason.message : "Unable to load the graph snapshot.");
        setStatus("error");
      });
    return () => controller.abort();
  }, [appliedFilters, refreshVersion]);

  const metrics = useMemo(
    () => snapshot ? deriveDashboardMetrics(snapshot) : null,
    [snapshot],
  );
  const selectedNode = snapshot?.nodes.find((node) => node.id === selectedNodeId) ?? null;
  const selectedEdges = snapshot?.edges.filter(
    (edge) => edge.source === selectedNodeId || edge.target === selectedNodeId,
  ) ?? [];

  const selectNode = useCallback((nodeId: string) => {
    setSelectedNodeId(nodeId);
    setSelectedEdgeId(null);
  }, []);

  const selectEdge = useCallback((edgeId: string) => {
    const edge = snapshot?.edges.find((candidate) => candidate.id === edgeId);
    if (!edge) return;
    setSelectedEdgeId(edgeId);
    setSelectedNodeId((current) =>
      current === edge.source || current === edge.target ? current : edge.source,
    );
  }, [snapshot]);

  function applyFilters(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSelectedEdgeId(null);
    setAppliedFilters({ ...draftFilters });
  }

  function findAccount(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const match = findAccountByQuery(snapshot?.nodes ?? [], accountQuery);
    if (!match) {
      setSearchMessage(accountQuery.trim() ? "No account in this snapshot" : "Enter an account ID");
      return;
    }
    selectNode(match.id);
    setAccountQuery(match.label);
    setSearchMessage(`Focused ${match.label}`);
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <a className="brand" href="#workspace" aria-label="FinGraph dashboard home">
          <span className="brand-mark" aria-hidden="true"><i /><i /><i /></span>
          <span><strong>FinGraph</strong><small>Fraud network intelligence</small></span>
        </a>
        <div className="pipeline-status" aria-live="polite">
          <span className={`status-dot status-${status}`} />
          <span><strong>{status === "ready" ? "Pipeline live" : status === "loading" ? "Syncing graph" : "Pipeline unavailable"}</strong><small>Neo4j snapshot · {formatSnapshotTime(snapshot?.generated_at)}</small></span>
        </div>
      </header>

      <main id="workspace">
        <section className="workspace-heading">
          <div>
            <span className="eyebrow">Live investigation workspace</span>
            <h1>Follow the money, not the rows.</h1>
            <p>Trace high-risk accounts, central intermediaries, and syndicate transfers as one connected graph.</p>
          </div>
          <button className="refresh-button" type="button" onClick={() => setRefreshVersion((value) => value + 1)} disabled={status === "loading"}>
            <span aria-hidden="true">↻</span> Refresh snapshot
          </button>
        </section>

        <section className="metric-strip" aria-label="Current graph summary">
          <article><span>Accounts in view</span><strong>{metricValue(metrics?.accountCount ?? 0)}</strong><small>Unique graph nodes</small></article>
          <article><span>Transfers traced</span><strong>{metricValue(metrics?.transferCount ?? 0)}</strong><small>Bounded latest edges</small></article>
          <article className="critical-metric"><span>High-risk accounts</span><strong>{metricValue(metrics?.highRiskCount ?? 0)}</strong><small>Risk score ≥ 70</small></article>
          <article><span>Detected communities</span><strong>{metricValue(metrics?.communityCount ?? 0)}</strong><small>Louvain groups</small></article>
          <article><span>Flagged transfers</span><strong>{metricValue(metrics?.flaggedTransferCount ?? 0)}</strong><small>With risk indicators</small></article>
        </section>

        <form className="filter-bar" onSubmit={applyFilters}>
          <div className="filter-title"><span aria-hidden="true">⌁</span><strong>Graph filters</strong></div>
          <label>
            Latest transfers
            <select value={draftFilters.edge_limit} onChange={(event) => setDraftFilters((filters) => ({ ...filters, edge_limit: Number(event.target.value) }))}>
              <option value={50}>50 edges</option><option value={100}>100 edges</option><option value={200}>200 edges</option><option value={500}>500 edges</option>
            </select>
          </label>
          <label>
            Minimum risk
            <span className="range-control"><input type="range" min="0" max="100" step="5" value={draftFilters.minimum_risk_score} onChange={(event) => setDraftFilters((filters) => ({ ...filters, minimum_risk_score: Number(event.target.value) }))} /><output>{draftFilters.minimum_risk_score}</output></span>
          </label>
          <label>
            Community ID
            <input type="number" min="0" placeholder="All" value={draftFilters.community_id ?? ""} onChange={(event) => setDraftFilters((filters) => ({ ...filters, community_id: event.target.value === "" ? null : Number(event.target.value) }))} />
          </label>
          <button type="submit">Apply filters</button>
        </form>

        {status === "error" && (
          <div className="error-banner" role="alert"><strong>Live graph unavailable.</strong> {error} Check the dashboard API, then refresh this snapshot.</div>
        )}

        <section className="investigation-grid">
          <article className="graph-panel">
            <header className="panel-header graph-panel-header">
              <div><span className="section-label">Transaction network</span><h2>Account flow map</h2></div>
              <div className="graph-header-tools">
                <form className="account-search" role="search" onSubmit={findAccount}>
                  <label htmlFor="account-search">Find account</label>
                  <span><input id="account-search" list="account-options" placeholder="Account ID" value={accountQuery} onChange={(event) => { setAccountQuery(event.target.value); setSearchMessage(""); }} /><button type="submit">Find</button></span>
                  <datalist id="account-options">{snapshot?.nodes.map((node) => <option key={node.id} value={node.label} />)}</datalist>
                </form>
                <small className="search-feedback" aria-live="polite">{searchMessage}</small>
                <div className="legend" aria-label="Risk legend"><span className="legend-critical">Critical</span><span className="legend-watch">Watch</span><span className="legend-normal">Normal</span></div>
              </div>
            </header>
            <div className="graph-stage">
              {snapshot && snapshot.nodes.length > 0 ? (
                <NetworkGraph
                  nodes={snapshot.nodes}
                  edges={snapshot.edges}
                  selectedNodeId={selectedNodeId}
                  selectedEdgeId={selectedEdgeId}
                  onSelectNode={selectNode}
                  onSelectEdge={selectEdge}
                />
              ) : status === "loading" ? (
                <div className="graph-message"><span className="loading-ring" /><strong>Mapping connected accounts</strong><p>Reading the latest bounded Neo4j snapshot.</p></div>
              ) : (
                <div className="graph-message"><span className="empty-graph" aria-hidden="true">∅</span><strong>No transfers match these filters</strong><p>Lower the risk threshold or inspect another community.</p></div>
              )}
              {status === "loading" && snapshot && <div className="updating-badge">Updating graph…</div>}
            </div>
            <footer className="graph-footer"><span>Drag nodes to separate clusters</span><span>Select an arrow to inspect its transfer</span></footer>
          </article>

          <aside className="detail-panel">
            <header className="panel-header"><div><span className="section-label">Analyst focus</span><h2>Investigation details</h2></div></header>
            <AccountDetails
              node={selectedNode}
              nodes={snapshot?.nodes ?? []}
              edges={selectedEdges}
              selectedEdgeId={selectedEdgeId}
              onSelectEdge={selectEdge}
              onClearEdge={() => setSelectedEdgeId(null)}
            />
          </aside>
        </section>
      </main>
    </div>
  );
}
