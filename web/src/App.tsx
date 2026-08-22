import { useCallback, useEffect, useMemo, useState } from "react";
import { fetchGraphSnapshot } from "./api";
import {
  deriveDashboardMetrics,
  highestRiskNode,
  riskBand,
} from "./graph";
import NetworkGraph from "./NetworkGraph";
import type { DashboardNode, GraphFilters, GraphSnapshot } from "./types";

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

function AccountDetails({
  node,
  connections,
  indicators,
}: {
  node: DashboardNode | null;
  connections: number;
  indicators: string[];
}) {
  if (!node) {
    return (
      <div className="detail-empty">
        <span className="detail-empty-mark" aria-hidden="true">◎</span>
        <h3>Select an account</h3>
        <p>Choose a node to inspect its graph risk, centrality, and transfer signals.</p>
      </div>
    );
  }

  const band = riskBand(node.graph_risk_score);
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
        <div><dt>Connections</dt><dd>{connections}</dd></div>
        <div><dt>Risk tier</dt><dd>{node.risk_tier ?? "Unknown"}</dd></div>
        <div><dt>Account type</dt><dd>{node.account_type ?? "Unknown"}</dd></div>
        <div><dt>Country</dt><dd>{node.country ?? "Unknown"}</dd></div>
      </dl>

      <div className="signals">
        <span className="section-label">Observed signals</span>
        {indicators.length > 0 ? (
          <ul>{indicators.map((indicator) => <li key={indicator}>{indicator.replaceAll("_", " ")}</li>)}</ul>
        ) : (
          <p>No explicit transfer indicators in this snapshot.</p>
        )}
      </div>
    </div>
  );
}

export default function App() {
  const [snapshot, setSnapshot] = useState<GraphSnapshot | null>(null);
  const [draftFilters, setDraftFilters] = useState(DEFAULT_FILTERS);
  const [appliedFilters, setAppliedFilters] = useState(DEFAULT_FILTERS);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
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
          if (current && nextSnapshot.nodes.some((node) => node.id === current)) {
            return current;
          }
          return highestRiskNode(nextSnapshot.nodes)?.id ?? null;
        });
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
  const selectedIndicators = [...new Set(selectedEdges.flatMap((edge) => edge.risk_indicators))];
  const selectNode = useCallback((nodeId: string) => setSelectedNodeId(nodeId), []);

  function applyFilters(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setAppliedFilters({ ...draftFilters });
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <a className="brand" href="#workspace" aria-label="FinGraph dashboard home">
          <span className="brand-mark" aria-hidden="true">
            <i /><i /><i />
          </span>
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
          <div className="error-banner" role="alert">
            <strong>Live graph unavailable.</strong> {error} Check the dashboard API, then refresh this snapshot.
          </div>
        )}

        <section className="investigation-grid">
          <article className="graph-panel">
            <header className="panel-header">
              <div><span className="section-label">Transaction network</span><h2>Account flow map</h2></div>
              <div className="legend" aria-label="Risk legend"><span className="legend-critical">Critical</span><span className="legend-watch">Watch</span><span className="legend-normal">Normal</span></div>
            </header>
            <div className="graph-stage">
              {snapshot && snapshot.nodes.length > 0 ? (
                <NetworkGraph nodes={snapshot.nodes} edges={snapshot.edges} selectedNodeId={selectedNodeId} onSelectNode={selectNode} />
              ) : status === "loading" ? (
                <div className="graph-message"><span className="loading-ring" /><strong>Mapping connected accounts</strong><p>Reading the latest bounded Neo4j snapshot.</p></div>
              ) : (
                <div className="graph-message"><span className="empty-graph" aria-hidden="true">∅</span><strong>No transfers match these filters</strong><p>Lower the risk threshold or inspect another community.</p></div>
              )}
              {status === "loading" && snapshot && <div className="updating-badge">Updating graph…</div>}
            </div>
            <footer className="graph-footer"><span>Drag nodes to separate clusters</span><span>Scroll to zoom · select to inspect</span></footer>
          </article>

          <aside className="detail-panel">
            <header className="panel-header"><div><span className="section-label">Analyst focus</span><h2>Account profile</h2></div></header>
            <AccountDetails node={selectedNode} connections={selectedEdges.length} indicators={selectedIndicators} />
          </aside>
        </section>
      </main>
    </div>
  );
}
