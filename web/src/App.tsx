import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
} from "react";
import {
  fetchAlertStatus,
  fetchGraphSnapshot,
  fetchStarburstPatterns,
} from "./api";
import {
  buildGraphView,
  deriveDashboardMetrics,
  deriveCommunitySummaries,
  findAccountByQuery,
  formatTransferAmount,
  highestRiskNode,
  riskBand,
} from "./graph";
import NetworkGraph from "./NetworkGraph";
import { AUTO_REFRESH_MS, startAutoRefresh } from "./refresh";
import type {
  AlertStatusSnapshot,
  CommunitySummary,
  DashboardEdge,
  DashboardNode,
  GraphFilters,
  GraphSnapshot,
  StarburstPattern,
  StarburstSnapshot,
} from "./types";

const DEFAULT_FILTERS: GraphFilters = {
  edge_limit: 200,
  minimum_risk_score: 0,
  minimum_pagerank_score: 0,
  community_id: null,
};

type LoadStatus = "loading" | "refreshing" | "ready" | "error";

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

function CommunityControls({
  communities,
  collapsedCommunityIds,
  hiddenInternalTransferCount,
  onToggleCommunity,
  onExpandAll,
  onCollapseAll,
}: {
  communities: CommunitySummary[];
  collapsedCommunityIds: ReadonlySet<number>;
  hiddenInternalTransferCount: number;
  onToggleCommunity: (communityId: number) => void;
  onExpandAll: () => void;
  onCollapseAll: () => void;
}) {
  const allCollapsed =
    communities.length > 0 &&
    communities.every((community) => collapsedCommunityIds.has(community.id));
  return (
    <section className="community-toolbar" aria-label="Community display controls">
      <div className="community-toolbar-heading">
        <span><strong>Louvain communities</strong><small>{hiddenInternalTransferCount > 0 ? `${hiddenInternalTransferCount} internal transfers summarized` : "All transfers visible"}</small></span>
        <button type="button" onClick={allCollapsed ? onExpandAll : onCollapseAll} disabled={communities.length === 0}>
          {allCollapsed ? "Expand all" : "Collapse all"}
        </button>
      </div>
      <div className="community-list">
        {communities.length > 0 ? communities.map((community) => {
          const collapsed = collapsedCommunityIds.has(community.id);
          return (
            <button
              type="button"
              key={community.id}
              className={collapsed ? "community-control collapsed" : "community-control"}
              aria-pressed={collapsed}
              onClick={() => onToggleCommunity(community.id)}
            >
              <span>Community {community.id}</span>
              <strong>{community.accountCount}</strong>
              <small>{collapsed ? "Expand" : "Collapse"}</small>
            </button>
          );
        }) : <p>Run Louvain scoring to enable community controls.</p>}
      </div>
    </section>
  );
}

function StarburstAlerts({
  snapshot,
  visibleAccountIds,
  onFocusSink,
}: {
  snapshot: StarburstSnapshot | null;
  visibleAccountIds: ReadonlySet<string>;
  onFocusSink: (pattern: StarburstPattern) => void;
}) {
  const patterns = snapshot?.patterns ?? [];
  return (
    <section
      className={patterns.length > 0 ? "starburst-alerts active" : "starburst-alerts"}
      aria-label="Starburst topology alerts"
      aria-live="polite"
    >
      <header>
        <span className="starburst-mark" aria-hidden="true">✦</span>
        <span>
          <span className="section-label">Topology surveillance</span>
          <strong>
            {!snapshot
              ? "Scanning for multi-hop funnels"
              : patterns.length > 0
                ? `${patterns.length} starburst ${patterns.length === 1 ? "pattern" : "patterns"} detected`
                : "No starburst pattern detected"}
          </strong>
        </span>
        <small>
          {snapshot
            ? `${snapshot.filters.lookback_hours}-hour graph window`
            : "Awaiting Neo4j analysis"}
        </small>
      </header>
      {patterns.length > 0 && (
        <div className="starburst-list">
          {patterns.map((pattern) => {
            const sinkVisible = visibleAccountIds.has(pattern.sink_account_id);
            return (
              <article key={pattern.id}>
                <div>
                  <strong title={pattern.sink_account_id}>{pattern.sink_account_id}</strong>
                  <p>
                    <b>{pattern.source_count}</b> source accounts funnel through{" "}
                    <b>{pattern.intermediary_count}</b> intermediaries into one sink.
                  </p>
                  <small>
                    {pattern.inbound_transfer_count + pattern.outbound_transfer_count} linked transfers · latest {formatTransactionTime(pattern.latest_transfer_at)}
                  </small>
                </div>
                <button
                  type="button"
                  onClick={() => onFocusSink(pattern)}
                  disabled={!sinkVisible}
                  title={sinkVisible ? "Inspect the destination account" : "Sink account is outside the current graph filters"}
                >
                  {sinkVisible ? "Focus sink" : "Outside view"}
                </button>
              </article>
            );
          })}
        </div>
      )}
    </section>
  );
}

function AlertStatusPanel({
  snapshot,
  visibleAccountIds,
  onFocusAccount,
}: {
  snapshot: AlertStatusSnapshot | null;
  visibleAccountIds: ReadonlySet<string>;
  onFocusAccount: (accountId: string) => void;
}) {
  const candidates = snapshot?.candidates ?? [];
  const deliveredAccounts = candidates.filter(
    (candidate) => candidate.deliveries.length > 0,
  ).length;
  return (
    <section
      className={candidates.length > 0 ? "alert-status active" : "alert-status"}
      aria-label="Automated risk alert status"
      aria-live="polite"
    >
      <header>
        <span className="alert-status-mark" aria-hidden="true">!</span>
        <span>
          <span className="section-label">Automated response</span>
          <strong>
            {!snapshot
              ? "Evaluating high-risk account rules"
              : candidates.length > 0
                ? `${candidates.length} ${candidates.length === 1 ? "account meets" : "accounts meet"} the alert rule`
                : "No account meets the alert rule"}
          </strong>
        </span>
        <small>
          {snapshot
            ? `Risk ≥ ${snapshot.filters.minimum_risk_score} · ${deliveredAccounts} with recorded delivery`
            : "Awaiting rules engine status"}
        </small>
      </header>
      {candidates.length > 0 && (
        <div className="alert-status-list">
          {candidates.slice(0, 6).map((candidate) => {
            const accountVisible = visibleAccountIds.has(candidate.account_id);
            const latestDelivery = candidate.deliveries[0] ?? null;
            return (
              <article key={candidate.account_id}>
                <div className="alert-score">
                  <strong>{candidate.graph_risk_score.toFixed(1)}</strong>
                  <small>risk score</small>
                </div>
                <div>
                  <strong title={candidate.account_id}>{candidate.account_id}</strong>
                  <p>
                    {candidate.risk_tier ?? "Unknown tier"} · {candidate.country ?? "Unknown country"} · {candidate.counterparty_count} counterparties
                  </p>
                  <small>
                    {latestDelivery
                      ? `${latestDelivery.channel} delivered ${formatTransactionTime(latestDelivery.delivered_at)}`
                      : "No delivery recorded — preview mode or channel not configured"}
                  </small>
                </div>
                <button
                  type="button"
                  onClick={() => onFocusAccount(candidate.account_id)}
                  disabled={!accountVisible}
                  title={accountVisible ? "Inspect this high-risk account" : "Account is outside the current graph filters"}
                >
                  {accountVisible ? "Inspect" : "Outside view"}
                </button>
              </article>
            );
          })}
        </div>
      )}
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
  const [starburstSnapshot, setStarburstSnapshot] = useState<StarburstSnapshot | null>(null);
  const [alertStatusSnapshot, setAlertStatusSnapshot] = useState<AlertStatusSnapshot | null>(null);
  const [draftFilters, setDraftFilters] = useState(DEFAULT_FILTERS);
  const [appliedFilters, setAppliedFilters] = useState(DEFAULT_FILTERS);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null);
  const [collapsedCommunityIds, setCollapsedCommunityIds] = useState<Set<number>>(
    () => new Set(),
  );
  const [autoRefreshEnabled, setAutoRefreshEnabled] = useState(true);
  const [accountQuery, setAccountQuery] = useState("");
  const [searchMessage, setSearchMessage] = useState("");
  const [status, setStatus] = useState<LoadStatus>("loading");
  const [error, setError] = useState("");
  const [refreshVersion, setRefreshVersion] = useState(0);
  const hasSnapshot = useRef(false);

  useEffect(() => {
    const controller = new AbortController();
    setStatus(hasSnapshot.current ? "refreshing" : "loading");
    setError("");
    Promise.all([
      fetchGraphSnapshot(appliedFilters, controller.signal),
      fetchStarburstPatterns(controller.signal),
      fetchAlertStatus(controller.signal),
    ])
      .then(([nextSnapshot, nextStarburstSnapshot, nextAlertStatusSnapshot]) => {
        hasSnapshot.current = true;
        setSnapshot(nextSnapshot);
        setStarburstSnapshot(nextStarburstSnapshot);
        setAlertStatusSnapshot(nextAlertStatusSnapshot);
        setSelectedNodeId((current) => {
          if (current && nextSnapshot.nodes.some((node) => node.id === current)) return current;
          return highestRiskNode(nextSnapshot.nodes)?.id ?? null;
        });
        setSelectedEdgeId((current) =>
          current && nextSnapshot.edges.some((edge) => edge.id === current) ? current : null,
        );
        setCollapsedCommunityIds((current) => {
          const availableCommunities = new Set(
            nextSnapshot.nodes.flatMap((node) =>
              node.community_id === null ? [] : [node.community_id],
            ),
          );
          return new Set(
            [...current].filter((communityId) =>
              availableCommunities.has(communityId),
            ),
          );
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

  useEffect(() => {
    if (!autoRefreshEnabled) return undefined;
    return startAutoRefresh(
      () => setRefreshVersion((current) => current + 1),
      AUTO_REFRESH_MS,
      () => document.visibilityState === "visible",
    );
  }, [autoRefreshEnabled]);

  const metrics = useMemo(
    () => snapshot ? deriveDashboardMetrics(snapshot) : null,
    [snapshot],
  );
  const communities = useMemo(
    () => deriveCommunitySummaries(snapshot?.nodes ?? []),
    [snapshot],
  );
  const graphView = useMemo(
    () => snapshot ? buildGraphView(snapshot, collapsedCommunityIds) : null,
    [collapsedCommunityIds, snapshot],
  );
  const visibleAccountIds = useMemo(
    () => new Set(snapshot?.nodes.map((node) => node.id) ?? []),
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

  const expandCommunity = useCallback((communityId: number) => {
    setCollapsedCommunityIds((current) => {
      const next = new Set(current);
      next.delete(communityId);
      return next;
    });
  }, []);

  const toggleCommunity = useCallback((communityId: number) => {
    const willCollapse = !collapsedCommunityIds.has(communityId);
    setCollapsedCommunityIds((current) => {
      const next = new Set(current);
      if (next.has(communityId)) next.delete(communityId);
      else next.add(communityId);
      return next;
    });
    if (willCollapse && selectedNode?.community_id === communityId) {
      setSelectedNodeId(null);
      setSelectedEdgeId(null);
    }
  }, [collapsedCommunityIds, selectedNode]);

  const collapseAllCommunities = useCallback(() => {
    setCollapsedCommunityIds(new Set(communities.map((community) => community.id)));
    if (selectedNode?.community_id != null) {
      setSelectedNodeId(null);
      setSelectedEdgeId(null);
    }
  }, [communities, selectedNode]);

  const expandAllCommunities = useCallback(() => {
    setCollapsedCommunityIds(new Set());
  }, []);

  const focusStarburstSink = useCallback((pattern: StarburstPattern) => {
    const sink = snapshot?.nodes.find(
      (node) => node.id === pattern.sink_account_id,
    );
    if (!sink) return;
    if (sink.community_id !== null) expandCommunity(sink.community_id);
    selectNode(sink.id);
    setAccountQuery(sink.label);
    setSearchMessage(`Focused starburst sink ${sink.label}`);
  }, [expandCommunity, selectNode, snapshot]);

  const focusAlertAccount = useCallback((accountId: string) => {
    const account = snapshot?.nodes.find((node) => node.id === accountId);
    if (!account) return;
    if (account.community_id !== null) expandCommunity(account.community_id);
    selectNode(account.id);
    setAccountQuery(account.label);
    setSearchMessage(`Focused alert candidate ${account.label}`);
  }, [expandCommunity, selectNode, snapshot]);

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
    if (match.community_id !== null) expandCommunity(match.community_id);
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
          <span><strong>{status === "ready" ? "Pipeline live" : status === "refreshing" ? "Refreshing live graph" : status === "loading" ? "Syncing graph" : "Pipeline unavailable"}</strong><small>Neo4j snapshot · {formatSnapshotTime(snapshot?.generated_at)}</small></span>
        </div>
      </header>

      <main id="workspace">
        <section className="workspace-heading">
          <div>
            <span className="eyebrow">Live investigation workspace</span>
            <h1>Follow the money, not the rows.</h1>
            <p>Trace high-risk accounts, central intermediaries, and syndicate transfers as one connected graph.</p>
          </div>
          <div className="refresh-actions">
            <button className="auto-refresh-button" type="button" aria-pressed={autoRefreshEnabled} onClick={() => setAutoRefreshEnabled((enabled) => !enabled)}>
              <span className={autoRefreshEnabled ? "auto-refresh-dot enabled" : "auto-refresh-dot"} aria-hidden="true" />
              <span><strong>Live refresh {autoRefreshEnabled ? "on" : "off"}</strong><small>Every {AUTO_REFRESH_MS / 1000} seconds</small></span>
            </button>
            <button className="refresh-button" type="button" onClick={() => setRefreshVersion((value) => value + 1)} disabled={status === "loading" || status === "refreshing"}>
              <span aria-hidden="true">↻</span> Refresh now
            </button>
          </div>
        </section>

        <section className="metric-strip" aria-label="Current graph summary">
          <article><span>Accounts in view</span><strong>{metricValue(metrics?.accountCount ?? 0)}</strong><small>Unique graph nodes</small></article>
          <article><span>Transfers traced</span><strong>{metricValue(metrics?.transferCount ?? 0)}</strong><small>Bounded latest edges</small></article>
          <article className="critical-metric"><span>High-risk accounts</span><strong>{metricValue(metrics?.highRiskCount ?? 0)}</strong><small>Risk score ≥ 70</small></article>
          <article><span>Detected communities</span><strong>{metricValue(metrics?.communityCount ?? 0)}</strong><small>Louvain groups</small></article>
          <article><span>Flagged transfers</span><strong>{metricValue(metrics?.flaggedTransferCount ?? 0)}</strong><small>With risk indicators</small></article>
        </section>

        <StarburstAlerts
          snapshot={starburstSnapshot}
          visibleAccountIds={visibleAccountIds}
          onFocusSink={focusStarburstSink}
        />

        <AlertStatusPanel
          snapshot={alertStatusSnapshot}
          visibleAccountIds={visibleAccountIds}
          onFocusAccount={focusAlertAccount}
        />

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
            <CommunityControls
              communities={communities}
              collapsedCommunityIds={collapsedCommunityIds}
              hiddenInternalTransferCount={graphView?.hiddenInternalTransferCount ?? 0}
              onToggleCommunity={toggleCommunity}
              onExpandAll={expandAllCommunities}
              onCollapseAll={collapseAllCommunities}
            />
            <div className="graph-stage">
              {graphView && graphView.nodes.length > 0 ? (
                <NetworkGraph
                  nodes={graphView.nodes}
                  edges={graphView.edges}
                  selectedNodeId={selectedNodeId}
                  selectedEdgeId={selectedEdgeId}
                  onSelectNode={selectNode}
                  onSelectEdge={selectEdge}
                  onExpandCommunity={expandCommunity}
                />
              ) : status === "loading" ? (
                <div className="graph-message"><span className="loading-ring" /><strong>Mapping connected accounts</strong><p>Reading the latest bounded Neo4j snapshot.</p></div>
              ) : (
                <div className="graph-message"><span className="empty-graph" aria-hidden="true">∅</span><strong>No transfers match these filters</strong><p>Lower the risk threshold or inspect another community.</p></div>
              )}
              {status === "refreshing" && snapshot && <div className="updating-badge">Live refresh…</div>}
            </div>
            <footer className="graph-footer"><span>Collapsed clusters summarize internal transfers</span><span>Select a cluster node to expand it</span></footer>
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
