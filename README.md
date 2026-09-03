# FinGraph

FinGraph is a streaming graph-analytics foundation for investigating money
laundering syndicates. It models people, accounts, banks, and transfers as a
connected Neo4j graph so that analysts can investigate patterns that isolated
transaction rules miss.

## Week 1: ingestion and graph foundation

The initial delivery contains:

- a deterministic Python simulator that creates ordinary transactions and a
  nested `starburst` syndicate: many unrelated accounts send sub-threshold
  transfers through intermediaries to an offshore shell account;
- a Kafka publisher for the versioned `fingraph.transactions.v1` topic;
- a Docker Compose development stack for Kafka and Neo4j; and
- Neo4j constraints, indexes, and an idempotent transaction-upsert query.

## Local setup

1. Copy `.env.example` to `.env` and adjust credentials if needed.
2. Create a virtual environment and install the package:

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -e ".[dev]"
   ```

3. Start Kafka and Neo4j:

   ```powershell
   docker compose up -d
   ```

   Kafka exposes its host-facing listener on port `9092` by default and keeps
   its Docker-network listener on `kafka:9092` for the later Flink job. If the
   host port is already used, set both `KAFKA_HOST_PORT` and
   `KAFKA_BOOTSTRAP_SERVERS` in `.env` to the same alternate host port before
   starting the stack.

4. Explicitly provision the three-partition transaction and dead-letter
   Kafka topics. This avoids relying on topic auto-creation, which is disabled
   in the local stack:

   ```powershell
   fingraph-sim provision
   ```

5. Apply the graph constraints and indexes in Neo4j Browser at
   `http://localhost:7474`, using the contents of `neo4j/schema.cypher`.

## Generate and inspect transaction events

Run a deterministic dry run first. It writes JSON Lines that can be inspected
or replayed later without needing Kafka:

```powershell
fingraph-sim generate --seed 42 --normal-transactions 20 --syndicate-sources 50 --intermediaries 5 --output data/transactions.jsonl
```

Publish the same shaped data to Kafka when the local stack is running:

```powershell
fingraph-sim publish --seed 42 --normal-transactions 20 --syndicate-sources 50 --intermediaries 5
```

The publisher provisions the topic idempotently before sending events. Every
event has a stable transaction ID, source and destination account metadata,
ISO-8601 timestamp, origin IP, and syndicate risk indicators.
`neo4j/upsert_transaction.cypher` specifies the idempotent graph write that
the Week 2 Flink consumer will call.

## Run the Week 2 stream consumer

The supported local path runs PyFlink in Docker so Java, the Python worker,
and the version-matched Flink Kafka connector are reproducible on Windows.
Start the data services, provision the topics, apply the schema, and then
start the consumer:

```powershell
docker compose up -d kafka neo4j
fingraph-sim provision
Get-Content .\neo4j\schema.cypher | docker compose exec -T neo4j `
  cypher-shell -u neo4j -p change-me-now
docker compose --profile streaming up --build -d stream-processor
docker compose logs --follow stream-processor
```

The first build downloads PyFlink and OpenJDK; later builds reuse Docker's
cache. The image verifies the pinned Kafka connector JAR checksum during the
build. For an advanced non-Docker Flink installation, download the same
connector to the ignored `.flink` directory before starting the job:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass `
  -File .\scripts\install_flink_connector.ps1
fingraph-stream
```

The PyFlink job resumes from committed offsets for consumer group
`fingraph-neo4j-v1`. Valid events are canonicalised and written with the
idempotent `neo4j/upsert_transaction.cypher` query. Invalid JSON or contract
violations are published to `fingraph.transactions.dlq.v1` with the original
value and validation error. Configure topic names, the consumer group, and
Neo4j connection through `.env.example`'s environment variables.

### Mid-project pipeline audit

With the stream processor running, publish one uniquely identified simulator
event and measure when its `TRANSFERRED_TO` edge becomes queryable in Neo4j:

```powershell
fingraph-audit --target-ms 1000
```

The command opens and verifies both connections before starting the timer,
waits for Kafka's acknowledgement, and polls Neo4j every 10 ms. It prints a
JSON result and exits non-zero unless the edge is visible in under one second.
The consumer uses a 50 ms Python bundle timeout by default so low-volume fraud
events are not held by Flink's throughput-oriented 1000 ms default.

### Stream-contract validation

Before events enter the graph-writing flow, the Week 2 stream processor uses
`normalise_transaction_event` to reject malformed identities, account-map
mismatches, invalid IPs, unsupported channels, non-ISO currencies, money with
more than two decimal places, and timestamps without a timezone. It also
canonicalises countries, currency, channel, risk indicators, and timestamps
to UTC. Invalid records must go to a dead-letter path rather than be upserted.

### Circular-flow detection and account risk scores

Run the Week 2 graph analytics after transactions have reached Neo4j:

```powershell
fingraph-analytics --lookback-hours 24 --minimum-amount 100 `
  --high-risk-country KY --high-risk-country IR
```

`neo4j/detect_circular_flows.cypher` finds time-ordered three-account loops
such as `A -> B -> C -> A`, anchors each directed loop by its lowest account
ID to avoid rotational duplicates, and bounds both its time window and result
count. `neo4j/refresh_account_risk_scores.cypher` writes a capped, explainable
0-100 score to every account using its configured risk tier, country,
counterparty breadth, recent transfer volume, and transaction risk indicators.
The score components are returned for analyst review; Week 3 GDS algorithms
will enrich rather than replace this rules-based baseline.

## Run Week 3 GDS community detection

The local Neo4j service uses Neo4j 5.26 Community with the compatible Graph
Data Science plugin. Start Neo4j and verify that GDS is available:

```powershell
docker compose up -d neo4j
docker compose exec neo4j cypher-shell -u neo4j -p change-me-now `
  "RETURN gds.version();"
```

After transaction data has been ingested and the Week 2 risk scores have been
refreshed, run weighted Louvain community detection:

```powershell
fingraph-gds --concurrency 1
```

The command projects `Account` nodes and `TRANSFERRED_TO` relationships into
the GDS in-memory catalog. Relationships are treated as undirected for
community detection, and parallel transaction amounts are summed into a
`transaction_volume` weight. Louvain writes `louvain_community_id` back to
each account and returns community size and risk-score summaries for analyst
review. The temporary projection is dropped after every run so repeated runs
do not leak graph-catalog memory.

### PageRank account centrality

Run weighted PageRank after transaction data has reached Neo4j:

```powershell
fingraph-pagerank --concurrency 1
```

The command preserves the direction of `TRANSFERRED_TO` relationships and
sums parallel transaction amounts into a `transaction_volume` weight. It
writes a MinMax-normalized `pagerank_score` between 0 and 1 to each account,
then returns the highest-centrality accounts with their community ID, graph
risk score, and inbound/outbound transfer context. The temporary directed
projection is always dropped after the run.

## Run the Week 3 analyst dashboard API

Start the analyst API and Neo4j through the dashboard Compose profile:

```powershell
docker compose --profile dashboard up --build -d dashboard-api
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod "http://localhost:8000/api/graph?edge_limit=200"
```

Interactive API documentation is available at `http://localhost:8000/docs`.
`GET /api/graph` returns visualization-ready account nodes and transaction
edges. Analysts can bound the response and filter it with `edge_limit`,
`minimum_risk_score`, `minimum_pagerank_score`, and `community_id`. The API
routes parameterized Cypher as read traffic, caps every response at 500 edges,
and keeps Neo4j credentials on the server instead of exposing them to the
browser. Configure the React development origin with
`DASHBOARD_ALLOWED_ORIGINS`.

`GET /api/patterns/starbursts` independently scans the recent graph for a
multi-hop funnel shape: many distinct source accounts transferring through
multiple intermediaries into one destination. The bounded query uses topology
and transfer time order rather than trusting simulator syndicate labels. Its
lookback, source, intermediary, and result thresholds are validated by the API.

`GET /api/alerts` returns the current accounts meeting the automated risk rule
and combines them with the latest recorded delivery per notification channel.
The endpoint defaults to a risk score of 70, accepts bounded
`minimum_risk_score` and `limit` parameters, and exposes no webhook, SMTP, or
Neo4j credentials.

`POST /api/actions/freeze` accepts 1–200 validated account IDs and an analyst
reason. It changes state only when every requested account exists, marks the
accounts as frozen, and creates a timestamped `ContainmentCase` node connected
to each account by `FROZEN_IN` relationships. Individual and whole-network
actions therefore leave a graph-native audit trail instead of silently changing
an account property. This project endpoint demonstrates containment inside the
mock FinGraph environment; it is not connected to a real bank core system.

### Explore the React/D3 fraud network

The analyst workspace renders the API snapshot as a directed, force-positioned
network. Node colour and size surface graph risk, transfer arrows retain money
flow direction, and selecting an account reveals its PageRank, Louvain
community, transfer connections, and observed risk indicators. Analysts can
bound the latest transfers, set a minimum risk score, or isolate a community
without exposing Neo4j credentials to the browser.

Use **Find account** to focus an account ID from the current bounded snapshot.
Select any transfer arrow or an account's connected-transfer row to inspect the
amount in its original currency, source and destination, timestamp, channel,
syndicate link, transaction ID, and individual risk indicators.

Live refresh is enabled by default and requests a new bounded snapshot every
10 seconds while the dashboard tab is visible; analysts can pause it or refresh
immediately. Louvain community controls collapse account groups into summary
nodes, hide only their internal arrows, and retain cross-community transfers.
Selecting a community summary node expands its member accounts again.
Starburst surveillance refreshes with the graph, shows the number of sources,
intermediaries, and linked transfers for every detected funnel, and lets the
analyst focus its destination account when it is present in the current view.
The automated-response panel refreshes on the same schedule, surfaces every
high-risk rule candidate, distinguishes recorded Slack/email delivery from
preview or unconfigured operation, and can focus visible candidate accounts.

Select **Export evidence** in the investigation details to download a
timestamped JSON dossier for the focused account. The export records the exact
bounded graph filters, account risk and centrality, connected counterparties,
flagged transfers, incoming and outgoing totals kept separate by currency,
matched starburst patterns, and automated-alert delivery context. This makes
the dashboard snapshot portable for a review while clearly identifying that it
is not a complete bank ledger.

Use **Freeze account** for the focused account. When starburst surveillance
finds a funnel, **Freeze network** submits its distinct sources,
intermediaries, and destination in one bounded action. The dashboard refreshes
after success, displays the containment case ID, and disables repeated actions
for accounts already marked as frozen.

Run the dashboard API and frontend together:

```powershell
docker compose --profile dashboard up --build -d dashboard-web
Start-Process http://localhost:5173
```

For frontend development with hot reload, keep the API on port 8000 and run:

```powershell
cd web
npm install
npm run dev
```

Set `VITE_API_BASE_URL` before `npm run build` when the browser-facing API is
not available at `http://localhost:8000`.

## Run the Week 4 risk-alert rules engine

Run one self-contained cycle that refreshes the explainable account risk scores
and previews every account meeting the configured threshold without contacting
an external service:

```powershell
python -m fingraph.alerting --dry-run
```

The bounded, read-only candidate query defaults to a graph risk score of 70 or
higher. Set `SLACK_WEBHOOK_URL` and/or the SMTP variables documented in
`.env.example` in the current shell, then run `python -m fingraph.alerting`
without `--dry-run` to deliver alerts locally. Delivery state is recorded per
account and channel in the ignored `data/alert-state.json` file, suppressing
repeat notifications for 24 hours by default. Failed channels are reported
without being marked as delivered, so a later cycle can retry them. No
notification channel or secret is enabled in the repository.

Start continuous polling through the opt-in Docker profile:

```powershell
docker compose --profile alerts up --build -d alert-worker
docker compose logs --follow alert-worker
```

The worker recalculates risk scores before every alert cycle, polls every 60
seconds, survives temporary Neo4j or delivery failures, and exits cleanly on
`SIGINT` or `SIGTERM`. Docker persists cooldown state in the `alert_state`
volume and mounts it read-only in the dashboard API so analysts can see the
latest per-channel delivery status. Docker Compose reads these values from the
local `.env` file.
`ALERT_DRY_RUN` defaults to `true`; set it to `false` only after a Slack webhook
or complete SMTP configuration has been added.

## Verification

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v
```

The tests check simulator integrity, Kafka provisioning and payloads, stream
validation, Neo4j upsert boundaries, circular-flow safeguards, risk-score
parameters, weighted Louvain communities, weighted PageRank centrality, the
dashboard API contract, alert-status shaping, and cooldown-aware Slack/email
alert delivery. Containment tests verify all-or-nothing account validation,
write routing, audit-case creation, request bounds, and API failure handling. The
`web` workspace separately verifies API filter routing, graph summaries,
account matching, currency-safe transfer display, visibility-aware refresh
scheduling, community-collapse transformations, and deterministic evidence
exports with `npm test`, while
`npm run build` performs the strict TypeScript production build.

### Final-review readiness audit

With the local dashboard profile running, execute the repeatable final-review
preflight:

```powershell
fingraph-review --query-target-ms 100 --query-runs 3
```

The command exits non-zero unless Neo4j contains accounts and transfers, all
required constraints and indexes are installed, every account has rules-based
risk, Louvain, and PageRank properties, and every warmed circular-flow and
starburst query run completes in under 100 ms. It also verifies the analyst
dashboard, Neo4j Browser, API documentation, and API health pages. The emitted
JSON provides a timestamped, machine-readable pass/fail record for the final
review. Use `--skip-pages` only for a database-only diagnostic; the separate
`fingraph-audit --target-ms 1000` command remains the end-to-end Kafka-to-Neo4j
latency proof because it intentionally publishes a uniquely identified event.

## Planned milestones

- **Week 2:** Flink consumer, transaction cleaning, Neo4j real-time upserts,
  and multi-hop Cypher risk queries.
- **Week 3:** Neo4j Graph Data Science community and centrality scoring plus
  dashboard integration (analytics, read-only API, and interactive React/D3
  visualization foundation implemented).
- **Week 4:** continuously polling Slack/email alert rules with fresh risk
  scoring, cooldown suppression, failure retry, and persistent Docker state
  plus dashboard delivery-status integration and portable investigation
  evidence exports implemented. Audited one-click account and starburst-network
  containment completes the investigation workflow; a repeatable readiness
  audit now covers graph data, schema, analytics, query latency, and review-page
  availability.
