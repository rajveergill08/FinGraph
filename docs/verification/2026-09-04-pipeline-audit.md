# September 4, 2026: live pipeline verification

## Scope and environment

Story: a uniquely identified Python simulator event is published to Kafka,
cleaned by the running PyFlink consumer, upserted as a connected Neo4j transfer,
and returned by the dashboard API.

- Baseline: `6445a77` on `main`, with this day's audit/test/documentation changes.
- Windows host with Docker Compose; Kafka `apache/kafka:4.3.1`, Neo4j
  `neo4j:5.26-community`, and the existing FinGraph stream-processor image.
- Transaction and dead-letter topics were provisioned with three partitions.
- The schema was applied idempotently after Neo4j became available.
- No database reset, history rewriting, external alert delivery, or real-bank
  action was performed. The eleven unique synthetic probe edges were retained.

## Startup batch: failed, not discarded

Command: `fingraph-audit --target-ms 1000 --runs 5`

Started at `2026-09-04T11:56:15.317740+00:00` (17:26:15 IST).
The command returned exit code **1** and reported **2 of 5** samples passing.

| Transaction ID | Result |
| --- | --- |
| `audit-4d8411ee6c544166b229e0afb5eda8e1` | Not visible within 1 second |
| `audit-392d6303239f4eeead67661779878270` | Not visible within 1 second |
| `audit-3d0dd7136d444a5fafed323721b9d711` | Not visible within 1 second |
| `audit-619fb5386dfc4c2aa03e56612daa2399` | 266.892 ms |
| `audit-6ad1c21b3ac1431ab0aebd7b0438a14b` | 132.366 ms |

Flink subsequently logged `neo4j-upserted` for all five IDs, and Neo4j contained
them. This establishes eventual ingestion, not compliance with the one-second
target for the first three probes. The result is consistent with first-event
startup/warm-up overhead; the exact internal delay was not instrumented.

## Ready-pipeline batch: passed

The same command was run once after successful upsert confirmations.
Started at `2026-09-04T11:56:47.466172+00:00` (17:26:47 IST).
Exit code **0**, **5 of 5** samples passed, maximum **104.094 ms**.

| Transaction ID | Observed latency |
| --- | ---: |
| `audit-fafe3b6393a942fe99589fc330065393` | 104.094 ms |
| `audit-4cb37ccccd914cac944e9c57c36516b3` | 99.317 ms |
| `audit-2347d14ff60b49979ffb6b29292d9013` | 94.009 ms |
| `audit-0eee4006986f48ff899bf63800dc58bf` | 98.635 ms |
| `audit-e3b22790e4814c14918fade6f815ad64` | 89.246 ms |

For every probe the audit verified exactly one edge from `account-source-001`
to `account-intermediary-001`, amount `9900.0`, currency `USD`, and the unique
transaction ID. The timer includes Kafka send/acknowledgement, stream processing,
and the Neo4j visibility query; connection verification and the pre-publication
absence check occur before timing. `GET /api/graph?edge_limit=500` returned all
five new edges.

## Cleaning and API propagation

An additional simulator probe, `audit-8a75a09e7866451b9c70a02d37a26f51`, was
published with currency `usd` and channel `WEB`. Its connected edge appeared in
**138.097 ms**. The dashboard API returned exactly one matching edge with
currency `USD`, channel `web`, the expected account endpoints, and amount 9900.
Thus normalization took place in the stream, rather than only in the auditor.

## Readiness and regression checks

`fingraph-review --query-target-ms 100 --query-runs 3` passed at
`2026-09-04T11:56:53.446538+00:00`:

- 74 accounts and 90 transfers at that point, before the extra cleaning probe.
- All five required constraints and six indexes present.
- Risk, Louvain, and PageRank properties present on all 74 accounts.
- Maximum warmed circular-flow query time: 41.619 ms (zero matching loops).
- Maximum warmed starburst query time: 28.016 ms (two matching funnels).
- Dashboard, Neo4j Browser, API documentation, and API health returned HTTP 200.

The Python regression suite passed 84 tests, including ten new audit tests for
late-edge failure exit codes, duplicate/stale probes, connected-edge correctness,
canonicalization, retained failed samples, configuration bounds, and cleanup.
The dashboard's 20 tests and its TypeScript/Vite production build also passed.

## Interpretation and limits

The ready pipeline meets the one-second target on this small local fixture.
This is not a cold-start, production-load, restart-recovery, or exactly-once
delivery certification. No live invalid-event/dead-letter or external Slack/email
delivery audit was performed in this session. Fast circular-query execution with
zero results proves timing only, not a positive circular-pattern demonstration.
Allow startup time before presenting, retain failed batches, and rerun the audit
on the review machine rather than treating these measurements as a guarantee.
