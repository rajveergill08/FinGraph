# September 5, 2026: final acceptance

## Decision

FinGraph's four implementation phases are complete for the local synthetic AML
scope. This record distinguishes verified behavior from operational limits; it
does not claim integration with a real bank, production load, or external alert
credentials.

## Gaps closed

- Replaced the invalid PyFlink `ctx.output(...)` path with the supported Python
  side-output yield. A live event containing only
  `{"probe":"final-review-dlq-20260905"}` reached
  `fingraph.transactions.dlq.v1` with the original input, error type
  `EventValidationError`, and the validation message.
- Enabled 10-second Flink checkpoints. After the full fixture, Kafka reported
  committed offsets 53, 66, and 56 with zero lag across the three partitions.
- Namespaced generated transaction IDs by seed. Replaying one seed remains
  deterministic, while different seeded fixtures no longer reuse transaction
  IDs for potentially different endpoints. This was found when an old
  unnamespaced `normal-00001` relationship referred to different endpoints and
  Neo4j correctly rejected the new relationship under its uniqueness constraint.
- Added a deterministic three-account circular fixture so the circular-flow
  query has a positive live demonstration, not only structural unit tests.

## Live data and analytics

The seed-42 simulator published 78 events: 20 ordinary transfers, a 50-source /
5-intermediary starburst, and a three-transfer circular flow. The analytics run
detected this ordered loop:

`account-normal-001 -> account-normal-002 -> account-normal-003 -> account-normal-001`

The returned transaction IDs were `circular-s42-00001` through
`circular-s42-00003`, with a total amount of USD 21,600. Risk scoring updated all
76 accounts. Louvain wrote six communities to all 76 accounts, and PageRank
wrote normalized centrality to all 76 accounts; the shell account ranked first.
The dry-run alert cycle completed without errors and returned six candidates at
or above the score-70 threshold. No external Slack/email message was sent.

## Streaming acceptance

A deliberately invalid event was preserved in the dead-letter topic rather than
reaching Neo4j. After a checkpoint, the stream container was force-recreated.
Its new log contained only the newly published readiness probe, not the earlier
78-event fixture, confirming committed-offset recovery and no fixture replay.

To recover from the pre-fix collision during this local synthetic test, the
processor was stopped and its consumer group was advanced to the ends of the
three Kafka partitions (25, 46, and 25). The corrected namespaced fixture was
then republished and fully consumed. Kafka and Neo4j volumes were not reset, and
this controlled test-state change is not part of normal operating instructions.

The first post-restart probe was not visible within five seconds but appeared
after the worker initialized. That failed result is retained as a cold-start
limitation. The immediately following ready-pipeline command
`fingraph-audit --target-ms 1000 --runs 5` passed all five samples:

| Sample | Latency |
| --- | ---: |
| 1 | 67.887 ms |
| 2 | 101.548 ms |
| 3 | 123.380 ms |
| 4 | 122.913 ms |
| 5 | 75.806 ms |

Maximum ready-pipeline latency was 123.380 ms, below the 1,000 ms target. Each
sample verified exactly one Neo4j edge with its expected transaction ID,
endpoints, USD 9,900 amount, and canonical currency.

## Final readiness

The final readiness audit passed with 76 accounts and 176 transfers before the
last restart probes. All five constraints and six indexes were present, and all
76 accounts had risk, Louvain, and PageRank properties. Three warmed runs of
each query stayed below 100 ms: circular flow reached 12.621 ms maximum with one
positive result; starburst reached 12.306 ms maximum with 12 results. Dashboard,
Neo4j Browser, API documentation, and API health all returned HTTP 200.

The complete Python suite passed 89 tests. The dashboard suite passed 20 tests,
and its TypeScript/Vite production build completed successfully.

## Operational limits

- The PyFlink process has significant cold-start time. Start it before the demo
  and wait for a readiness probe before using the one-second measurement.
- Measurements use a small local synthetic fixture and do not certify
  production throughput or exactly-once Neo4j writes.
- Slack/email delivery requires user-supplied credentials and was intentionally
  verified only in dry-run and automated sink tests.
- The freeze endpoint affects only FinGraph's mock graph; it is not connected to
  a bank core system.
