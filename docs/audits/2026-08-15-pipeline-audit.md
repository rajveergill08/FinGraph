# Pipeline audit — 2026-08-15

## Scope

Verify the Week 2 path on the local Docker stack:

`Python simulator -> Kafka -> PyFlink validation -> Neo4j upsert`

The review target is for one acknowledged Kafka transaction to become a
queryable `TRANSFERRED_TO` relationship in Neo4j in under 1000 ms.

## Runtime

- Apache Kafka 4.3.1
- Apache Flink / PyFlink 1.20.5
- Flink Kafka connector 3.3.0-1.20 (SHA-1 verified during the image build)
- Neo4j Community 5.23
- Python 3.11

## Findings and correction

The first event missed the target because PyFlink's default Python bundle
timeout buffered low-volume events for up to 1000 ms. The stream job now uses
a 50 ms bundle timeout and a bundle size of 50. Both settings remain
configurable through environment variables.

## Evidence

Three independent post-tuning runs of
`fingraph-audit --target-ms 1000 --poll-ms 10` passed:

| Run | Kafka-to-Neo4j latency | Result |
| --- | ---: | --- |
| 1 | 169.713 ms | PASS |
| 2 | 104.254 ms | PASS |
| 3 | 89.959 ms | PASS |

Worst observed post-tuning latency was 169.713 ms, leaving 830.287 ms of
headroom against the review target. A graph verification query returned four
audit relationships with four distinct transaction IDs: the pre-tuning event
and the three passing post-tuning events.

Automated verification: 27 tests and 3 parameterized subtests passed. Docker's
static build check completed with no warnings.
