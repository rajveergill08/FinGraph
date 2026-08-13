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

Install the streaming extras, ensure Kafka and Neo4j are running, and apply
`neo4j/schema.cypher` once before starting the job:

```powershell
pip install -e ".[streaming]"
fingraph-stream
```

The PyFlink job resumes from committed offsets for consumer group
`fingraph-neo4j-v1`. Valid events are canonicalised and written with the
idempotent `neo4j/upsert_transaction.cypher` query. Invalid JSON or contract
violations are published to `fingraph.transactions.dlq.v1` with the original
value and validation error. Configure topic names, the consumer group, and
Neo4j connection through `.env.example`'s environment variables.

### Stream-contract validation

Before events enter the graph-writing flow, the Week 2 stream processor uses
`normalise_transaction_event` to reject malformed identities, account-map
mismatches, invalid IPs, unsupported channels, non-ISO currencies, money with
more than two decimal places, and timestamps without a timezone. It also
canonicalises countries, currency, channel, risk indicators, and timestamps
to UTC. Invalid records must go to a dead-letter path rather than be upserted.

## Verification

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v
```

The tests check that the starburst contains distinct source accounts and IPs,
that its transfers remain below the common USD 10,000 threshold, and that
Kafka event payloads are valid JSON.

## Planned milestones

- **Week 2:** Flink consumer, transaction cleaning, Neo4j real-time upserts,
  and multi-hop Cypher risk queries.
- **Week 3:** Neo4j Graph Data Science community and centrality scoring plus
  dashboard integration.
- **Week 4:** alert automation and investigation-dashboard refinement.
