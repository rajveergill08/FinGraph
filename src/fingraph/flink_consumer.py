"""PyFlink Kafka consumer that validates transactions and upserts Neo4j."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any, Mapping

from .stream_contract import EventValidationError, normalise_transaction_event


@dataclass(frozen=True, slots=True)
class StreamSettings:
    kafka_bootstrap_servers: str = "localhost:9092"
    transaction_topic: str = "fingraph.transactions.v1"
    dead_letter_topic: str = "fingraph.transactions.dlq.v1"
    consumer_group: str = "fingraph-neo4j-v1"
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_username: str = "neo4j"
    neo4j_password: str = "change-me-now"

    @classmethod
    def from_environment(cls) -> "StreamSettings":
        defaults = cls()
        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", defaults.kafka_bootstrap_servers
            ),
            transaction_topic=os.getenv("KAFKA_TRANSACTION_TOPIC", defaults.transaction_topic),
            dead_letter_topic=os.getenv("KAFKA_DEAD_LETTER_TOPIC", defaults.dead_letter_topic),
            consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", defaults.consumer_group),
            neo4j_uri=os.getenv("NEO4J_URI", defaults.neo4j_uri),
            neo4j_username=os.getenv("NEO4J_USERNAME", defaults.neo4j_username),
            neo4j_password=os.getenv("NEO4J_PASSWORD", defaults.neo4j_password),
        )


def decode_and_normalise(raw_event: str | bytes) -> dict[str, object]:
    """Decode one Kafka value and enforce the shared stream contract."""
    try:
        text = raw_event.decode("utf-8") if isinstance(raw_event, bytes) else raw_event
        payload = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        raise EventValidationError(f"Kafka value must be a UTF-8 JSON object: {exc}") from exc
    if not isinstance(payload, Mapping):
        raise EventValidationError("Kafka value must contain a JSON object.")
    return normalise_transaction_event(payload)


def dead_letter_record(raw_event: str | bytes, error: Exception) -> str:
    """Build a stable, JSON-safe dead-letter value without losing the input."""
    raw = raw_event.decode("utf-8", errors="replace") if isinstance(raw_event, bytes) else str(raw_event)
    return json.dumps(
        {"raw_event": raw, "error_type": type(error).__name__, "error": str(error)},
        separators=(",", ":"),
        sort_keys=True,
    )


class Neo4jTransactionWriter:
    """Small lifecycle wrapper used by the Flink sink and unit tests."""

    def __init__(self, uri: str, username: str, password: str, query: str) -> None:
        self._uri = uri
        self._username = username
        self._password = password
        self._query = query
        self._driver: Any = None

    def open(self) -> None:
        from neo4j import GraphDatabase

        self._driver = GraphDatabase.driver(
            self._uri, auth=(self._username, self._password)
        )
        self._driver.verify_connectivity()

    def write(self, event: Mapping[str, object]) -> None:
        if self._driver is None:
            raise RuntimeError("Neo4j writer is not open.")
        self._driver.execute_query(self._query, event=dict(event), database_="neo4j")

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
            self._driver = None


def _upsert_query() -> str:
    return (Path(__file__).parents[2] / "neo4j" / "upsert_transaction.cypher").read_text(
        encoding="utf-8"
    )


def build_job(settings: StreamSettings | None = None) -> Any:
    """Build the PyFlink graph; imports are lazy to keep core tooling light."""
    try:
        from pyflink.common import Types, WatermarkStrategy
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream import OutputTag, StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import (
            DeliveryGuarantee,
            KafkaOffsetsInitializer,
            KafkaRecordSerializationSchema,
            KafkaSink,
            KafkaSource,
        )
        from pyflink.datastream.functions import ProcessFunction, SinkFunction
    except ImportError as exc:
        raise RuntimeError(
            'Streaming dependencies are missing; install with pip install -e ".[streaming]".'
        ) from exc

    config = settings or StreamSettings.from_environment()
    dlq_tag = OutputTag("invalid-transactions", Types.STRING())
    upsert_query = _upsert_query()

    class ValidateTransactions(ProcessFunction):
        def process_element(self, value: str, ctx: Any):
            try:
                yield json.dumps(decode_and_normalise(value), separators=(",", ":"))
            except EventValidationError as exc:
                ctx.output(dlq_tag, dead_letter_record(value, exc))

    class Neo4jSink(SinkFunction):
        def open(self, runtime_context: Any) -> None:
            self.writer = Neo4jTransactionWriter(
                config.neo4j_uri,
                config.neo4j_username,
                config.neo4j_password,
                upsert_query,
            )
            self.writer.open()

        def invoke(self, value: str, context: Any) -> None:
            self.writer.write(json.loads(value))

        def close(self) -> None:
            self.writer.close()

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.kafka_bootstrap_servers)
        .set_topics(config.transaction_topic)
        .set_group_id(config.consumer_group)
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    env = StreamExecutionEnvironment.get_execution_environment()
    events = env.from_source(source, WatermarkStrategy.no_watermarks(), "transactions")
    valid = events.process(ValidateTransactions(), output_type=Types.STRING())
    valid.add_sink(Neo4jSink()).name("neo4j-transaction-upsert")

    dlq_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(config.kafka_bootstrap_servers)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(config.dead_letter_topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build()
    )
    valid.get_side_output(dlq_tag).sink_to(dlq_sink).name("invalid-transaction-dlq")
    return env


def main() -> None:
    build_job().execute("FinGraph Kafka to Neo4j")
