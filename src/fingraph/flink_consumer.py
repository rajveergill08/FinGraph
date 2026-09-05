"""PyFlink Kafka consumer that validates transactions and upserts Neo4j."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping

from .stream_contract import EventValidationError, normalise_transaction_event


@dataclass(frozen=True, slots=True)
class StreamSettings:
    kafka_bootstrap_servers: str = "localhost:9092"
    transaction_topic: str = "fingraph.transactions.v1"
    dead_letter_topic: str = "fingraph.transactions.dlq.v1"
    consumer_group: str = "fingraph-neo4j-v1"
    kafka_connector_jar: str | None = None
    python_execution_mode: str = "process"
    parallelism: int = 1
    python_bundle_size: int = 50
    python_bundle_time_ms: int = 50
    checkpoint_interval_ms: int = 10_000
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
            kafka_connector_jar=os.getenv("FLINK_KAFKA_CONNECTOR_JAR") or None,
            python_execution_mode=os.getenv(
                "FLINK_PYTHON_EXECUTION_MODE", defaults.python_execution_mode
            ).lower(),
            parallelism=_positive_environment_integer(
                "FLINK_PARALLELISM", defaults.parallelism
            ),
            python_bundle_size=_positive_environment_integer(
                "FLINK_PYTHON_BUNDLE_SIZE", defaults.python_bundle_size
            ),
            python_bundle_time_ms=_positive_environment_integer(
                "FLINK_PYTHON_BUNDLE_TIME_MS", defaults.python_bundle_time_ms
            ),
            checkpoint_interval_ms=_positive_environment_integer(
                "FLINK_CHECKPOINT_INTERVAL_MS", defaults.checkpoint_interval_ms
            ),
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


def _positive_environment_integer(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer.") from exc
    if value < 1:
        raise ValueError(f"{name} must be a positive integer.")
    return value


def dead_letter_record(raw_event: str | bytes, error: Exception) -> str:
    """Build a stable, JSON-safe dead-letter value without losing the input."""
    raw = raw_event.decode("utf-8", errors="replace") if isinstance(raw_event, bytes) else str(raw_event)
    return json.dumps(
        {"raw_event": raw, "error_type": type(error).__name__, "error": str(error)},
        separators=(",", ":"),
        sort_keys=True,
    )


def validate_and_route(raw_event: str | bytes) -> tuple[str | None, str | None]:
    """Return one canonical event or one dead-letter record, never both."""
    try:
        event = decode_and_normalise(raw_event)
    except EventValidationError as exc:
        return None, dead_letter_record(raw_event, exc)
    return json.dumps(event, separators=(",", ":")), None


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


def _connector_jar_uri(configured_path: str | None) -> str:
    if not configured_path:
        raise RuntimeError(
            "FLINK_KAFKA_CONNECTOR_JAR is required. Run "
            "scripts/install_flink_connector.ps1 and configure the generated JAR path."
        )
    connector_jar = Path(configured_path).expanduser().resolve()
    if not connector_jar.is_file() or connector_jar.suffix.lower() != ".jar":
        raise RuntimeError(f"Flink Kafka connector JAR does not exist: {connector_jar}")
    return connector_jar.as_uri()


def build_job(settings: StreamSettings | None = None) -> Any:
    """Build the PyFlink graph; imports are lazy to keep core tooling light."""
    try:
        from pyflink.common import Configuration, Types, WatermarkStrategy
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream import OutputTag, StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import (
            DeliveryGuarantee,
            KafkaOffsetResetStrategy,
            KafkaOffsetsInitializer,
            KafkaRecordSerializationSchema,
            KafkaSink,
            KafkaSource,
        )
        from pyflink.datastream.functions import MapFunction, ProcessFunction
    except ImportError as exc:
        raise RuntimeError(
            'Streaming dependencies are missing; install with pip install -e ".[streaming]".'
        ) from exc

    config = settings or StreamSettings.from_environment()
    if config.python_execution_mode not in {"process", "thread"}:
        raise ValueError("FLINK_PYTHON_EXECUTION_MODE must be 'process' or 'thread'.")
    dlq_tag = OutputTag("invalid-transactions", Types.STRING())
    upsert_query = _upsert_query()
    flink_configuration = Configuration()
    flink_configuration.set_string("python.execution-mode", config.python_execution_mode)
    flink_configuration.set_integer(
        "python.fn-execution.bundle.size", config.python_bundle_size
    )
    flink_configuration.set_integer(
        "python.fn-execution.bundle.time", config.python_bundle_time_ms
    )
    env = StreamExecutionEnvironment.get_execution_environment(flink_configuration)
    env.set_python_executable(sys.executable)
    env.set_parallelism(config.parallelism)
    env.enable_checkpointing(config.checkpoint_interval_ms)
    env.add_jars(_connector_jar_uri(config.kafka_connector_jar))

    class ValidateTransactions(ProcessFunction):
        def process_element(self, value: str, ctx: Any):
            valid_event, invalid_event = validate_and_route(value)
            if invalid_event is not None:
                yield dlq_tag, invalid_event
            else:
                yield valid_event

    class Neo4jUpsert(MapFunction):
        def open(self, runtime_context: Any) -> None:
            self.writer = Neo4jTransactionWriter(
                config.neo4j_uri,
                config.neo4j_username,
                config.neo4j_password,
                upsert_query,
            )
            self.writer.open()

        def map(self, value: str) -> str:
            event = json.loads(value)
            self.writer.write(event)
            return str(event["transaction"]["transaction_id"])

        def close(self) -> None:
            self.writer.close()

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.kafka_bootstrap_servers)
        .set_topics(config.transaction_topic)
        .set_group_id(config.consumer_group)
        .set_starting_offsets(
            KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)
        )
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    events = env.from_source(source, WatermarkStrategy.no_watermarks(), "transactions")
    valid = events.process(ValidateTransactions(), output_type=Types.STRING())
    upserted = valid.map(Neo4jUpsert(), output_type=Types.STRING()).name(
        "neo4j-transaction-upsert"
    )
    upserted.print("neo4j-upserted").name("neo4j-upsert-confirmation")

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
