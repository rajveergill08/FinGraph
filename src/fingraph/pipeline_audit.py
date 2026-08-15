"""Measure the live Kafka-to-Neo4j transaction path against the review target."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
import time
from typing import Any, Callable, Sequence
from uuid import uuid4

from .simulator import TransactionNetworkSimulator


_FIND_TRANSACTION = """
MATCH ()-[transfer:TRANSFERRED_TO {transaction_id: $transaction_id}]->()
RETURN transfer.transaction_id AS transaction_id
LIMIT 1
"""


@dataclass(frozen=True, slots=True)
class AuditSettings:
    kafka_bootstrap_servers: str = "localhost:9092"
    transaction_topic: str = "fingraph.transactions.v1"
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_username: str = "neo4j"
    neo4j_password: str = "change-me-now"
    neo4j_database: str = "neo4j"

    @classmethod
    def from_environment(cls) -> "AuditSettings":
        defaults = cls()
        return cls(
            kafka_bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", defaults.kafka_bootstrap_servers
            ),
            transaction_topic=os.getenv(
                "KAFKA_TRANSACTION_TOPIC", defaults.transaction_topic
            ),
            neo4j_uri=os.getenv("NEO4J_URI", defaults.neo4j_uri),
            neo4j_username=os.getenv("NEO4J_USERNAME", defaults.neo4j_username),
            neo4j_password=os.getenv("NEO4J_PASSWORD", defaults.neo4j_password),
            neo4j_database=os.getenv("NEO4J_DATABASE", defaults.neo4j_database),
        )


@dataclass(frozen=True, slots=True)
class AuditResult:
    transaction_id: str
    latency_ms: float
    target_ms: float

    @property
    def passed(self) -> bool:
        return self.latency_ms < self.target_ms


def build_audit_event(
    *, transaction_id: str | None = None, occurred_at: datetime | None = None
) -> dict[str, object]:
    """Create one simulator event with a unique transaction identity."""
    timestamp = occurred_at or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        raise ValueError("occurred_at must be timezone-aware.")
    fixture = TransactionNetworkSimulator(seed=815, start_at=timestamp).generate(
        normal_transaction_count=0,
        syndicate_source_count=2,
        intermediary_count=1,
    )
    event = next(fixture.events())
    transaction = event["transaction"]
    assert isinstance(transaction, dict)
    transaction["transaction_id"] = transaction_id or f"audit-{uuid4().hex}"
    transaction["occurred_at"] = timestamp.isoformat()
    return event


class PipelineAuditor:
    """Publish one event and poll Neo4j until its graph edge is visible."""

    def __init__(
        self,
        settings: AuditSettings | None = None,
        *,
        producer: Any = None,
        driver: Any = None,
        clock: Callable[[], float] = time.perf_counter,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings or AuditSettings.from_environment()
        self._producer = producer
        self._driver = driver
        self._owns_producer = producer is None
        self._owns_driver = driver is None
        self._clock = clock
        self._sleep = sleep

    def open(self) -> None:
        if self._driver is None:
            from neo4j import GraphDatabase

            self._driver = GraphDatabase.driver(
                self.settings.neo4j_uri,
                auth=(self.settings.neo4j_username, self.settings.neo4j_password),
            )
        self._driver.verify_connectivity()

        if self._producer is None:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=self.settings.kafka_bootstrap_servers.split(","),
                client_id="fingraph-pipeline-audit",
                acks="all",
                retries=3,
                request_timeout_ms=10_000,
            )
        bootstrap_connected = getattr(self._producer, "bootstrap_connected", None)
        if callable(bootstrap_connected) and not bootstrap_connected():
            raise RuntimeError(
                f"Kafka is not reachable at {self.settings.kafka_bootstrap_servers}."
            )

    def run(
        self,
        *,
        timeout_seconds: float = 1.0,
        poll_interval_seconds: float = 0.01,
        event: dict[str, object] | None = None,
    ) -> AuditResult:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive.")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive.")
        if self._producer is None or self._driver is None:
            raise RuntimeError("Pipeline auditor is not open.")

        audit_event = event or build_audit_event()
        transaction = audit_event.get("transaction")
        if not isinstance(transaction, dict) or not isinstance(
            transaction.get("transaction_id"), str
        ):
            raise ValueError("Audit event must contain a transaction_id.")
        transaction_id = transaction["transaction_id"]
        payload = json.dumps(audit_event, separators=(",", ":"), sort_keys=True).encode(
            "utf-8"
        )

        started_at = self._clock()
        acknowledgement = self._producer.send(
            self.settings.transaction_topic,
            key=transaction_id.encode("utf-8"),
            value=payload,
        )
        acknowledgement.get(timeout=min(10.0, timeout_seconds))

        while True:
            result = self._driver.execute_query(
                _FIND_TRANSACTION,
                parameters_={"transaction_id": transaction_id},
                database_=self.settings.neo4j_database,
            )
            elapsed = self._clock() - started_at
            if result.records:
                return AuditResult(
                    transaction_id=transaction_id,
                    latency_ms=round(elapsed * 1000, 3),
                    target_ms=timeout_seconds * 1000,
                )
            if elapsed >= timeout_seconds:
                raise TimeoutError(
                    f"Transaction {transaction_id} was not visible in Neo4j within "
                    f"{timeout_seconds:.3f}s."
                )
            self._sleep(min(poll_interval_seconds, timeout_seconds - elapsed))

    def close(self) -> None:
        if self._producer is not None and self._owns_producer:
            self._producer.close()
            self._producer = None
        if self._driver is not None and self._owns_driver:
            self._driver.close()
            self._driver = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit Kafka-to-Neo4j transaction visibility latency."
    )
    parser.add_argument("--target-ms", type=float, default=1000.0)
    parser.add_argument("--poll-ms", type=float, default=10.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.target_ms <= 0 or args.poll_ms <= 0:
        raise SystemExit("--target-ms and --poll-ms must be positive.")
    auditor = PipelineAuditor()
    try:
        auditor.open()
        result = auditor.run(
            timeout_seconds=args.target_ms / 1000,
            poll_interval_seconds=args.poll_ms / 1000,
        )
    except TimeoutError as exc:
        print(json.dumps({"passed": False, "error": str(exc)}, sort_keys=True))
        return 1
    finally:
        auditor.close()
    print(
        json.dumps(
            {
                "latency_ms": result.latency_ms,
                "passed": result.passed,
                "target_ms": result.target_ms,
                "transaction_id": result.transaction_id,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
