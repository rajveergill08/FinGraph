"""Measure the live Kafka-to-Neo4j transaction path against the review target."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import os
import time
from typing import Any, Callable, Sequence
from uuid import uuid4

from .simulator import TransactionNetworkSimulator
from .stream_contract import normalise_transaction_event


_FIND_TRANSACTION = """
MATCH (source:Account)-[transfer:TRANSFERRED_TO {transaction_id: $transaction_id}]
      ->(destination:Account)
RETURN transfer.transaction_id AS transaction_id,
       source.account_id AS source_account_id,
       destination.account_id AS destination_account_id,
       transfer.amount AS amount, transfer.currency AS currency
LIMIT 2
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
    edge: dict[str, object]

    @property
    def passed(self) -> bool:
        return self.latency_ms < self.target_ms

    def as_dict(self) -> dict[str, object]:
        return {
            "transaction_id": self.transaction_id,
            "latency_ms": self.latency_ms,
            "target_ms": self.target_ms,
            "passed": self.passed,
            "edge": self.edge,
        }


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
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be finite and positive.")
        if not math.isfinite(poll_interval_seconds) or poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be finite and positive.")
        if self._producer is None or self._driver is None:
            raise RuntimeError("Pipeline auditor is not open.")

        audit_event = event if event is not None else build_audit_event()
        canonical = normalise_transaction_event(audit_event)
        transaction = canonical["transaction"]
        assert isinstance(transaction, dict)
        transaction_id = transaction["transaction_id"]
        # An existing edge must never masquerade as a newly ingested probe.
        if self._find_transaction(transaction_id).records:
            raise ValueError(f"Audit transaction {transaction_id} already exists.")
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
            result = self._find_transaction(transaction_id)
            elapsed = self._clock() - started_at
            if result.records:
                edge = result.records[0].data()
                expected = {
                    field: transaction[field]
                    for field in (
                        "transaction_id", "source_account_id",
                        "destination_account_id", "currency",
                    )
                }
                expected["amount"] = float(transaction["amount"])
                if len(result.records) != 1 or edge != expected:
                    raise RuntimeError(
                        f"Audit transaction {transaction_id} has unexpected graph data."
                    )
                return AuditResult(
                    transaction_id=transaction_id,
                    latency_ms=round(elapsed * 1000, 3),
                    target_ms=timeout_seconds * 1000,
                    edge=edge,
                )
            if elapsed >= timeout_seconds:
                raise TimeoutError(
                    f"Transaction {transaction_id} was not visible in Neo4j within "
                    f"{timeout_seconds:.3f}s."
                )
            self._sleep(min(poll_interval_seconds, timeout_seconds - elapsed))

    def _find_transaction(self, transaction_id: str) -> Any:
        return self._driver.execute_query(
            _FIND_TRANSACTION,
            parameters_={"transaction_id": transaction_id},
            database_=self.settings.neo4j_database,
        )

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
    parser.add_argument(
        "--runs", type=int, default=1,
        help="Number of sequential unique probes (1-20); every sample must pass.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if any(not math.isfinite(value) or value <= 0 for value in (args.target_ms, args.poll_ms)):
        raise SystemExit("--target-ms and --poll-ms must be finite and positive.")
    if not 1 <= args.runs <= 20:
        raise SystemExit("--runs must be between 1 and 20.")
    auditor = PipelineAuditor()
    generated_at = datetime.now(timezone.utc).isoformat()
    samples: list[dict[str, Any]] = []
    try:
        auditor.open()
        for _ in range(args.runs):
            event = build_audit_event()
            try:
                result = auditor.run(
                    timeout_seconds=args.target_ms / 1000,
                    poll_interval_seconds=args.poll_ms / 1000,
                    event=event,
                )
                samples.append(result.as_dict())
            except Exception as exc:
                samples.append({
                    "transaction_id": event["transaction"]["transaction_id"],
                    "target_ms": args.target_ms,
                    "passed": False,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                })
    except Exception as exc:
        print(json.dumps({
            "generated_at": generated_at, "passed": False, "stage": "setup",
            "error_type": type(exc).__name__, "error": str(exc),
        }, sort_keys=True))
        return 1
    finally:
        auditor.close()
    passed = all(sample["passed"] for sample in samples)
    measured = [sample["latency_ms"] for sample in samples if "latency_ms" in sample]
    report = {
        "generated_at": generated_at,
        "passed": passed,
        "target_ms": args.target_ms,
        "sample_count": len(samples),
        "passed_count": sum(bool(sample["passed"]) for sample in samples),
        "maximum_latency_ms": max(measured) if measured else None,
        "samples": samples,
    }
    # Preserve the original top-level single-probe fields for existing callers.
    if args.runs == 1:
        report.update(samples[0])
    print(json.dumps(report, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
