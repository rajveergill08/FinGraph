"""Kafka publication boundary for FinGraph transaction events."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from typing import Iterable, Protocol


class KafkaPublishError(RuntimeError):
    """Raised when the configured Kafka client cannot publish an event batch."""


class ProducerProtocol(Protocol):
    def send(self, topic: str, value: bytes, key: bytes): ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    bootstrap_servers: str = "localhost:9092"
    transaction_topic: str = "fingraph.transactions.v1"
    client_id: str = "fingraph-simulator"

    @classmethod
    def from_environment(cls) -> "KafkaSettings":
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
            transaction_topic=os.getenv("KAFKA_TRANSACTION_TOPIC", cls.transaction_topic),
            client_id=os.getenv("KAFKA_CLIENT_ID", cls.client_id),
        )


class KafkaTransactionPublisher:
    """Serialise versioned transaction events and wait for Kafka acknowledgement."""

    def __init__(
        self,
        settings: KafkaSettings | None = None,
        *,
        producer: ProducerProtocol | None = None,
    ) -> None:
        self.settings = settings or KafkaSettings.from_environment()
        self._producer = producer
        self._owns_producer = producer is None

    def publish(self, events: Iterable[dict[str, object]]) -> int:
        producer = self._get_producer()
        published = 0
        try:
            for event in events:
                transaction = event.get("transaction")
                if not isinstance(transaction, dict):
                    raise KafkaPublishError("Event is missing its transaction payload.")
                transaction_id = transaction.get("transaction_id")
                if not isinstance(transaction_id, str) or not transaction_id:
                    raise KafkaPublishError("Event transaction_id must be a non-empty string.")
                acknowledgement = producer.send(
                    self.settings.transaction_topic,
                    key=transaction_id.encode("utf-8"),
                    value=json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8"),
                )
                get_result = getattr(acknowledgement, "get", None)
                if callable(get_result):
                    get_result(timeout=10)
                published += 1
            producer.flush()
            return published
        except KafkaPublishError:
            raise
        except Exception as exc:  # Library-specific exception types vary by client release.
            raise KafkaPublishError(
                f"Unable to publish FinGraph transactions to {self.settings.bootstrap_servers}."
            ) from exc
        finally:
            if self._owns_producer:
                producer.close()

    def _get_producer(self) -> ProducerProtocol:
        if self._producer is not None:
            return self._producer
        try:
            from kafka import KafkaProducer
        except ImportError as exc:
            raise KafkaPublishError(
                "Kafka client dependency is missing. Install the project dependencies first."
            ) from exc
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self.settings.bootstrap_servers.split(","),
                client_id=self.settings.client_id,
                acks="all",
                retries=3,
                request_timeout_ms=10_000,
            )
        except Exception as exc:
            raise KafkaPublishError(
                f"Kafka is not reachable at {self.settings.bootstrap_servers}."
            ) from exc
        return self._producer
