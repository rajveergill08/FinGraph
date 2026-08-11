"""Explicit Kafka topic provisioning for FinGraph's transaction stream."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class KafkaTopicError(RuntimeError):
    """Raised when the FinGraph transaction topic cannot be inspected or created."""


class KafkaAdminProtocol(Protocol):
    def list_topics(self) -> list[str]: ...

    def create_topics(
        self,
        new_topics: list[object],
        timeout_ms: int | None = None,
        validate_only: bool = False,
    ) -> object: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class TopicProvisioningResult:
    topic: str
    created: bool
    partitions: int
    replication_factor: int


class KafkaTopicProvisioner:
    """Create the transaction topic explicitly instead of relying on broker defaults."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        partitions: int,
        replication_factor: int,
        client_id: str,
        admin_client: KafkaAdminProtocol | None = None,
    ) -> None:
        if partitions < 1:
            raise ValueError("Kafka topic partitions must be at least one.")
        if replication_factor < 1:
            raise ValueError("Kafka topic replication factor must be at least one.")
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.partitions = partitions
        self.replication_factor = replication_factor
        self.client_id = client_id
        self._admin_client = admin_client
        self._owns_admin_client = admin_client is None

    def ensure_topic(self) -> TopicProvisioningResult:
        admin_client = self._get_admin_client()
        try:
            if self.topic in set(admin_client.list_topics()):
                return self._result(created=False)
            try:
                from kafka.admin import NewTopic
                from kafka.errors import TopicAlreadyExistsError

                admin_client.create_topics(
                    [
                        NewTopic(
                            name=self.topic,
                            num_partitions=self.partitions,
                            replication_factor=self.replication_factor,
                        )
                    ],
                    timeout_ms=10_000,
                )
            except TopicAlreadyExistsError:
                return self._result(created=False)
            return self._result(created=True)
        except KafkaTopicError:
            raise
        except Exception as exc:  # The Kafka client's error classes differ by broker/client version.
            raise KafkaTopicError(
                f"Unable to provision topic '{self.topic}' at {self.bootstrap_servers}."
            ) from exc
        finally:
            if self._owns_admin_client:
                admin_client.close()

    def _get_admin_client(self) -> KafkaAdminProtocol:
        if self._admin_client is not None:
            return self._admin_client
        try:
            from kafka.admin import KafkaAdminClient
        except ImportError as exc:
            raise KafkaTopicError(
                "Kafka client dependency is missing. Install the project dependencies first."
            ) from exc
        try:
            self._admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers.split(","),
                client_id=f"{self.client_id}-admin",
                request_timeout_ms=10_000,
                api_version_auto_timeout_ms=10_000,
            )
        except Exception as exc:
            raise KafkaTopicError(
                f"Kafka is not reachable at {self.bootstrap_servers}."
            ) from exc
        return self._admin_client

    def _result(self, *, created: bool) -> TopicProvisioningResult:
        return TopicProvisioningResult(
            topic=self.topic,
            created=created,
            partitions=self.partitions,
            replication_factor=self.replication_factor,
        )
