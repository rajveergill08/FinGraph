from __future__ import annotations

import unittest

from fingraph.kafka_topic import KafkaTopicProvisioner


class _RecordingAdminClient:
    def __init__(self, topics: list[str] | None = None) -> None:
        self.topics = topics or []
        self.create_calls: list[tuple[list[object], int | None]] = []
        self.closed = False

    def list_topics(self) -> list[str]:
        return self.topics

    def create_topics(
        self,
        new_topics: list[object],
        timeout_ms: int | None = None,
        validate_only: bool = False,
    ) -> None:
        self.create_calls.append((new_topics, timeout_ms))

    def close(self) -> None:
        self.closed = True


class KafkaTopicProvisionerTests(unittest.TestCase):
    def test_existing_topic_is_not_created_again(self) -> None:
        admin = _RecordingAdminClient(["fingraph.transactions.v1"])
        provisioner = KafkaTopicProvisioner(
            bootstrap_servers="localhost:9092",
            topic="fingraph.transactions.v1",
            partitions=3,
            replication_factor=1,
            client_id="test-client",
            admin_client=admin,
        )

        result = provisioner.ensure_topic()

        self.assertFalse(result.created)
        self.assertEqual([], admin.create_calls)
        self.assertFalse(admin.closed, "Injected admin ownership remains with the caller.")

    def test_rejects_invalid_topic_configuration(self) -> None:
        with self.assertRaises(ValueError):
            KafkaTopicProvisioner(
                bootstrap_servers="localhost:9092",
                topic="fingraph.transactions.v1",
                partitions=0,
                replication_factor=1,
                client_id="test-client",
            )


if __name__ == "__main__":
    unittest.main()
