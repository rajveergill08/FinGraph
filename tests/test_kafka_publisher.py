from __future__ import annotations

import json
import os
import unittest
from unittest.mock import patch

from fingraph.kafka_publisher import KafkaSettings, KafkaTransactionPublisher


class _Acknowledgement:
    def __init__(self) -> None:
        self.waited_for = None

    def get(self, timeout: int) -> None:
        self.waited_for = timeout


class _RecordingProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, bytes]] = []
        self.flushed = False
        self.closed = False

    def send(self, topic: str, *, key: bytes, value: bytes) -> _Acknowledgement:
        self.messages.append((topic, key, value))
        return _Acknowledgement()

    def flush(self) -> None:
        self.flushed = True

    def close(self) -> None:
        self.closed = True


class _NoopTopicProvisioner:
    def __init__(self) -> None:
        self.calls = 0

    def ensure_topic(self) -> None:
        self.calls += 1


class KafkaTransactionPublisherTests(unittest.TestCase):
    def test_settings_reads_environment_overrides(self) -> None:
        overrides = {
            "KAFKA_BOOTSTRAP_SERVERS": "kafka.example:19092",
            "KAFKA_TRANSACTION_TOPIC": "fingraph.test.v1",
            "KAFKA_CLIENT_ID": "test-simulator",
            "KAFKA_TRANSACTION_TOPIC_PARTITIONS": "6",
            "KAFKA_TRANSACTION_TOPIC_REPLICATION_FACTOR": "2",
        }
        with patch.dict(os.environ, overrides, clear=False):
            settings = KafkaSettings.from_environment()

        self.assertEqual("kafka.example:19092", settings.bootstrap_servers)
        self.assertEqual("fingraph.test.v1", settings.transaction_topic)
        self.assertEqual("test-simulator", settings.client_id)
        self.assertEqual(6, settings.transaction_topic_partitions)
        self.assertEqual(2, settings.transaction_topic_replication_factor)

    def test_publisher_sends_stable_transaction_key_and_json(self) -> None:
        producer = _RecordingProducer()
        topic_provisioner = _NoopTopicProvisioner()
        publisher = KafkaTransactionPublisher(
            KafkaSettings(transaction_topic="test.transactions"),
            producer=producer,
            topic_provisioner=topic_provisioner,
        )
        event = {
            "event_type": "transaction.created",
            "event_version": 1,
            "transaction": {"transaction_id": "tx-0001"},
        }

        self.assertEqual(1, publisher.publish([event]))
        self.assertEqual(1, topic_provisioner.calls)
        self.assertTrue(producer.flushed)
        self.assertFalse(producer.closed, "Injected producer ownership stays with the caller.")
        topic, key, payload = producer.messages[0]
        self.assertEqual("test.transactions", topic)
        self.assertEqual(b"tx-0001", key)
        self.assertEqual(event, json.loads(payload.decode("utf-8")))


if __name__ == "__main__":
    unittest.main()
