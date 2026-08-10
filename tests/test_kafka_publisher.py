from __future__ import annotations

import json
import unittest

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


class KafkaTransactionPublisherTests(unittest.TestCase):
    def test_publisher_sends_stable_transaction_key_and_json(self) -> None:
        producer = _RecordingProducer()
        publisher = KafkaTransactionPublisher(
            KafkaSettings(transaction_topic="test.transactions"), producer=producer
        )
        event = {
            "event_type": "transaction.created",
            "event_version": 1,
            "transaction": {"transaction_id": "tx-0001"},
        }

        self.assertEqual(1, publisher.publish([event]))
        self.assertTrue(producer.flushed)
        self.assertFalse(producer.closed, "Injected producer ownership stays with the caller.")
        topic, key, payload = producer.messages[0]
        self.assertEqual("test.transactions", topic)
        self.assertEqual(b"tx-0001", key)
        self.assertEqual(event, json.loads(payload.decode("utf-8")))


if __name__ == "__main__":
    unittest.main()
