import json
import unittest
from unittest.mock import Mock

from fingraph.flink_consumer import (
    Neo4jTransactionWriter,
    StreamSettings,
    dead_letter_record,
    decode_and_normalise,
)
from fingraph.stream_contract import EventValidationError


def valid_event():
    account = {
        "account_id": "acct-1", "person_id": "person-1", "bank_id": "bank-1",
        "country": "us", "account_type": "checking", "risk_tier": "low",
        "person_name": "A", "person_country": "us", "person_type": "individual",
        "bank_name": "Bank", "bank_country": "us",
    }
    destination = dict(account, account_id="acct-2", person_id="person-2")
    return {
        "event_type": "transaction.created", "event_version": 1,
        "transaction": {
            "transaction_id": "tx-1", "source_account_id": "acct-1",
            "destination_account_id": "acct-2", "amount": "12.30", "currency": "usd",
            "occurred_at": "2026-08-13T10:00:00+05:30", "origin_ip": "192.0.2.1",
            "channel": "web", "syndicate_id": None, "risk_indicators": [],
        },
        "source_account": account, "destination_account": destination,
    }


class FlinkConsumerTests(unittest.TestCase):
    def test_decode_and_normalise_returns_graph_safe_event(self):
        result = decode_and_normalise(json.dumps(valid_event()).encode())
        self.assertEqual(result["transaction"]["currency"], "USD")
        self.assertEqual(result["transaction"]["occurred_at"], "2026-08-13T04:30:00Z")

    def test_invalid_json_becomes_validation_error(self):
        with self.assertRaisesRegex(EventValidationError, "UTF-8 JSON object"):
            decode_and_normalise("not-json")

    def test_dead_letter_record_preserves_failure(self):
        result = json.loads(dead_letter_record(b"bad\xff", EventValidationError("broken")))
        self.assertEqual(result["error_type"], "EventValidationError")
        self.assertEqual(result["error"], "broken")
        self.assertIn("bad", result["raw_event"])

    def test_settings_read_environment(self):
        with unittest.mock.patch.dict("os.environ", {"KAFKA_CONSUMER_GROUP": "test-group"}):
            settings = StreamSettings.from_environment()
            self.assertEqual(settings.consumer_group, "test-group")
            self.assertEqual(settings.kafka_bootstrap_servers, "localhost:9092")

    def test_writer_executes_idempotent_query_with_event_parameter(self):
        writer = Neo4jTransactionWriter("bolt://test", "neo4j", "secret", "MERGE ($event)")
        writer._driver = Mock()
        event = valid_event()
        writer.write(event)
        writer._driver.execute_query.assert_called_once_with(
            "MERGE ($event)", event=event, database_="neo4j"
        )


if __name__ == "__main__":
    unittest.main()
