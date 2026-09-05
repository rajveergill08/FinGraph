import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock

from fingraph.flink_consumer import (
    Neo4jTransactionWriter,
    StreamSettings,
    _connector_jar_uri,
    dead_letter_record,
    decode_and_normalise,
    validate_and_route,
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

    def test_valid_event_routes_only_to_the_canonical_stream(self):
        valid, invalid = validate_and_route(json.dumps(valid_event()))

        self.assertIsNone(invalid)
        self.assertEqual(json.loads(valid)["transaction"]["currency"], "USD")

    def test_invalid_event_routes_only_to_the_dead_letter_stream(self):
        valid, invalid = validate_and_route('{"probe":"final-review-dlq"}')

        self.assertIsNone(valid)
        payload = json.loads(invalid)
        self.assertEqual(payload["error_type"], "EventValidationError")
        self.assertIn("final-review-dlq", payload["raw_event"])

    def test_settings_read_environment(self):
        with unittest.mock.patch.dict("os.environ", {"KAFKA_CONSUMER_GROUP": "test-group"}):
            settings = StreamSettings.from_environment()
            self.assertEqual(settings.consumer_group, "test-group")
            self.assertEqual(settings.kafka_bootstrap_servers, "localhost:9092")
            self.assertEqual(settings.python_execution_mode, "process")
            self.assertEqual(settings.parallelism, 1)
            self.assertEqual(settings.python_bundle_size, 50)
            self.assertEqual(settings.python_bundle_time_ms, 50)
            self.assertEqual(settings.checkpoint_interval_ms, 10_000)

    def test_checkpoint_interval_reads_positive_environment_value(self):
        with unittest.mock.patch.dict(
            "os.environ", {"FLINK_CHECKPOINT_INTERVAL_MS": "2500"}
        ):
            self.assertEqual(StreamSettings.from_environment().checkpoint_interval_ms, 2500)

        with unittest.mock.patch.dict(
            "os.environ", {"FLINK_CHECKPOINT_INTERVAL_MS": "0"}
        ):
            with self.assertRaisesRegex(ValueError, "positive integer"):
                StreamSettings.from_environment()

    def test_connector_jar_must_be_configured_and_exist(self):
        with self.assertRaisesRegex(RuntimeError, "FLINK_KAFKA_CONNECTOR_JAR"):
            _connector_jar_uri(None)
        with tempfile.TemporaryDirectory() as directory:
            jar = Path(directory) / "connector.jar"
            jar.touch()
            self.assertEqual(_connector_jar_uri(str(jar)), jar.resolve().as_uri())

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
