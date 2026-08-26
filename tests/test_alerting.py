from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, Mock

from fingraph.alerting import (
    AlertEngine,
    EmailSink,
    HighRiskAccountRepository,
    JsonAlertStateStore,
    RiskAlert,
    SlackWebhookSink,
    format_alert_message,
    load_query,
)


class _Record:
    def __init__(self, data):
        self._data = data

    def data(self):
        return self._data


def _alert(*, score: float = 87.5) -> RiskAlert:
    return RiskAlert(
        account_id="account-shell-001",
        graph_risk_score=score,
        risk_tier="critical",
        country="PA",
        pagerank_score=0.98,
        community_id=4,
        transaction_count=55,
        counterparty_count=5,
        latest_transfer_at="2026-08-26T09:00:00Z",
    )


class _Sink:
    def __init__(self, name: str = "test"):
        self.name = name
        self.alerts = []

    def send(self, alert):
        self.alerts.append(alert)


class AlertingTests(unittest.TestCase):
    def test_candidate_query_is_bounded_parameterised_and_read_only(self):
        query = load_query()

        self.assertIn("$minimum_risk_score", query)
        self.assertIn("LIMIT $limit", query)
        self.assertIn("graph_risk_score", query)
        self.assertNotIn("CREATE ", query.upper())
        self.assertNotIn(" SET ", query.upper())
        self.assertNotIn("DELETE ", query.upper())

    def test_repository_returns_typed_candidates_as_read_traffic(self):
        driver = Mock()
        driver.execute_query.return_value.records = [
            _Record(
                {
                    "account_id": "account-shell-001",
                    "graph_risk_score": 87.5,
                    "risk_tier": "critical",
                    "country": "PA",
                    "pagerank_score": 0.98,
                    "community_id": 4,
                    "transaction_count": 55,
                    "counterparty_count": 5,
                    "latest_transfer_at": "2026-08-26T09:00:00Z",
                }
            )
        ]
        repository = HighRiskAccountRepository(driver=driver)

        candidates = repository.find_candidates(minimum_risk_score=75.0, limit=25)

        self.assertEqual(candidates, [_alert()])
        call = driver.execute_query.call_args
        self.assertEqual(
            call.kwargs["parameters_"],
            {"minimum_risk_score": 75.0, "limit": 25},
        )
        self.assertEqual(call.kwargs["routing_"], "r")
        self.assertEqual(call.kwargs["database_"], "neo4j")

    def test_repository_rejects_invalid_bounds(self):
        repository = HighRiskAccountRepository(driver=Mock())
        with self.assertRaises(ValueError):
            repository.find_candidates(minimum_risk_score=101)
        with self.assertRaises(ValueError):
            repository.find_candidates(limit=0)

    def test_dry_run_never_sends_or_creates_delivery_state(self):
        repository = Mock(spec=HighRiskAccountRepository)
        repository.find_candidates.return_value = [_alert()]
        sink = _Sink()
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            summary = AlertEngine(
                repository,
                [sink],
                JsonAlertStateStore(state_path),
            ).run(threshold=70.0, limit=100, cooldown_hours=24, dry_run=True)

            self.assertEqual(summary.candidate_count, 1)
            self.assertEqual(summary.delivered_count, 0)
            self.assertEqual(sink.alerts, [])
            self.assertFalse(state_path.exists())

    def test_live_run_requires_a_delivery_channel(self):
        repository = Mock(spec=HighRiskAccountRepository)
        with tempfile.TemporaryDirectory() as directory:
            engine = AlertEngine(
                repository,
                [],
                JsonAlertStateStore(Path(directory) / "state.json"),
            )

            with self.assertRaisesRegex(ValueError, "No alert channel"):
                engine.run(threshold=70, limit=100, cooldown_hours=24)

        repository.find_candidates.assert_not_called()

    def test_delivery_is_suppressed_per_channel_until_cooldown_expires(self):
        repository = Mock(spec=HighRiskAccountRepository)
        repository.find_candidates.return_value = [_alert()]
        slack = _Sink("slack")
        email = _Sink("email")
        first_run = datetime(2026, 8, 26, 10, 0, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            clock = Mock(return_value=first_run)
            engine = AlertEngine(
                repository,
                [slack, email],
                JsonAlertStateStore(state_path),
                now=clock,
            )

            first = engine.run(threshold=70, limit=100, cooldown_hours=24)
            second = engine.run(threshold=70, limit=100, cooldown_hours=24)
            clock.return_value = first_run + timedelta(hours=25)
            third = engine.run(threshold=70, limit=100, cooldown_hours=24)

            self.assertEqual(first.delivered_count, 2)
            self.assertEqual(second.suppressed_count, 2)
            self.assertEqual(third.delivered_count, 2)
            self.assertEqual(len(slack.alerts), 2)
            self.assertEqual(len(email.alerts), 2)
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(len(payload["deliveries"]), 2)

    def test_failed_channel_is_reported_and_not_marked_delivered(self):
        repository = Mock(spec=HighRiskAccountRepository)
        repository.find_candidates.return_value = [_alert()]
        failing_sink = Mock(name="slack")
        failing_sink.name = "slack"
        failing_sink.send.side_effect = RuntimeError("temporary failure")

        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            summary = AlertEngine(
                repository,
                [failing_sink],
                JsonAlertStateStore(state_path),
            ).run(threshold=70, limit=100, cooldown_hours=24)

            self.assertEqual(summary.delivered_count, 0)
            self.assertEqual(summary.errors[0]["channel"], "slack")
            self.assertFalse(state_path.exists())

    def test_slack_sink_posts_json_without_exposing_the_webhook(self):
        post_json = Mock()
        sink = SlackWebhookSink(
            "https://hooks.slack.test/services/redacted",
            post_json=post_json,
        )

        sink.send(_alert())

        url, payload, timeout = post_json.call_args.args
        self.assertEqual(url, "https://hooks.slack.test/services/redacted")
        self.assertEqual(timeout, 10.0)
        self.assertIn("account-shell-001", json.loads(payload)["text"])

    def test_email_sink_uses_tls_login_and_sends_formatted_message(self):
        client = Mock()
        smtp_factory = MagicMock()
        smtp_factory.return_value.__enter__.return_value = client
        sink = EmailSink(
            host="smtp.test",
            port=587,
            sender="alerts@test",
            recipients=["analyst@test"],
            username="service-user",
            password="secret",
            smtp_factory=smtp_factory,
        )

        sink.send(_alert())

        client.starttls.assert_called_once_with()
        client.login.assert_called_once_with("service-user", "secret")
        message = client.send_message.call_args.args[0]
        self.assertIn("account-shell-001", message["Subject"])
        self.assertIn("87.50/100", message.get_content())

    def test_alert_message_contains_explainable_context(self):
        message = format_alert_message(_alert())

        self.assertIn("Graph risk score: 87.50/100", message)
        self.assertIn("Transactions: 55", message)
        self.assertIn("Counterparties: 5", message)


if __name__ == "__main__":
    unittest.main()
