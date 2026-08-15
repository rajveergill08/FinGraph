from __future__ import annotations

from datetime import datetime, timezone
import unittest
from unittest.mock import Mock

from fingraph.pipeline_audit import PipelineAuditor, build_audit_event


class PipelineAuditTests(unittest.TestCase):
    def test_builds_a_unique_simulator_event(self):
        event = build_audit_event(
            transaction_id="audit-test-001",
            occurred_at=datetime(2026, 8, 15, tzinfo=timezone.utc),
        )

        self.assertEqual(event["transaction"]["transaction_id"], "audit-test-001")
        self.assertEqual(event["event_type"], "transaction.created")

    def test_reports_latency_when_edge_is_visible(self):
        producer = Mock()
        producer.send.return_value.get.return_value = None
        driver = Mock()
        driver.execute_query.return_value.records = [Mock()]
        clock = Mock(side_effect=[10.0, 10.2])
        auditor = PipelineAuditor(producer=producer, driver=driver, clock=clock)

        result = auditor.run(event=build_audit_event(transaction_id="audit-visible"))

        self.assertEqual(result.latency_ms, 200.0)
        self.assertTrue(result.passed)
        producer.send.return_value.get.assert_called_once_with(timeout=1.0)

    def test_fails_when_edge_misses_target(self):
        producer = Mock()
        producer.send.return_value.get.return_value = None
        driver = Mock()
        driver.execute_query.return_value.records = []
        clock = Mock(side_effect=[20.0, 21.1])
        auditor = PipelineAuditor(
            producer=producer,
            driver=driver,
            clock=clock,
            sleep=Mock(),
        )

        with self.assertRaisesRegex(TimeoutError, "not visible"):
            auditor.run(event=build_audit_event(transaction_id="audit-timeout"))


if __name__ == "__main__":
    unittest.main()
