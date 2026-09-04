from __future__ import annotations

from datetime import datetime, timezone
from contextlib import redirect_stdout
import io
import json
import unittest
from unittest.mock import Mock, patch

from fingraph.pipeline_audit import AuditResult, PipelineAuditor, build_audit_event, main


def _graph_result(event=None, **overrides):
    result = Mock()
    result.records = []
    if event is not None:
        transaction = event["transaction"]
        edge = {field: transaction[field] for field in (
            "transaction_id", "source_account_id", "destination_account_id", "currency",
        )}
        edge["amount"] = float(transaction["amount"])
        edge.update(overrides)
        record = Mock()
        record.data.return_value = edge
        result.records = [record]
    return result


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
        event = build_audit_event(transaction_id="audit-visible")
        driver.execute_query.side_effect = [_graph_result(), _graph_result(event)]
        clock = Mock(side_effect=[10.0, 10.2])
        auditor = PipelineAuditor(producer=producer, driver=driver, clock=clock)

        result = auditor.run(event=event)

        self.assertEqual(result.latency_ms, 200.0)
        self.assertTrue(result.passed)
        self.assertEqual(result.edge["source_account_id"], event["source_account"]["account_id"])
        self.assertEqual(result.edge["amount"], float(event["transaction"]["amount"]))
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

    def test_existing_probe_is_rejected_before_publish(self):
        event = build_audit_event(transaction_id="audit-reused")
        driver = Mock()
        driver.execute_query.return_value = _graph_result(event)
        producer = Mock()
        auditor = PipelineAuditor(driver=driver, producer=producer)

        with self.assertRaisesRegex(ValueError, "already exists"):
            auditor.run(event=event)

        producer.send.assert_not_called()

    def test_compares_canonical_graph_values_but_publishes_original_event(self):
        event = build_audit_event()
        event["transaction"]["currency"] = "usd"
        driver = Mock()
        driver.execute_query.side_effect = [
            _graph_result(), _graph_result(event, currency="USD"),
        ]
        producer = Mock()
        auditor = PipelineAuditor(
            driver=driver, producer=producer, clock=Mock(side_effect=[0, 0.1]),
        )

        result = auditor.run(event=event)

        self.assertTrue(result.passed)
        self.assertEqual(result.edge["currency"], "USD")
        payload = json.loads(producer.send.call_args.kwargs["value"])
        self.assertEqual(payload["transaction"]["currency"], "usd")

    def test_graph_must_match_published_endpoints_amount_and_currency(self):
        event = build_audit_event(transaction_id="audit-mismatch")
        for field, value in (
            ("source_account_id", "wrong-source"),
            ("destination_account_id", "wrong-destination"),
            ("amount", 1.0), ("currency", "EUR"),
        ):
            with self.subTest(field=field):
                driver = Mock()
                driver.execute_query.side_effect = [
                    _graph_result(), _graph_result(event, **{field: value}),
                ]
                auditor = PipelineAuditor(
                    driver=driver, producer=Mock(), clock=Mock(side_effect=[0, 0.1]),
                )
                with self.assertRaisesRegex(RuntimeError, "unexpected graph data"):
                    auditor.run(event=event)

    def test_duplicate_edges_are_not_a_successful_ingestion_proof(self):
        event = build_audit_event()
        duplicate = _graph_result(event)
        duplicate.records *= 2
        driver = Mock()
        driver.execute_query.side_effect = [_graph_result(), duplicate]
        auditor = PipelineAuditor(
            driver=driver, producer=Mock(), clock=Mock(side_effect=[0, 0.1]),
        )
        with self.assertRaisesRegex(RuntimeError, "unexpected graph data"):
            auditor.run(event=event)

    def test_late_visible_edge_fails_target(self):
        event = build_audit_event()
        driver = Mock()
        driver.execute_query.side_effect = [_graph_result(), _graph_result(event)]
        auditor = PipelineAuditor(
            driver=driver, producer=Mock(), clock=Mock(side_effect=[0, 1.1]),
        )

        result = auditor.run(event=event)

        self.assertEqual(result.latency_ms, 1100.0)
        self.assertFalse(result.passed)

    def test_invalid_bounds_or_event_never_publish(self):
        producer = Mock()
        auditor = PipelineAuditor(driver=Mock(), producer=producer)
        for value in (0, -1, float("nan"), float("inf")):
            with self.subTest(value=value), self.assertRaises(ValueError):
                auditor.run(timeout_seconds=value)
            with self.subTest(poll=value), self.assertRaises(ValueError):
                auditor.run(poll_interval_seconds=value)
        with self.assertRaises(ValueError):
            auditor.run(event={})
        producer.send.assert_not_called()

    def test_cli_returns_failure_for_late_visible_edge(self):
        result = AuditResult("audit-late", 1000.0, 1000.0, {})
        output = io.StringIO()
        with patch("fingraph.pipeline_audit.PipelineAuditor") as factory:
            factory.return_value.run.return_value = result
            with redirect_stdout(output):
                exit_code = main([])

        self.assertEqual(exit_code, 1)
        report = json.loads(output.getvalue())
        self.assertFalse(report["passed"])
        self.assertEqual(report["latency_ms"], 1000.0)

    def test_cli_preserves_failed_samples_and_uses_unique_probe_ids(self):
        output = io.StringIO()
        with patch("fingraph.pipeline_audit.PipelineAuditor") as factory:
            auditor = factory.return_value
            auditor.run.side_effect = [
                TimeoutError("not visible"), AuditResult("audit-fast", 200.0, 1000.0, {}),
            ]
            with redirect_stdout(output):
                exit_code = main(["--runs", "2"])
            probe_ids = [
                call.kwargs["event"]["transaction"]["transaction_id"]
                for call in auditor.run.call_args_list
            ]
            auditor.close.assert_called_once_with()

        report = json.loads(output.getvalue())
        self.assertEqual(exit_code, 1)
        self.assertEqual(len(set(probe_ids)), 2)
        self.assertEqual(report["sample_count"], 2)
        self.assertEqual(report["passed_count"], 1)
        self.assertEqual(report["samples"][0]["transaction_id"], probe_ids[0])
        self.assertEqual(report["samples"][0]["error_type"], "TimeoutError")
        self.assertEqual(report["maximum_latency_ms"], 200.0)

    def test_cli_reports_setup_failure_and_closes_resources(self):
        output = io.StringIO()
        with patch("fingraph.pipeline_audit.PipelineAuditor") as factory:
            factory.return_value.open.side_effect = ConnectionError("unreachable")
            with redirect_stdout(output):
                exit_code = main([])
            factory.return_value.close.assert_called_once_with()
            factory.return_value.run.assert_not_called()

        self.assertEqual(exit_code, 1)
        self.assertEqual(json.loads(output.getvalue())["stage"], "setup")

    def test_cli_success_and_sample_count_bounds(self):
        with patch("fingraph.pipeline_audit.PipelineAuditor") as factory:
            factory.return_value.run.return_value = AuditResult("audit-ok", 200, 1000, {})
            with redirect_stdout(io.StringIO()):
                self.assertEqual(main([]), 0)
            for value in ("0", "21"):
                with self.assertRaises(SystemExit):
                    main(["--runs", value])


if __name__ == "__main__":
    unittest.main()
