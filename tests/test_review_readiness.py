from __future__ import annotations

import json
import unittest
from unittest.mock import Mock

from fingraph.review_readiness import (
    ReviewReadinessAuditor,
    ReviewSettings,
    load_review_query,
)


class _Record:
    def __init__(self, data):
        self._data = data

    def data(self):
        return self._data


def _result(*records):
    result = Mock()
    result.records = [_Record(record) for record in records]
    return result


class ReviewReadinessTests(unittest.TestCase):
    def test_query_catalog_uses_bounded_multi_hop_queries(self):
        circular = load_review_query("circular_flows")
        starbursts = load_review_query("starbursts")

        self.assertIn("-[bc:TRANSFERRED_TO]->", circular)
        self.assertIn("LIMIT $limit", circular)
        self.assertIn("-[outbound:TRANSFERRED_TO]->", starbursts)
        self.assertIn("LIMIT $limit", starbursts)

    def test_complete_graph_and_fast_queries_pass_without_page_checks(self):
        inventory = {
            "accounts": 12,
            "transfers": 20,
            "risk_scored": 12,
            "community_scored": 12,
            "centrality_scored": 12,
        }
        driver = Mock()
        driver.execute_query.side_effect = [
            _result(inventory),
            _result(
                {
                    "names": [
                        "account_id_unique",
                        "bank_id_unique",
                        "containment_case_id_unique",
                        "person_id_unique",
                        "transaction_id_unique",
                    ]
                }
            ),
            _result(
                {
                    "names": [
                        "account_country",
                        "account_graph_risk_score",
                        "account_louvain_community_id",
                        "account_pagerank_score",
                        "account_risk_tier",
                        "transfer_occurred_at",
                    ]
                }
            ),
            _result(inventory),
            _result({"account_ids": ["a", "b", "c"]}),
            _result({"account_ids": ["a", "b", "c"]}),
            _result({"sink_account_id": "sink"}),
            _result({"sink_account_id": "sink"}),
        ]
        clock = Mock(side_effect=[1.0, 1.04, 2.0, 2.06])
        auditor = ReviewReadinessAuditor(driver=driver, clock=clock)

        report = auditor.run(query_runs=1, include_pages=False)

        self.assertTrue(report.passed)
        self.assertEqual(len(report.checks), 5)
        self.assertEqual(
            report.checks[3].details,
            {
                "maximum_ms": 40.0,
                "result_count": 1,
                "run_ms": [40.0],
                "target_ms": 100.0,
            },
        )

    def test_missing_schema_and_incomplete_analytics_are_reported(self):
        inventory = {
            "accounts": 4,
            "transfers": 3,
            "risk_scored": 4,
            "community_scored": 3,
            "centrality_scored": 0,
        }
        driver = Mock()
        driver.execute_query.side_effect = [
            _result(inventory),
            _result({"names": ["account_id_unique"]}),
            _result({"names": ["account_country"]}),
            _result(inventory),
            _result(),
            _result(),
            _result(),
            _result(),
        ]
        auditor = ReviewReadinessAuditor(
            driver=driver,
            clock=Mock(side_effect=[0.0, 0.01, 1.0, 1.01]),
        )

        report = auditor.run(query_runs=1, include_pages=False)

        self.assertFalse(report.passed)
        self.assertFalse(report.checks[1].passed)
        self.assertIn(
            "transaction_id_unique",
            report.checks[1].details["missing_constraints"],
        )
        self.assertFalse(report.checks[2].passed)
        self.assertEqual(report.checks[2].details["centrality_scored"], 0)

    def test_slowest_warmed_query_run_must_be_under_target(self):
        driver = Mock()
        driver.execute_query.return_value = _result()
        auditor = ReviewReadinessAuditor(
            driver=driver,
            clock=Mock(side_effect=[0.0, 0.04, 1.0, 1.12]),
        )

        passed, details = auditor._check_query_latency(
            "circular_flows", {}, 100.0, 2
        )

        self.assertFalse(passed)
        self.assertEqual(details["maximum_ms"], 120.0)
        self.assertEqual(driver.execute_query.call_count, 3)

    def test_review_pages_include_health_contract(self):
        def fetcher(url, _timeout):
            if url.endswith("/health"):
                return 200, json.dumps({"status": "ok"})
            return 200, "ready"

        auditor = ReviewReadinessAuditor(driver=Mock(), url_fetcher=fetcher)

        passed, details = auditor._check_review_pages(1.0)

        self.assertTrue(passed)
        self.assertEqual(
            set(details["pages"]),
            {
                "dashboard",
                "neo4j_browser",
                "api_docs",
                "api_health",
            },
        )

    def test_invalid_run_bounds_are_rejected(self):
        auditor = ReviewReadinessAuditor(driver=Mock())
        with self.assertRaises(ValueError):
            auditor.run(query_target_ms=0, include_pages=False)
        with self.assertRaises(ValueError):
            auditor.run(query_runs=21, include_pages=False)
        with self.assertRaises(ValueError):
            auditor.run(lookback_hours=0, include_pages=False)

    def test_open_verifies_injected_driver(self):
        driver = Mock()
        auditor = ReviewReadinessAuditor(
            settings=ReviewSettings(neo4j_database="review"), driver=driver
        )

        auditor.open()

        driver.verify_connectivity.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
