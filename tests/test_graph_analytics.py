from __future__ import annotations

import unittest
from unittest.mock import Mock

from fingraph.graph_analytics import GraphAnalyticsRunner, load_query


class _Record:
    def __init__(self, data):
        self._data = data

    def data(self):
        return self._data


class GraphAnalyticsTests(unittest.TestCase):
    def test_circular_flow_query_is_bounded_and_deduplicated(self):
        query = load_query("circular_flows")
        self.assertIn("(a:Account)-[ab:TRANSFERRED_TO]->(b:Account)", query)
        self.assertIn("-[ca:TRANSFERRED_TO]->(a)", query)
        self.assertIn("a.account_id < b.account_id", query)
        self.assertIn("LIMIT $limit", query)

    def test_risk_query_persists_explainable_capped_score(self):
        query = load_query("risk_scores")
        self.assertIn("account.graph_risk_score = risk_score", query)
        self.assertIn("raw_score > 100.0", query)
        self.assertIn("score_components", query)

    def test_runner_passes_safe_parameters_and_returns_record_data(self):
        driver = Mock()
        driver.execute_query.return_value.records = [_Record({"account_ids": ["a", "b", "c"]})]
        runner = GraphAnalyticsRunner(driver=driver)

        result = runner.circular_flows(lookback_hours=12, minimum_amount=50.0, limit=25)

        self.assertEqual(result, [{"account_ids": ["a", "b", "c"]}])
        _, kwargs = driver.execute_query.call_args
        self.assertEqual(
            kwargs["parameters_"],
            {"lookback_hours": 12, "minimum_amount": 50.0, "limit": 25},
        )
        self.assertEqual(kwargs["database_"], "neo4j")

    def test_risk_parameters_are_normalised(self):
        driver = Mock()
        driver.execute_query.return_value.records = []
        runner = GraphAnalyticsRunner(driver=driver)

        runner.refresh_risk_scores(high_risk_countries=["ky", "IR", "ky"])

        parameters = driver.execute_query.call_args.kwargs["parameters_"]
        self.assertEqual(parameters["high_risk_countries"], ["IR", "KY"])

    def test_rejects_unbounded_or_invalid_parameters(self):
        runner = GraphAnalyticsRunner(driver=Mock())
        with self.assertRaises(ValueError):
            runner.circular_flows(limit=0)
        with self.assertRaises(ValueError):
            runner.refresh_risk_scores(high_risk_countries=["INVALID"])
        with self.assertRaises(ValueError):
            runner.refresh_risk_scores(volume_unit=0)


if __name__ == "__main__":
    unittest.main()
