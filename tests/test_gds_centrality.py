from __future__ import annotations

import json
import unittest
from unittest.mock import Mock, patch

from fingraph.gds_centrality import GdsCentralityRunner, load_query, main


class _Record:
    def __init__(self, data):
        self._data = data

    def data(self):
        return self._data


def _result(*records):
    result = Mock()
    result.records = [_Record(record) for record in records]
    return result


class GdsCentralityTests(unittest.TestCase):
    def test_projection_preserves_direction_and_aggregates_transfer_volume(self):
        query = load_query("project_account_flow")

        self.assertIn("'Account'", query)
        self.assertIn("TRANSFERRED_TO", query)
        self.assertIn("orientation: 'NATURAL'", query)
        self.assertIn("property: 'amount'", query)
        self.assertIn("aggregation: 'SUM'", query)

    def test_pagerank_writes_normalized_weighted_scores(self):
        query = load_query("write_pagerank_scores")

        self.assertIn("gds.pageRank.write", query)
        self.assertIn("relationshipWeightProperty: 'transaction_volume'", query)
        self.assertIn("writeProperty: $write_property", query)
        self.assertIn("scaler: 'MinMax'", query)

    def test_runner_returns_metrics_and_always_drops_projection(self):
        driver = Mock()
        driver.execute_query.side_effect = [
            _result({"gds_version": "2.13.12"}),
            _result(),
            _result(
                {
                    "graphName": "fingraph-account-flow",
                    "nodeCount": 6,
                    "relationshipCount": 8,
                    "projectMillis": 4,
                }
            ),
            _result(
                {
                    "nodePropertiesWritten": 6,
                    "ranIterations": 12,
                    "didConverge": True,
                    "centralityDistribution": {"min": 0.0, "max": 1.0},
                    "computeMillis": 3,
                    "writeMillis": 1,
                }
            ),
            _result(
                {
                    "account_id": "shell-001",
                    "pagerank_score": 1.0,
                    "inbound_transfer_count": 5,
                }
            ),
            _result({"graphName": "fingraph-account-flow"}),
        ]
        runner = GdsCentralityRunner(driver=driver)

        result = runner.refresh_centrality(concurrency=2)

        self.assertEqual(result["gds_version"], "2.13.12")
        self.assertTrue(result["pagerank"]["didConverge"])
        self.assertEqual(
            result["highest_centrality_accounts"][0]["account_id"], "shell-001"
        )
        self.assertEqual(driver.execute_query.call_count, 6)
        self.assertIn("gds.graph.drop", driver.execute_query.call_args.args[0])
        self.assertEqual(
            driver.execute_query.call_args.kwargs["parameters_"],
            {"graph_name": "fingraph-account-flow"},
        )

    def test_empty_projection_is_rejected_and_cleaned_up(self):
        driver = Mock()
        driver.execute_query.side_effect = [
            _result({"gds_version": "2.13.12"}),
            _result(),
            _result(
                {
                    "graphName": "fingraph-account-flow",
                    "nodeCount": 1,
                    "relationshipCount": 0,
                    "projectMillis": 1,
                }
            ),
            _result({"graphName": "fingraph-account-flow"}),
        ]
        runner = GdsCentralityRunner(driver=driver)

        with self.assertRaisesRegex(RuntimeError, "at least two Account nodes"):
            runner.refresh_centrality()

        self.assertEqual(driver.execute_query.call_count, 4)
        self.assertIn("gds.graph.drop", driver.execute_query.call_args.args[0])

    def test_invalid_parameters_are_rejected(self):
        runner = GdsCentralityRunner(driver=Mock())

        with self.assertRaises(ValueError):
            runner.refresh_centrality(graph_name="invalid graph")
        with self.assertRaises(ValueError):
            runner.refresh_centrality(write_property="invalid-property")
        with self.assertRaises(ValueError):
            runner.refresh_centrality(damping_factor=1.0)
        with self.assertRaises(ValueError):
            runner.refresh_centrality(tolerance=0)
        with self.assertRaises(ValueError):
            runner.refresh_centrality(concurrency=5)
        with self.assertRaises(ValueError):
            runner.refresh_centrality(account_limit=0)

    @patch("builtins.print")
    @patch("fingraph.gds_centrality.GdsCentralityRunner")
    def test_cli_prints_machine_readable_summary(self, runner_type, output):
        runner = runner_type.return_value
        runner.refresh_centrality.return_value = {
            "gds_version": "2.13.12",
            "graph_name": "test-flow",
            "highest_centrality_accounts": [],
        }

        self.assertEqual(main(["--graph-name", "test-flow"]), 0)

        runner.open.assert_called_once_with()
        runner.close.assert_called_once_with()
        self.assertEqual(json.loads(output.call_args.args[0])["graph_name"], "test-flow")


if __name__ == "__main__":
    unittest.main()
