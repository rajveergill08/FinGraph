from __future__ import annotations

import json
import unittest
from unittest.mock import Mock, patch

from fingraph.gds_analytics import GdsCommunityRunner, load_query, main


class _Record:
    def __init__(self, data):
        self._data = data

    def data(self):
        return self._data


def _result(*records):
    result = Mock()
    result.records = [_Record(record) for record in records]
    return result


class GdsCommunityTests(unittest.TestCase):
    def test_projection_is_weighted_undirected_and_aggregated(self):
        query = load_query("project_account_network")

        self.assertIn("'Account'", query)
        self.assertIn("TRANSFERRED_TO", query)
        self.assertIn("orientation: 'UNDIRECTED'", query)
        self.assertIn("property: 'amount'", query)
        self.assertIn("aggregation: 'SUM'", query)

    def test_louvain_writes_a_weighted_community_property(self):
        query = load_query("write_louvain_communities")

        self.assertIn("gds.louvain.write", query)
        self.assertIn("relationshipWeightProperty: 'transaction_volume'", query)
        self.assertIn("writeProperty: $write_property", query)

    def test_runner_returns_metrics_and_always_drops_projection(self):
        driver = Mock()
        driver.execute_query.side_effect = [
            _result({"gds_version": "2.13.0"}),
            _result(),
            _result(
                {
                    "graphName": "fingraph-account-network",
                    "nodeCount": 6,
                    "relationshipCount": 10,
                    "projectMillis": 4,
                }
            ),
            _result(
                {
                    "communityCount": 2,
                    "modularity": 0.42,
                    "modularities": [0.2, 0.42],
                    "ranLevels": 2,
                    "nodePropertiesWritten": 6,
                    "computeMillis": 3,
                    "writeMillis": 1,
                }
            ),
            _result(
                {
                    "community_id": 7,
                    "member_count": 4,
                    "sample_account_ids": ["a", "b", "c", "d"],
                    "highest_risk_score": 88.0,
                    "average_risk_score": 41.5,
                }
            ),
            _result({"graphName": "fingraph-account-network"}),
        ]
        runner = GdsCommunityRunner(driver=driver)

        result = runner.refresh_communities(concurrency=2)

        self.assertEqual(result["gds_version"], "2.13.0")
        self.assertEqual(result["louvain"]["communityCount"], 2)
        self.assertEqual(result["communities"][0]["member_count"], 4)
        self.assertEqual(driver.execute_query.call_count, 6)
        last_query = driver.execute_query.call_args.args[0]
        last_kwargs = driver.execute_query.call_args.kwargs
        self.assertIn("gds.graph.drop", last_query)
        self.assertEqual(
            last_kwargs["parameters_"], {"graph_name": "fingraph-account-network"}
        )

    def test_empty_projection_is_rejected_and_cleaned_up(self):
        driver = Mock()
        driver.execute_query.side_effect = [
            _result({"gds_version": "2.13.0"}),
            _result(),
            _result(
                {
                    "graphName": "fingraph-account-network",
                    "nodeCount": 1,
                    "relationshipCount": 0,
                    "projectMillis": 1,
                }
            ),
            _result({"graphName": "fingraph-account-network"}),
        ]
        runner = GdsCommunityRunner(driver=driver)

        with self.assertRaisesRegex(RuntimeError, "at least two Account nodes"):
            runner.refresh_communities()

        self.assertEqual(driver.execute_query.call_count, 4)
        self.assertIn("gds.graph.drop", driver.execute_query.call_args.args[0])

    def test_invalid_or_community_edition_unsafe_parameters_are_rejected(self):
        runner = GdsCommunityRunner(driver=Mock())

        with self.assertRaises(ValueError):
            runner.refresh_communities(graph_name="invalid graph name")
        with self.assertRaises(ValueError):
            runner.refresh_communities(write_property="invalid-property")
        with self.assertRaises(ValueError):
            runner.refresh_communities(concurrency=5)
        with self.assertRaises(ValueError):
            runner.refresh_communities(community_limit=0)

    @patch("builtins.print")
    @patch("fingraph.gds_analytics.GdsCommunityRunner")
    def test_cli_prints_machine_readable_summary(self, runner_type, output):
        runner = runner_type.return_value
        runner.refresh_communities.return_value = {
            "gds_version": "2.13.0",
            "graph_name": "test-graph",
            "communities": [],
        }

        self.assertEqual(main(["--graph-name", "test-graph"]), 0)

        runner.open.assert_called_once_with()
        runner.close.assert_called_once_with()
        self.assertEqual(json.loads(output.call_args.args[0])["graph_name"], "test-graph")


if __name__ == "__main__":
    unittest.main()
