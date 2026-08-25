from __future__ import annotations

from datetime import datetime, timezone
import os
import unittest
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from fingraph.dashboard_api import (
    DashboardGraphRepository,
    DashboardNode,
    GraphFilters,
    GraphSnapshot,
    StarburstFilters,
    StarburstPattern,
    StarburstSnapshot,
    allowed_origins_from_environment,
    create_app,
    load_query,
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


def _node(account_id: str, *, score: float = 0.0):
    return {
        "id": account_id,
        "label": account_id,
        "country": "US",
        "account_type": "checking",
        "risk_tier": "medium",
        "graph_risk_score": 40.0,
        "pagerank_score": score,
        "community_id": 7,
    }


def _edge(edge_id: str, source: str, target: str):
    return {
        "id": edge_id,
        "source": source,
        "target": target,
        "amount": 9900.0,
        "currency": "USD",
        "occurred_at": "2026-08-21T12:00:00Z",
        "channel": "wire",
        "syndicate_id": "syndicate-001",
        "risk_indicators": ["below_reporting_threshold"],
    }


def _snapshot() -> GraphSnapshot:
    return GraphSnapshot(
        generated_at=datetime.now(timezone.utc),
        nodes=[DashboardNode.model_validate(_node("account-a"))],
        edges=[],
        filters=GraphFilters(
            edge_limit=200,
            minimum_risk_score=0.0,
            minimum_pagerank_score=0.0,
            community_id=None,
        ),
    )


def _starburst_snapshot() -> StarburstSnapshot:
    return StarburstSnapshot(
        generated_at=datetime.now(timezone.utc),
        patterns=[
            StarburstPattern(
                id="starburst:account-shell-001",
                sink_account_id="account-shell-001",
                source_account_ids=[f"account-source-{index:03d}" for index in range(1, 11)],
                intermediary_account_ids=["account-intermediary-001", "account-intermediary-002"],
                source_count=10,
                intermediary_count=2,
                inbound_transfer_count=10,
                outbound_transfer_count=2,
                latest_transfer_at="2026-08-25T12:00:00Z",
            )
        ],
        filters=StarburstFilters(
            lookback_hours=24,
            minimum_source_accounts=10,
            minimum_intermediaries=2,
            limit=20,
        ),
    )


class DashboardApiTests(unittest.TestCase):
    def test_graph_query_is_bounded_parameterised_and_read_only(self):
        query = load_query("graph")

        self.assertIn("LIMIT $edge_limit", query)
        self.assertIn("$minimum_risk_score", query)
        self.assertIn("$minimum_pagerank_score", query)
        self.assertIn("$community_id", query)
        self.assertNotIn("CREATE ", query.upper())
        self.assertNotIn(" SET ", query.upper())
        self.assertNotIn("DELETE ", query.upper())

    def test_starburst_query_detects_a_bounded_multi_hop_funnel(self):
        query = load_query("starbursts")

        self.assertIn("(source:Account)-[inbound:TRANSFERRED_TO]->(intermediary:Account)", query)
        self.assertIn("-[outbound:TRANSFERRED_TO]->(sink:Account)", query)
        self.assertIn("$lookback_hours", query)
        self.assertIn("$minimum_source_accounts", query)
        self.assertIn("$minimum_intermediaries", query)
        self.assertIn("LIMIT $limit", query)
        self.assertNotIn("CREATE ", query.upper())
        self.assertNotIn(" SET ", query.upper())
        self.assertNotIn("DELETE ", query.upper())

    def test_repository_deduplicates_nodes_and_routes_query_as_read(self):
        driver = Mock()
        driver.execute_query.return_value = _result(
            {
                "source": _node("account-a"),
                "target": _node("account-b", score=0.8),
                "edge": _edge("transaction-1", "account-a", "account-b"),
            },
            {
                "source": _node("account-a"),
                "target": _node("account-c", score=1.0),
                "edge": _edge("transaction-2", "account-a", "account-c"),
            },
        )
        repository = DashboardGraphRepository(driver=driver)

        snapshot = repository.graph_snapshot(
            edge_limit=25,
            minimum_risk_score=20.0,
            minimum_pagerank_score=0.25,
            community_id=7,
        )

        self.assertEqual(
            [node.id for node in snapshot.nodes],
            ["account-a", "account-b", "account-c"],
        )
        self.assertEqual(len(snapshot.edges), 2)
        self.assertIsNotNone(snapshot.generated_at.tzinfo)
        call = driver.execute_query.call_args
        self.assertEqual(call.kwargs["routing_"], "r")
        self.assertEqual(call.kwargs["database_"], "neo4j")
        self.assertEqual(call.kwargs["parameters_"]["edge_limit"], 25)
        self.assertEqual(call.kwargs["parameters_"]["community_id"], 7)

    def test_repository_returns_typed_starbursts_and_routes_query_as_read(self):
        driver = Mock()
        driver.execute_query.return_value = _result(
            _starburst_snapshot().patterns[0].model_dump()
        )
        repository = DashboardGraphRepository(driver=driver)

        snapshot = repository.starburst_patterns(
            lookback_hours=48,
            minimum_source_accounts=12,
            minimum_intermediaries=3,
            limit=5,
        )

        self.assertEqual(snapshot.patterns[0].sink_account_id, "account-shell-001")
        self.assertEqual(snapshot.patterns[0].source_count, 10)
        self.assertEqual(snapshot.filters.lookback_hours, 48)
        call = driver.execute_query.call_args
        self.assertEqual(call.kwargs["routing_"], "r")
        self.assertEqual(call.kwargs["parameters_"]["minimum_source_accounts"], 12)
        self.assertEqual(call.kwargs["parameters_"]["minimum_intermediaries"], 3)
        self.assertEqual(call.kwargs["parameters_"]["limit"], 5)

    def test_health_and_graph_endpoints_return_typed_payloads_with_cors(self):
        repository = Mock(spec=DashboardGraphRepository)
        repository.health.return_value = True
        repository.graph_snapshot.return_value = _snapshot()
        repository.starburst_patterns.return_value = _starburst_snapshot()
        api = create_app(
            repository,
            allowed_origins=("http://localhost:5173",),
        )

        with TestClient(api) as client:
            health = client.get(
                "/health", headers={"Origin": "http://localhost:5173"}
            )
            graph = client.get("/api/graph")
            starbursts = client.get("/api/patterns/starbursts")

        self.assertEqual(health.status_code, 200)
        self.assertEqual(health.json(), {"status": "ok", "neo4j": "connected"})
        self.assertEqual(
            health.headers["access-control-allow-origin"], "http://localhost:5173"
        )
        self.assertEqual(graph.status_code, 200)
        self.assertEqual(graph.json()["nodes"][0]["id"], "account-a")
        self.assertEqual(starbursts.status_code, 200)
        self.assertEqual(
            starbursts.json()["patterns"][0]["sink_account_id"],
            "account-shell-001",
        )
        repository.graph_snapshot.assert_called_once_with(
            edge_limit=200,
            minimum_risk_score=0.0,
            minimum_pagerank_score=0.0,
            community_id=None,
        )
        repository.starburst_patterns.assert_called_once_with(
            lookback_hours=24,
            minimum_source_accounts=10,
            minimum_intermediaries=2,
            limit=20,
        )

    def test_invalid_filters_are_rejected_before_query_execution(self):
        repository = Mock(spec=DashboardGraphRepository)
        api = create_app(repository, allowed_origins=("http://localhost:5173",))

        with TestClient(api) as client:
            response = client.get("/api/graph?edge_limit=501")

        self.assertEqual(response.status_code, 422)
        repository.graph_snapshot.assert_not_called()

        with TestClient(api) as client:
            response = client.get(
                "/api/patterns/starbursts?minimum_source_accounts=1"
            )

        self.assertEqual(response.status_code, 422)
        repository.starburst_patterns.assert_not_called()

    def test_database_failure_is_reported_as_service_unavailable(self):
        repository = Mock(spec=DashboardGraphRepository)
        repository.health.side_effect = RuntimeError("connection failed")
        api = create_app(repository, allowed_origins=("http://localhost:5173",))

        with TestClient(api) as client:
            response = client.get("/health")

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"], "Neo4j is unavailable.")

    def test_allowed_origins_are_explicit_and_validated(self):
        with patch.dict(
            os.environ,
            {"DASHBOARD_ALLOWED_ORIGINS": "http://localhost:5173,https://analyst.test/"},
        ):
            self.assertEqual(
                allowed_origins_from_environment(),
                ("http://localhost:5173", "https://analyst.test"),
            )
        with patch.dict(os.environ, {"DASHBOARD_ALLOWED_ORIGINS": "*"}):
            with self.assertRaises(ValueError):
                allowed_origins_from_environment()

    @patch("fingraph.dashboard_api.DashboardGraphRepository")
    def test_owned_repository_uses_application_lifespan(self, repository_type):
        repository = repository_type.return_value
        api = create_app(allowed_origins=("http://localhost:5173",))

        with TestClient(api):
            repository.open.assert_called_once_with()

        repository.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
