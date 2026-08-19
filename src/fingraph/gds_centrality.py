"""Run FinGraph's Week 3 weighted PageRank centrality workflow."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any, Sequence

from .graph_analytics import AnalyticsSettings


_QUERY_FILES = {
    "gds_version": "gds_version.cypher",
    "drop_graph": "drop_gds_graph.cypher",
    "project_account_flow": "project_account_flow.cypher",
    "write_pagerank_scores": "write_pagerank_scores.cypher",
    "summarise_pagerank_scores": "summarise_pagerank_scores.cypher",
}
_GRAPH_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,62}$")
_PROPERTY_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


def load_query(name: str) -> str:
    """Load one query from the PageRank query catalog."""
    try:
        filename = _QUERY_FILES[name]
    except KeyError as exc:
        raise ValueError(f"Unknown PageRank query: {name!r}.") from exc
    return (Path(__file__).parents[2] / "neo4j" / filename).read_text(encoding="utf-8")


class GdsCentralityRunner:
    """Project directed transfers and persist weighted PageRank scores."""

    def __init__(
        self,
        settings: AnalyticsSettings | None = None,
        *,
        driver: Any = None,
    ) -> None:
        self.settings = settings or AnalyticsSettings.from_environment()
        self._driver = driver
        self._owns_driver = driver is None

    def open(self) -> None:
        if self._driver is not None:
            return
        from neo4j import GraphDatabase

        self._driver = GraphDatabase.driver(
            self.settings.neo4j_uri,
            auth=(self.settings.neo4j_username, self.settings.neo4j_password),
        )
        self._driver.verify_connectivity()

    def refresh_centrality(
        self,
        *,
        graph_name: str = "fingraph-account-flow",
        write_property: str = "pagerank_score",
        max_iterations: int = 20,
        damping_factor: float = 0.85,
        tolerance: float = 0.0000001,
        concurrency: int = 1,
        account_limit: int = 100,
    ) -> dict[str, Any]:
        self._validate_parameters(
            graph_name=graph_name,
            write_property=write_property,
            max_iterations=max_iterations,
            damping_factor=damping_factor,
            tolerance=tolerance,
            concurrency=concurrency,
            account_limit=account_limit,
        )
        if self._driver is None:
            raise RuntimeError("GDS centrality runner is not open.")

        version = self._single_record("gds_version", {})
        graph_parameters = {"graph_name": graph_name}
        self._execute("drop_graph", graph_parameters)
        try:
            projection = self._single_record("project_account_flow", graph_parameters)
            if projection["nodeCount"] < 2 or projection["relationshipCount"] < 1:
                raise RuntimeError(
                    "PageRank requires at least two Account nodes and one transfer."
                )
            pagerank = self._single_record(
                "write_pagerank_scores",
                {
                    **graph_parameters,
                    "write_property": write_property,
                    "max_iterations": max_iterations,
                    "damping_factor": damping_factor,
                    "tolerance": tolerance,
                    "concurrency": concurrency,
                },
            )
            accounts = self._execute(
                "summarise_pagerank_scores",
                {
                    "write_property": write_property,
                    "account_limit": account_limit,
                },
            )
            return {
                "gds_version": version["gds_version"],
                "graph_name": graph_name,
                "write_property": write_property,
                "projection": projection,
                "pagerank": pagerank,
                "highest_centrality_accounts": accounts,
            }
        finally:
            self._execute("drop_graph", graph_parameters)

    def close(self) -> None:
        if self._driver is not None and self._owns_driver:
            self._driver.close()
            self._driver = None

    def _execute(self, query_name: str, parameters: dict[str, Any]) -> list[dict[str, Any]]:
        result = self._driver.execute_query(
            load_query(query_name), parameters_=parameters, database_="neo4j"
        )
        return [record.data() for record in result.records]

    def _single_record(self, query_name: str, parameters: dict[str, Any]) -> dict[str, Any]:
        records = self._execute(query_name, parameters)
        if len(records) != 1:
            raise RuntimeError(
                f"Expected one result from {query_name!r}, received {len(records)}."
            )
        return records[0]

    @staticmethod
    def _validate_parameters(
        *,
        graph_name: str,
        write_property: str,
        max_iterations: int,
        damping_factor: float,
        tolerance: float,
        concurrency: int,
        account_limit: int,
    ) -> None:
        if not _GRAPH_NAME_PATTERN.fullmatch(graph_name):
            raise ValueError("graph_name must be 1-63 letters, numbers, underscores, or hyphens.")
        if not _PROPERTY_NAME_PATTERN.fullmatch(write_property):
            raise ValueError("write_property must be a valid Neo4j property name.")
        if max_iterations < 1 or max_iterations > 100:
            raise ValueError("max_iterations must be between 1 and 100.")
        if damping_factor < 0 or damping_factor >= 1:
            raise ValueError("damping_factor must be at least 0 and less than 1.")
        if tolerance <= 0 or tolerance > 1:
            raise ValueError("tolerance must be greater than 0 and at most 1.")
        if concurrency < 1 or concurrency > 4:
            raise ValueError("concurrency must be between 1 and 4 for GDS Community Edition.")
        if account_limit < 1 or account_limit > 1_000:
            raise ValueError("account_limit must be between 1 and 1000.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Project directed transfers and write weighted PageRank account scores."
    )
    parser.add_argument("--graph-name", default="fingraph-account-flow")
    parser.add_argument("--write-property", default="pagerank_score")
    parser.add_argument("--max-iterations", type=int, default=20)
    parser.add_argument("--damping-factor", type=float, default=0.85)
    parser.add_argument("--tolerance", type=float, default=0.0000001)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--account-limit", type=int, default=100)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runner = GdsCentralityRunner()
    try:
        runner.open()
        result = runner.refresh_centrality(
            graph_name=args.graph_name,
            write_property=args.write_property,
            max_iterations=args.max_iterations,
            damping_factor=args.damping_factor,
            tolerance=args.tolerance,
            concurrency=args.concurrency,
            account_limit=args.account_limit,
        )
    finally:
        runner.close()
    print(json.dumps(result, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
