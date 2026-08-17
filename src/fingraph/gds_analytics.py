"""Run FinGraph's Week 3 Neo4j GDS community-detection workflow."""

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
    "project_account_network": "project_account_network.cypher",
    "write_louvain_communities": "write_louvain_communities.cypher",
    "summarise_louvain_communities": "summarise_louvain_communities.cypher",
}
_GRAPH_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,62}$")
_PROPERTY_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


def load_query(name: str) -> str:
    """Load one query from the Week 3 GDS query catalog."""
    try:
        filename = _QUERY_FILES[name]
    except KeyError as exc:
        raise ValueError(f"Unknown GDS query: {name!r}.") from exc
    return (Path(__file__).parents[2] / "neo4j" / filename).read_text(encoding="utf-8")


class GdsCommunityRunner:
    """Project the account graph and persist weighted Louvain communities."""

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

    def refresh_communities(
        self,
        *,
        graph_name: str = "fingraph-account-network",
        write_property: str = "louvain_community_id",
        max_levels: int = 10,
        max_iterations: int = 10,
        concurrency: int = 1,
        community_limit: int = 100,
        member_sample_size: int = 10,
    ) -> dict[str, Any]:
        self._validate_parameters(
            graph_name=graph_name,
            write_property=write_property,
            max_levels=max_levels,
            max_iterations=max_iterations,
            concurrency=concurrency,
            community_limit=community_limit,
            member_sample_size=member_sample_size,
        )
        if self._driver is None:
            raise RuntimeError("GDS community runner is not open.")

        version = self._single_record("gds_version", {})
        parameters = {"graph_name": graph_name}
        self._execute("drop_graph", parameters)
        try:
            projection = self._single_record("project_account_network", parameters)
            if projection["nodeCount"] < 2 or projection["relationshipCount"] < 1:
                raise RuntimeError(
                    "Louvain requires at least two Account nodes and one transfer."
                )
            louvain = self._single_record(
                "write_louvain_communities",
                {
                    **parameters,
                    "write_property": write_property,
                    "max_levels": max_levels,
                    "max_iterations": max_iterations,
                    "concurrency": concurrency,
                },
            )
            communities = self._execute(
                "summarise_louvain_communities",
                {
                    "write_property": write_property,
                    "community_limit": community_limit,
                    "member_sample_size": member_sample_size,
                },
            )
            return {
                "gds_version": version["gds_version"],
                "graph_name": graph_name,
                "write_property": write_property,
                "projection": projection,
                "louvain": louvain,
                "communities": communities,
            }
        finally:
            self._execute("drop_graph", parameters)

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
        max_levels: int,
        max_iterations: int,
        concurrency: int,
        community_limit: int,
        member_sample_size: int,
    ) -> None:
        if not _GRAPH_NAME_PATTERN.fullmatch(graph_name):
            raise ValueError("graph_name must be 1-63 letters, numbers, underscores, or hyphens.")
        if not _PROPERTY_NAME_PATTERN.fullmatch(write_property):
            raise ValueError("write_property must be a valid Neo4j property name.")
        if max_levels < 1 or max_levels > 100:
            raise ValueError("max_levels must be between 1 and 100.")
        if max_iterations < 1 or max_iterations > 100:
            raise ValueError("max_iterations must be between 1 and 100.")
        if concurrency < 1 or concurrency > 4:
            raise ValueError("concurrency must be between 1 and 4 for GDS Community Edition.")
        if community_limit < 1 or community_limit > 1_000:
            raise ValueError("community_limit must be between 1 and 1000.")
        if member_sample_size < 1 or member_sample_size > 100:
            raise ValueError("member_sample_size must be between 1 and 100.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Project FinGraph accounts and write weighted Louvain communities."
    )
    parser.add_argument("--graph-name", default="fingraph-account-network")
    parser.add_argument("--write-property", default="louvain_community_id")
    parser.add_argument("--max-levels", type=int, default=10)
    parser.add_argument("--max-iterations", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--community-limit", type=int, default=100)
    parser.add_argument("--member-sample-size", type=int, default=10)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runner = GdsCommunityRunner()
    try:
        runner.open()
        result = runner.refresh_communities(
            graph_name=args.graph_name,
            write_property=args.write_property,
            max_levels=args.max_levels,
            max_iterations=args.max_iterations,
            concurrency=args.concurrency,
            community_limit=args.community_limit,
            member_sample_size=args.member_sample_size,
        )
    finally:
        runner.close()
    print(json.dumps(result, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
