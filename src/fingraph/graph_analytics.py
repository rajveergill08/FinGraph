"""Run FinGraph's explainable Week 2 Cypher analytics against Neo4j."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any, Sequence


_QUERY_FILES = {
    "circular_flows": "detect_circular_flows.cypher",
    "risk_scores": "refresh_account_risk_scores.cypher",
}


@dataclass(frozen=True, slots=True)
class AnalyticsSettings:
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_username: str = "neo4j"
    neo4j_password: str = "change-me-now"

    @classmethod
    def from_environment(cls) -> "AnalyticsSettings":
        defaults = cls()
        return cls(
            neo4j_uri=os.getenv("NEO4J_URI", defaults.neo4j_uri),
            neo4j_username=os.getenv("NEO4J_USERNAME", defaults.neo4j_username),
            neo4j_password=os.getenv("NEO4J_PASSWORD", defaults.neo4j_password),
        )


def load_query(name: str) -> str:
    """Load a named query from the repository's Neo4j query catalog."""
    try:
        filename = _QUERY_FILES[name]
    except KeyError as exc:
        raise ValueError(f"Unknown analytics query: {name!r}.") from exc
    return (Path(__file__).parents[2] / "neo4j" / filename).read_text(encoding="utf-8")


class GraphAnalyticsRunner:
    """Execute bounded circular-flow and account-risk queries."""

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

    def circular_flows(
        self,
        *,
        lookback_hours: int = 24,
        minimum_amount: float = 0.01,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if lookback_hours < 1:
            raise ValueError("lookback_hours must be at least one.")
        if minimum_amount < 0:
            raise ValueError("minimum_amount cannot be negative.")
        if limit < 1 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000.")
        return self._execute(
            "circular_flows",
            {
                "lookback_hours": lookback_hours,
                "minimum_amount": minimum_amount,
                "limit": limit,
            },
        )

    def refresh_risk_scores(
        self,
        *,
        lookback_hours: int = 24,
        high_risk_countries: Sequence[str] = (),
        volume_unit: float = 10_000.0,
    ) -> list[dict[str, Any]]:
        if lookback_hours < 1:
            raise ValueError("lookback_hours must be at least one.")
        if volume_unit <= 0:
            raise ValueError("volume_unit must be positive.")
        countries = sorted({country.strip().upper() for country in high_risk_countries})
        if any(len(country) != 2 or not country.isalpha() for country in countries):
            raise ValueError("high-risk countries must be two-letter ISO-3166 codes.")
        return self._execute(
            "risk_scores",
            {
                "lookback_hours": lookback_hours,
                "high_risk_countries": countries,
                "volume_unit": volume_unit,
            },
        )

    def close(self) -> None:
        if self._driver is not None and self._owns_driver:
            self._driver.close()
            self._driver = None

    def _execute(self, query_name: str, parameters: dict[str, Any]) -> list[dict[str, Any]]:
        if self._driver is None:
            raise RuntimeError("Graph analytics runner is not open.")
        result = self._driver.execute_query(
            load_query(query_name), parameters_=parameters, database_="neo4j"
        )
        return [record.data() for record in result.records]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Detect circular transfers and refresh Neo4j account risk scores."
    )
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--minimum-amount", type=float, default=0.01)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument(
        "--high-risk-country",
        action="append",
        default=[],
        help="Two-letter ISO country code; repeat for multiple countries.",
    )
    parser.add_argument("--volume-unit", type=float, default=10_000.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    runner = GraphAnalyticsRunner()
    try:
        runner.open()
        circular_flows = runner.circular_flows(
            lookback_hours=args.lookback_hours,
            minimum_amount=args.minimum_amount,
            limit=args.limit,
        )
        risk_scores = runner.refresh_risk_scores(
            lookback_hours=args.lookback_hours,
            high_risk_countries=args.high_risk_country,
            volume_unit=args.volume_unit,
        )
    finally:
        runner.close()
    print(
        json.dumps(
            {
                "circular_flows": circular_flows,
                "risk_scores_updated": len(risk_scores),
                "highest_risk_accounts": risk_scores[:10],
            },
            default=str,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
