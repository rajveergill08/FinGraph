"""Run repeatable, read-only checks for FinGraph's final review."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
from typing import Any, Callable, Sequence
from urllib.request import Request, urlopen


_QUERY_FILES = {
    "circular_flows": "detect_circular_flows.cypher",
    "starbursts": "detect_starburst_patterns.cypher",
}

_GRAPH_INVENTORY = """
CALL () {
  MATCH (account:Account)
  RETURN count(account) AS accounts,
         count(CASE WHEN account.graph_risk_score IS NOT NULL THEN 1 END) AS risk_scored,
         count(CASE WHEN account.louvain_community_id IS NOT NULL THEN 1 END) AS community_scored,
         count(CASE WHEN account.pagerank_score IS NOT NULL THEN 1 END) AS centrality_scored
}
CALL () {
  MATCH ()-[transfer:TRANSFERRED_TO]->()
  RETURN count(transfer) AS transfers
}
RETURN accounts, transfers, risk_scored, community_scored, centrality_scored
"""

_CONSTRAINT_NAMES = {
    "account_id_unique",
    "bank_id_unique",
    "containment_case_id_unique",
    "person_id_unique",
    "transaction_id_unique",
}

_INDEX_NAMES = {
    "account_country",
    "account_graph_risk_score",
    "account_louvain_community_id",
    "account_pagerank_score",
    "account_risk_tier",
    "transfer_occurred_at",
}

UrlFetcher = Callable[[str, float], tuple[int, str]]


@dataclass(frozen=True, slots=True)
class ReviewSettings:
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_username: str = "neo4j"
    neo4j_password: str = "change-me-now"
    neo4j_database: str = "neo4j"
    dashboard_url: str = "http://localhost:5173"
    neo4j_browser_url: str = "http://localhost:7474"
    dashboard_api_url: str = "http://localhost:8000"

    @classmethod
    def from_environment(cls) -> "ReviewSettings":
        defaults = cls()
        return cls(
            neo4j_uri=os.getenv("NEO4J_URI", defaults.neo4j_uri),
            neo4j_username=os.getenv("NEO4J_USERNAME", defaults.neo4j_username),
            neo4j_password=os.getenv("NEO4J_PASSWORD", defaults.neo4j_password),
            neo4j_database=os.getenv("NEO4J_DATABASE", defaults.neo4j_database),
            dashboard_url=os.getenv("DASHBOARD_WEB_URL", defaults.dashboard_url),
            neo4j_browser_url=os.getenv(
                "NEO4J_BROWSER_URL", defaults.neo4j_browser_url
            ),
            dashboard_api_url=os.getenv(
                "DASHBOARD_API_URL", defaults.dashboard_api_url
            ),
        )


@dataclass(frozen=True, slots=True)
class ReviewCheck:
    name: str
    passed: bool
    details: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "passed": self.passed, "details": self.details}


@dataclass(frozen=True, slots=True)
class ReviewReport:
    generated_at: str
    checks: tuple[ReviewCheck, ...]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    def as_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "passed": self.passed,
            "checks": [check.as_dict() for check in self.checks],
        }


def load_review_query(name: str) -> str:
    """Load one of the two multi-hop queries exercised by the review audit."""
    try:
        filename = _QUERY_FILES[name]
    except KeyError as exc:
        raise ValueError(f"Unknown review query: {name!r}.") from exc
    return (Path(__file__).parents[2] / "neo4j" / filename).read_text(encoding="utf-8")


def fetch_url(url: str, timeout_seconds: float) -> tuple[int, str]:
    """Fetch a local review page without adding an HTTP client dependency."""
    request = Request(url, headers={"Accept": "application/json,text/html"})
    with urlopen(request, timeout=timeout_seconds) as response:
        return response.status, response.read().decode("utf-8")


class ReviewReadinessAuditor:
    """Verify graph, analytics, query latency, and review-page readiness."""

    def __init__(
        self,
        settings: ReviewSettings | None = None,
        *,
        driver: Any = None,
        clock: Callable[[], float] = time.perf_counter,
        url_fetcher: UrlFetcher = fetch_url,
    ) -> None:
        self.settings = settings or ReviewSettings.from_environment()
        self._driver = driver
        self._owns_driver = driver is None
        self._clock = clock
        self._url_fetcher = url_fetcher

    def open(self) -> None:
        if self._driver is None:
            from neo4j import GraphDatabase

            self._driver = GraphDatabase.driver(
                self.settings.neo4j_uri,
                auth=(self.settings.neo4j_username, self.settings.neo4j_password),
            )
        self._driver.verify_connectivity()

    def run(
        self,
        *,
        query_target_ms: float = 100.0,
        query_runs: int = 3,
        lookback_hours: int = 720,
        include_pages: bool = True,
        http_timeout_seconds: float = 5.0,
    ) -> ReviewReport:
        if self._driver is None:
            raise RuntimeError("Review readiness auditor is not open.")
        if query_target_ms <= 0:
            raise ValueError("query_target_ms must be positive.")
        if query_runs < 1 or query_runs > 20:
            raise ValueError("query_runs must be between 1 and 20.")
        if lookback_hours < 1:
            raise ValueError("lookback_hours must be at least one.")
        if http_timeout_seconds <= 0:
            raise ValueError("http_timeout_seconds must be positive.")

        checks = [
            self._capture("graph_data", self._check_graph_data),
            self._capture("graph_schema", self._check_graph_schema),
            self._capture("analytics_coverage", self._check_analytics_coverage),
            self._capture(
                "circular_flow_query_latency",
                lambda: self._check_query_latency(
                    "circular_flows",
                    {
                        "lookback_hours": lookback_hours,
                        "minimum_amount": 0.01,
                        "limit": 100,
                    },
                    query_target_ms,
                    query_runs,
                ),
            ),
            self._capture(
                "starburst_query_latency",
                lambda: self._check_query_latency(
                    "starbursts",
                    {
                        "lookback_hours": lookback_hours,
                        "minimum_source_accounts": 2,
                        "minimum_intermediaries": 1,
                        "limit": 100,
                    },
                    query_target_ms,
                    query_runs,
                ),
            ),
        ]
        if include_pages:
            checks.append(
                self._capture(
                    "review_pages",
                    lambda: self._check_review_pages(http_timeout_seconds),
                )
            )
        return ReviewReport(
            generated_at=datetime.now(timezone.utc).isoformat(),
            checks=tuple(checks),
        )

    def close(self) -> None:
        if self._driver is not None and self._owns_driver:
            self._driver.close()
            self._driver = None

    def _capture(
        self, name: str, check: Callable[[], tuple[bool, dict[str, Any]]]
    ) -> ReviewCheck:
        try:
            passed, details = check()
        except Exception as exc:
            return ReviewCheck(name=name, passed=False, details={"error": str(exc)})
        return ReviewCheck(name=name, passed=passed, details=details)

    def _check_graph_data(self) -> tuple[bool, dict[str, Any]]:
        inventory = self._inventory()
        passed = inventory["accounts"] > 0 and inventory["transfers"] > 0
        return passed, {
            "accounts": inventory["accounts"],
            "transfers": inventory["transfers"],
        }

    def _check_graph_schema(self) -> tuple[bool, dict[str, Any]]:
        constraints = self._single(
            "SHOW CONSTRAINTS YIELD name RETURN collect(name) AS names"
        )["names"]
        indexes = self._single(
            "SHOW INDEXES YIELD name, type "
            "WHERE type <> 'LOOKUP' RETURN collect(name) AS names"
        )["names"]
        missing_constraints = sorted(_CONSTRAINT_NAMES - set(constraints))
        missing_indexes = sorted(_INDEX_NAMES - set(indexes))
        return not missing_constraints and not missing_indexes, {
            "missing_constraints": missing_constraints,
            "missing_indexes": missing_indexes,
            "required_constraints": len(_CONSTRAINT_NAMES),
            "required_indexes": len(_INDEX_NAMES),
        }

    def _check_analytics_coverage(self) -> tuple[bool, dict[str, Any]]:
        inventory = self._inventory()
        accounts = inventory["accounts"]
        fields = ("risk_scored", "community_scored", "centrality_scored")
        passed = accounts > 0 and all(inventory[field] == accounts for field in fields)
        return passed, {
            "accounts": accounts,
            **{field: inventory[field] for field in fields},
        }

    def _check_query_latency(
        self,
        query_name: str,
        parameters: dict[str, Any],
        target_ms: float,
        runs: int,
    ) -> tuple[bool, dict[str, Any]]:
        query = load_review_query(query_name)
        self._execute(query, parameters)
        durations = []
        result_count = 0
        for _ in range(runs):
            started_at = self._clock()
            result = self._execute(query, parameters)
            durations.append(round((self._clock() - started_at) * 1000, 3))
            result_count = len(result.records)
        maximum_ms = max(durations)
        return maximum_ms < target_ms, {
            "maximum_ms": maximum_ms,
            "result_count": result_count,
            "run_ms": durations,
            "target_ms": target_ms,
        }

    def _check_review_pages(self, timeout_seconds: float) -> tuple[bool, dict[str, Any]]:
        urls = {
            "dashboard": self.settings.dashboard_url.rstrip("/"),
            "neo4j_browser": self.settings.neo4j_browser_url.rstrip("/"),
            "api_docs": f"{self.settings.dashboard_api_url.rstrip('/')}/docs",
            "api_health": f"{self.settings.dashboard_api_url.rstrip('/')}/health",
        }
        pages: dict[str, dict[str, Any]] = {}
        for name, url in urls.items():
            try:
                status, body = self._url_fetcher(url, timeout_seconds)
                page_passed = 200 <= status < 300 and bool(body.strip())
                if name == "api_health" and page_passed:
                    payload = json.loads(body)
                    page_passed = payload.get("status") == "ok"
                pages[name] = {"passed": page_passed, "status": status, "url": url}
            except Exception as exc:
                pages[name] = {"passed": False, "error": str(exc), "url": url}
        return all(page["passed"] for page in pages.values()), {"pages": pages}

    def _inventory(self) -> dict[str, Any]:
        return self._single(_GRAPH_INVENTORY)

    def _single(self, query: str) -> dict[str, Any]:
        result = self._execute(query)
        if not result.records:
            raise RuntimeError("Neo4j readiness query returned no records.")
        return result.records[0].data()

    def _execute(self, query: str, parameters: dict[str, Any] | None = None) -> Any:
        return self._driver.execute_query(
            query,
            parameters_=parameters or {},
            database_=self.settings.neo4j_database,
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit FinGraph's local final-review readiness."
    )
    parser.add_argument("--query-target-ms", type=float, default=100.0)
    parser.add_argument("--query-runs", type=int, default=3)
    parser.add_argument("--lookback-hours", type=int, default=720)
    parser.add_argument("--http-timeout-seconds", type=float, default=5.0)
    parser.add_argument(
        "--skip-pages",
        action="store_true",
        help="Skip checks for the dashboard, Neo4j Browser, and API pages.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    auditor = ReviewReadinessAuditor()
    try:
        auditor.open()
        report = auditor.run(
            query_target_ms=args.query_target_ms,
            query_runs=args.query_runs,
            lookback_hours=args.lookback_hours,
            include_pages=not args.skip_pages,
            http_timeout_seconds=args.http_timeout_seconds,
        )
    except Exception as exc:
        print(json.dumps({"passed": False, "error": str(exc)}, sort_keys=True))
        return 1
    finally:
        auditor.close()
    print(json.dumps(report.as_dict(), default=str, sort_keys=True))
    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
