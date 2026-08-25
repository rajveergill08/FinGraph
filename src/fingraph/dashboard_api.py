"""Read-only API for FinGraph's analyst network dashboard."""

from __future__ import annotations

import argparse
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Annotated, Any, Sequence
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .graph_analytics import AnalyticsSettings


_QUERY_FILES = {
    "health": "dashboard_health.cypher",
    "graph": "dashboard_graph.cypher",
    "starbursts": "detect_starburst_patterns.cypher",
}


def load_query(name: str) -> str:
    """Load one query from the dashboard's read-only Cypher catalog."""
    try:
        filename = _QUERY_FILES[name]
    except KeyError as exc:
        raise ValueError(f"Unknown dashboard query: {name!r}.") from exc
    return (Path(__file__).parents[2] / "neo4j" / filename).read_text(encoding="utf-8")


def allowed_origins_from_environment() -> tuple[str, ...]:
    """Return explicit browser origins allowed to call the API."""
    raw_origins = os.getenv("DASHBOARD_ALLOWED_ORIGINS", "http://localhost:5173")
    origins = tuple(
        origin.strip().rstrip("/")
        for origin in raw_origins.split(",")
        if origin.strip()
    )
    if not origins:
        raise ValueError("DASHBOARD_ALLOWED_ORIGINS must contain at least one origin.")
    for origin in origins:
        parsed = urlsplit(origin)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.netloc
            or parsed.path
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "DASHBOARD_ALLOWED_ORIGINS must contain comma-separated HTTP(S) origins."
            )
    return origins


class DashboardNode(BaseModel):
    id: str
    label: str
    country: str | None = None
    account_type: str | None = None
    risk_tier: str | None = None
    graph_risk_score: float = Field(default=0.0, ge=0.0, le=100.0)
    pagerank_score: float = Field(default=0.0, ge=0.0)
    community_id: int | None = None


class DashboardEdge(BaseModel):
    id: str
    source: str
    target: str
    amount: float = Field(ge=0.0)
    currency: str | None = None
    occurred_at: str | None = None
    channel: str | None = None
    syndicate_id: str | None = None
    risk_indicators: list[str] = Field(default_factory=list)


class GraphFilters(BaseModel):
    edge_limit: int
    minimum_risk_score: float
    minimum_pagerank_score: float
    community_id: int | None


class GraphSnapshot(BaseModel):
    generated_at: datetime
    nodes: list[DashboardNode]
    edges: list[DashboardEdge]
    filters: GraphFilters


class StarburstPattern(BaseModel):
    id: str
    sink_account_id: str
    source_account_ids: list[str]
    intermediary_account_ids: list[str]
    source_count: int = Field(ge=2)
    intermediary_count: int = Field(ge=1)
    inbound_transfer_count: int = Field(ge=1)
    outbound_transfer_count: int = Field(ge=1)
    latest_transfer_at: str


class StarburstFilters(BaseModel):
    lookback_hours: int
    minimum_source_accounts: int
    minimum_intermediaries: int
    limit: int


class StarburstSnapshot(BaseModel):
    generated_at: datetime
    patterns: list[StarburstPattern]
    filters: StarburstFilters


class HealthResponse(BaseModel):
    status: str
    neo4j: str


class DashboardGraphRepository:
    """Execute bounded read queries and shape them for network visualization."""

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

    def health(self) -> bool:
        records = self._execute("health", {})
        return len(records) == 1 and records[0].get("ok") == 1

    def graph_snapshot(
        self,
        *,
        edge_limit: int = 200,
        minimum_risk_score: float = 0.0,
        minimum_pagerank_score: float = 0.0,
        community_id: int | None = None,
    ) -> GraphSnapshot:
        self._validate_filters(
            edge_limit=edge_limit,
            minimum_risk_score=minimum_risk_score,
            minimum_pagerank_score=minimum_pagerank_score,
            community_id=community_id,
        )
        records = self._execute(
            "graph",
            {
                "edge_limit": edge_limit,
                "minimum_risk_score": minimum_risk_score,
                "minimum_pagerank_score": minimum_pagerank_score,
                "community_id": community_id,
            },
        )
        nodes: dict[str, DashboardNode] = {}
        edges: list[DashboardEdge] = []
        for record in records:
            source = DashboardNode.model_validate(record["source"])
            target = DashboardNode.model_validate(record["target"])
            nodes[source.id] = source
            nodes[target.id] = target
            edges.append(DashboardEdge.model_validate(record["edge"]))
        return GraphSnapshot(
            generated_at=datetime.now(timezone.utc),
            nodes=sorted(nodes.values(), key=lambda node: node.id),
            edges=edges,
            filters=GraphFilters(
                edge_limit=edge_limit,
                minimum_risk_score=minimum_risk_score,
                minimum_pagerank_score=minimum_pagerank_score,
                community_id=community_id,
            ),
        )

    def starburst_patterns(
        self,
        *,
        lookback_hours: int = 24,
        minimum_source_accounts: int = 10,
        minimum_intermediaries: int = 2,
        limit: int = 20,
    ) -> StarburstSnapshot:
        self._validate_starburst_filters(
            lookback_hours=lookback_hours,
            minimum_source_accounts=minimum_source_accounts,
            minimum_intermediaries=minimum_intermediaries,
            limit=limit,
        )
        records = self._execute(
            "starbursts",
            {
                "lookback_hours": lookback_hours,
                "minimum_source_accounts": minimum_source_accounts,
                "minimum_intermediaries": minimum_intermediaries,
                "limit": limit,
            },
        )
        return StarburstSnapshot(
            generated_at=datetime.now(timezone.utc),
            patterns=[StarburstPattern.model_validate(record) for record in records],
            filters=StarburstFilters(
                lookback_hours=lookback_hours,
                minimum_source_accounts=minimum_source_accounts,
                minimum_intermediaries=minimum_intermediaries,
                limit=limit,
            ),
        )

    def close(self) -> None:
        if self._driver is not None and self._owns_driver:
            self._driver.close()
            self._driver = None

    def _execute(self, query_name: str, parameters: dict[str, Any]) -> list[dict[str, Any]]:
        if self._driver is None:
            raise RuntimeError("Dashboard graph repository is not open.")
        result = self._driver.execute_query(
            load_query(query_name),
            parameters_=parameters,
            routing_="r",
            database_="neo4j",
        )
        return [record.data() for record in result.records]

    @staticmethod
    def _validate_filters(
        *,
        edge_limit: int,
        minimum_risk_score: float,
        minimum_pagerank_score: float,
        community_id: int | None,
    ) -> None:
        if edge_limit < 1 or edge_limit > 500:
            raise ValueError("edge_limit must be between 1 and 500.")
        if minimum_risk_score < 0 or minimum_risk_score > 100:
            raise ValueError("minimum_risk_score must be between 0 and 100.")
        if minimum_pagerank_score < 0 or minimum_pagerank_score > 1:
            raise ValueError("minimum_pagerank_score must be between 0 and 1.")
        if community_id is not None and community_id < 0:
            raise ValueError("community_id cannot be negative.")

    @staticmethod
    def _validate_starburst_filters(
        *,
        lookback_hours: int,
        minimum_source_accounts: int,
        minimum_intermediaries: int,
        limit: int,
    ) -> None:
        if lookback_hours < 1 or lookback_hours > 720:
            raise ValueError("lookback_hours must be between 1 and 720.")
        if minimum_source_accounts < 2 or minimum_source_accounts > 1_000:
            raise ValueError("minimum_source_accounts must be between 2 and 1000.")
        if minimum_intermediaries < 1 or minimum_intermediaries > 100:
            raise ValueError("minimum_intermediaries must be between 1 and 100.")
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100.")


def create_app(
    repository: DashboardGraphRepository | None = None,
    *,
    allowed_origins: Sequence[str] | None = None,
) -> FastAPI:
    graph_repository = repository or DashboardGraphRepository()
    owns_repository = repository is None

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if owns_repository:
            graph_repository.open()
        try:
            yield
        finally:
            if owns_repository:
                graph_repository.close()

    api = FastAPI(
        title="FinGraph Analyst API",
        version="0.1.0",
        description="Read-only fraud-network snapshots for the analyst dashboard.",
        lifespan=lifespan,
    )
    api.add_middleware(
        CORSMiddleware,
        allow_origins=list(allowed_origins or allowed_origins_from_environment()),
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=["Accept", "Content-Type"],
    )

    @api.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        try:
            connected = graph_repository.health()
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Neo4j is unavailable.",
            ) from exc
        if not connected:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Neo4j health check failed.",
            )
        return HealthResponse(status="ok", neo4j="connected")

    @api.get("/api/graph", response_model=GraphSnapshot)
    def graph_snapshot(
        edge_limit: Annotated[int, Query(ge=1, le=500)] = 200,
        minimum_risk_score: Annotated[float, Query(ge=0.0, le=100.0)] = 0.0,
        minimum_pagerank_score: Annotated[float, Query(ge=0.0, le=1.0)] = 0.0,
        community_id: Annotated[int | None, Query(ge=0)] = None,
    ) -> GraphSnapshot:
        try:
            return graph_repository.graph_snapshot(
                edge_limit=edge_limit,
                minimum_risk_score=minimum_risk_score,
                minimum_pagerank_score=minimum_pagerank_score,
                community_id=community_id,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="The fraud-network snapshot is unavailable.",
            ) from exc

    @api.get("/api/patterns/starbursts", response_model=StarburstSnapshot)
    def starburst_patterns(
        lookback_hours: Annotated[int, Query(ge=1, le=720)] = 24,
        minimum_source_accounts: Annotated[int, Query(ge=2, le=1_000)] = 10,
        minimum_intermediaries: Annotated[int, Query(ge=1, le=100)] = 2,
        limit: Annotated[int, Query(ge=1, le=100)] = 20,
    ) -> StarburstSnapshot:
        try:
            return graph_repository.starburst_patterns(
                lookback_hours=lookback_hours,
                minimum_source_accounts=minimum_source_accounts,
                minimum_intermediaries=minimum_intermediaries,
                limit=limit,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Starburst-pattern detection is unavailable.",
            ) from exc

    return api


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve the FinGraph analyst API.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--log-level", default="info")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.port < 1 or args.port > 65_535:
        raise ValueError("port must be between 1 and 65535.")
    import uvicorn

    uvicorn.run(
        "fingraph.dashboard_api:app",
        host=args.host,
        port=args.port,
        log_level=args.log_level,
    )
    return 0


app = create_app()


if __name__ == "__main__":
    raise SystemExit(main())
