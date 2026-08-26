"""Evaluate high-risk accounts and deliver cooldown-aware analyst alerts."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
import json
import os
from pathlib import Path
import smtplib
from typing import Any, Callable, Protocol, Sequence
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from .graph_analytics import AnalyticsSettings


_QUERY_PATH = Path(__file__).parents[2] / "neo4j" / "find_alert_candidates.cypher"


def load_query() -> str:
    """Load the bounded high-risk-account candidate query."""
    return _QUERY_PATH.read_text(encoding="utf-8")


def _environment_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    value = raw_value.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value.")


@dataclass(frozen=True, slots=True)
class AlertSettings:
    risk_threshold: float = 70.0
    candidate_limit: int = 100
    cooldown_hours: float = 24.0
    state_path: Path = Path("data/alert-state.json")
    slack_webhook_url: str | None = None
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_use_tls: bool = True
    email_from: str | None = None
    email_to: tuple[str, ...] = ()

    @classmethod
    def from_environment(cls) -> "AlertSettings":
        defaults = cls()
        recipients = tuple(
            address.strip()
            for address in os.getenv("ALERT_EMAIL_TO", "").split(",")
            if address.strip()
        )
        settings = cls(
            risk_threshold=float(
                os.getenv("ALERT_RISK_THRESHOLD", defaults.risk_threshold)
            ),
            candidate_limit=int(
                os.getenv("ALERT_CANDIDATE_LIMIT", defaults.candidate_limit)
            ),
            cooldown_hours=float(
                os.getenv("ALERT_COOLDOWN_HOURS", defaults.cooldown_hours)
            ),
            state_path=Path(os.getenv("ALERT_STATE_PATH", str(defaults.state_path))),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL") or None,
            smtp_host=os.getenv("SMTP_HOST") or None,
            smtp_port=int(os.getenv("SMTP_PORT", defaults.smtp_port)),
            smtp_username=os.getenv("SMTP_USERNAME") or None,
            smtp_password=os.getenv("SMTP_PASSWORD") or None,
            smtp_use_tls=_environment_bool("SMTP_USE_TLS", defaults.smtp_use_tls),
            email_from=os.getenv("ALERT_EMAIL_FROM") or None,
            email_to=recipients,
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if self.risk_threshold < 0 or self.risk_threshold > 100:
            raise ValueError("ALERT_RISK_THRESHOLD must be between 0 and 100.")
        if self.candidate_limit < 1 or self.candidate_limit > 1_000:
            raise ValueError("ALERT_CANDIDATE_LIMIT must be between 1 and 1000.")
        if self.cooldown_hours < 0 or self.cooldown_hours > 8_760:
            raise ValueError("ALERT_COOLDOWN_HOURS must be between 0 and 8760.")
        if self.smtp_port < 1 or self.smtp_port > 65_535:
            raise ValueError("SMTP_PORT must be between 1 and 65535.")
        if self.slack_webhook_url:
            parsed = urlsplit(self.slack_webhook_url)
            if parsed.scheme != "https" or not parsed.netloc:
                raise ValueError("SLACK_WEBHOOK_URL must be an HTTPS URL.")
        email_values = (
            self.smtp_host,
            self.email_from,
            self.email_to,
            self.smtp_username,
            self.smtp_password,
        )
        if any(email_values) and not all(
            (self.smtp_host, self.email_from, self.email_to)
        ):
            raise ValueError(
                "Email alerts require SMTP_HOST, ALERT_EMAIL_FROM, and ALERT_EMAIL_TO."
            )
        if self.smtp_username and not self.smtp_password:
            raise ValueError("SMTP_PASSWORD is required when SMTP_USERNAME is set.")


@dataclass(frozen=True, slots=True)
class RiskAlert:
    account_id: str
    graph_risk_score: float
    risk_tier: str | None
    country: str | None
    pagerank_score: float
    community_id: int | None
    transaction_count: int
    counterparty_count: int
    latest_transfer_at: str | None

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "RiskAlert":
        return cls(
            account_id=str(record["account_id"]),
            graph_risk_score=float(record["graph_risk_score"]),
            risk_tier=record.get("risk_tier"),
            country=record.get("country"),
            pagerank_score=float(record.get("pagerank_score") or 0.0),
            community_id=record.get("community_id"),
            transaction_count=int(record.get("transaction_count") or 0),
            counterparty_count=int(record.get("counterparty_count") or 0),
            latest_transfer_at=record.get("latest_transfer_at"),
        )


class HighRiskAccountRepository:
    """Read alert candidates from Neo4j without mutating the graph."""

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

    def find_candidates(
        self,
        *,
        minimum_risk_score: float = 70.0,
        limit: int = 100,
    ) -> list[RiskAlert]:
        if minimum_risk_score < 0 or minimum_risk_score > 100:
            raise ValueError("minimum_risk_score must be between 0 and 100.")
        if limit < 1 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000.")
        if self._driver is None:
            raise RuntimeError("High-risk account repository is not open.")
        result = self._driver.execute_query(
            load_query(),
            parameters_={
                "minimum_risk_score": minimum_risk_score,
                "limit": limit,
            },
            routing_="r",
            database_="neo4j",
        )
        return [RiskAlert.from_record(record.data()) for record in result.records]

    def close(self) -> None:
        if self._driver is not None and self._owns_driver:
            self._driver.close()
            self._driver = None


class NotificationSink(Protocol):
    name: str

    def send(self, alert: RiskAlert) -> None: ...


def format_alert_message(alert: RiskAlert) -> str:
    """Create a concise, channel-neutral analyst notification."""
    return "\n".join(
        (
            "FinGraph high-risk account alert",
            f"Account: {alert.account_id}",
            f"Graph risk score: {alert.graph_risk_score:.2f}/100",
            f"Risk tier: {alert.risk_tier or 'unknown'}",
            f"Country: {alert.country or 'unknown'}",
            f"PageRank: {alert.pagerank_score:.4f}",
            f"Community: {alert.community_id if alert.community_id is not None else 'unassigned'}",
            f"Transactions: {alert.transaction_count}",
            f"Counterparties: {alert.counterparty_count}",
        )
    )


def _post_json(url: str, payload: bytes, timeout: float) -> None:
    request = Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=timeout) as response:
        if response.status < 200 or response.status >= 300:
            raise RuntimeError(f"Slack webhook returned HTTP {response.status}.")


class SlackWebhookSink:
    name = "slack"

    def __init__(
        self,
        webhook_url: str,
        *,
        timeout: float = 10.0,
        post_json: Callable[[str, bytes, float], None] = _post_json,
    ) -> None:
        parsed = urlsplit(webhook_url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ValueError("Slack webhook URL must use HTTPS.")
        self.webhook_url = webhook_url
        self.timeout = timeout
        self._post_json = post_json

    def send(self, alert: RiskAlert) -> None:
        payload = json.dumps({"text": format_alert_message(alert)}).encode("utf-8")
        self._post_json(self.webhook_url, payload, self.timeout)


class EmailSink:
    name = "email"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        sender: str,
        recipients: Sequence[str],
        username: str | None = None,
        password: str | None = None,
        use_tls: bool = True,
        timeout: float = 10.0,
        smtp_factory: Callable[..., Any] = smtplib.SMTP,
    ) -> None:
        if not host or not sender or not recipients:
            raise ValueError("Email alerts require a host, sender, and recipients.")
        if port < 1 or port > 65_535:
            raise ValueError("SMTP port must be between 1 and 65535.")
        if username and not password:
            raise ValueError("An SMTP password is required with an SMTP username.")
        self.host = host
        self.port = port
        self.sender = sender
        self.recipients = tuple(recipients)
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.timeout = timeout
        self._smtp_factory = smtp_factory

    def send(self, alert: RiskAlert) -> None:
        message = EmailMessage()
        message["Subject"] = (
            f"FinGraph alert: {alert.account_id} scored {alert.graph_risk_score:.2f}"
        )
        message["From"] = self.sender
        message["To"] = ", ".join(self.recipients)
        message.set_content(format_alert_message(alert))

        with self._smtp_factory(self.host, self.port, timeout=self.timeout) as client:
            if self.use_tls:
                client.starttls()
            if self.username:
                client.login(self.username, self.password)
            client.send_message(message)


class JsonAlertStateStore:
    """Persist per-channel delivery times so polling does not spam analysts."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._deliveries: dict[str, dict[str, Any]] | None = None

    def should_deliver(
        self,
        alert: RiskAlert,
        channel: str,
        *,
        now: datetime,
        cooldown: timedelta,
    ) -> bool:
        deliveries = self._load()
        previous = deliveries.get(self._key(alert, channel))
        if previous is None:
            return True
        delivered_at = datetime.fromisoformat(previous["delivered_at"])
        return now >= delivered_at + cooldown

    def mark_delivered(
        self,
        alert: RiskAlert,
        channel: str,
        *,
        delivered_at: datetime,
    ) -> None:
        deliveries = self._load()
        deliveries[self._key(alert, channel)] = {
            "account_id": alert.account_id,
            "channel": channel,
            "graph_risk_score": alert.graph_risk_score,
            "delivered_at": delivered_at.isoformat(),
        }
        self._save(deliveries)

    def _load(self) -> dict[str, dict[str, Any]]:
        if self._deliveries is not None:
            return self._deliveries
        if not self.path.exists():
            self._deliveries = {}
            return self._deliveries
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        if payload.get("version") != 1 or not isinstance(payload.get("deliveries"), dict):
            raise ValueError(f"Unsupported alert state file: {self.path}.")
        self._deliveries = payload["deliveries"]
        return self._deliveries

    def _save(self, deliveries: dict[str, dict[str, Any]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary_path.write_text(
            json.dumps({"version": 1, "deliveries": deliveries}, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(self.path)

    @staticmethod
    def _key(alert: RiskAlert, channel: str) -> str:
        return f"high-risk-account:{channel}:{alert.account_id}"


@dataclass(frozen=True, slots=True)
class AlertRunSummary:
    threshold: float
    candidate_count: int
    delivered_count: int
    suppressed_count: int
    dry_run: bool
    channels: tuple[str, ...]
    candidates: tuple[RiskAlert, ...]
    errors: tuple[dict[str, str], ...]

    def as_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["channels"] = list(self.channels)
        result["candidates"] = [asdict(candidate) for candidate in self.candidates]
        result["errors"] = list(self.errors)
        return result


class AlertEngine:
    """Evaluate the threshold rule and fan eligible alerts out to channels."""

    def __init__(
        self,
        repository: HighRiskAccountRepository,
        sinks: Sequence[NotificationSink],
        state_store: JsonAlertStateStore,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.repository = repository
        self.sinks = tuple(sinks)
        self.state_store = state_store
        self._now = now

    def run(
        self,
        *,
        threshold: float,
        limit: int,
        cooldown_hours: float,
        dry_run: bool = False,
    ) -> AlertRunSummary:
        if threshold < 0 or threshold > 100:
            raise ValueError("threshold must be between 0 and 100.")
        if limit < 1 or limit > 1_000:
            raise ValueError("limit must be between 1 and 1000.")
        if cooldown_hours < 0 or cooldown_hours > 8_760:
            raise ValueError("cooldown_hours must be between 0 and 8760.")
        if not dry_run and not self.sinks:
            raise ValueError(
                "No alert channel is configured. Set Slack/email variables or use --dry-run."
            )
        candidates = self.repository.find_candidates(
            minimum_risk_score=threshold,
            limit=limit,
        )
        if dry_run:
            return AlertRunSummary(
                threshold=threshold,
                candidate_count=len(candidates),
                delivered_count=0,
                suppressed_count=0,
                dry_run=True,
                channels=tuple(sink.name for sink in self.sinks),
                candidates=tuple(candidates),
                errors=(),
            )

        delivered_count = 0
        suppressed_count = 0
        errors: list[dict[str, str]] = []
        cooldown = timedelta(hours=cooldown_hours)
        for alert in candidates:
            for sink in self.sinks:
                current_time = self._now()
                if not self.state_store.should_deliver(
                    alert,
                    sink.name,
                    now=current_time,
                    cooldown=cooldown,
                ):
                    suppressed_count += 1
                    continue
                try:
                    sink.send(alert)
                except Exception as exc:
                    errors.append(
                        {
                            "account_id": alert.account_id,
                            "channel": sink.name,
                            "error": str(exc),
                        }
                    )
                    continue
                self.state_store.mark_delivered(
                    alert,
                    sink.name,
                    delivered_at=current_time,
                )
                delivered_count += 1

        return AlertRunSummary(
            threshold=threshold,
            candidate_count=len(candidates),
            delivered_count=delivered_count,
            suppressed_count=suppressed_count,
            dry_run=False,
            channels=tuple(sink.name for sink in self.sinks),
            candidates=tuple(candidates),
            errors=tuple(errors),
        )


def build_sinks(settings: AlertSettings) -> list[NotificationSink]:
    sinks: list[NotificationSink] = []
    if settings.slack_webhook_url:
        sinks.append(SlackWebhookSink(settings.slack_webhook_url))
    if settings.smtp_host and settings.email_from and settings.email_to:
        sinks.append(
            EmailSink(
                host=settings.smtp_host,
                port=settings.smtp_port,
                sender=settings.email_from,
                recipients=settings.email_to,
                username=settings.smtp_username,
                password=settings.smtp_password,
                use_tls=settings.smtp_use_tls,
            )
        )
    return sinks


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Send cooldown-aware Slack/email alerts for high-risk Neo4j accounts."
    )
    parser.add_argument("--risk-threshold", type=float)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--cooldown-hours", type=float)
    parser.add_argument("--state-path", type=Path)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print matching accounts without sending notifications or changing state.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = AlertSettings.from_environment()
    threshold = (
        settings.risk_threshold if args.risk_threshold is None else args.risk_threshold
    )
    limit = settings.candidate_limit if args.limit is None else args.limit
    cooldown_hours = (
        settings.cooldown_hours if args.cooldown_hours is None else args.cooldown_hours
    )
    if threshold < 0 or threshold > 100:
        raise ValueError("risk threshold must be between 0 and 100.")
    if limit < 1 or limit > 1_000:
        raise ValueError("limit must be between 1 and 1000.")
    if cooldown_hours < 0 or cooldown_hours > 8_760:
        raise ValueError("cooldown hours must be between 0 and 8760.")

    repository = HighRiskAccountRepository()
    try:
        repository.open()
        summary = AlertEngine(
            repository,
            build_sinks(settings),
            JsonAlertStateStore(args.state_path or settings.state_path),
        ).run(
            threshold=threshold,
            limit=limit,
            cooldown_hours=cooldown_hours,
            dry_run=args.dry_run,
        )
    finally:
        repository.close()

    print(json.dumps(summary.as_dict(), default=str, sort_keys=True))
    return 1 if summary.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
