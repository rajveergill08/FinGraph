"""Immutable domain records shared by the simulator and stream publisher."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime


@dataclass(frozen=True, slots=True)
class Bank:
    bank_id: str
    name: str
    country: str


@dataclass(frozen=True, slots=True)
class Person:
    person_id: str
    name: str
    country: str
    entity_type: str = "individual"


@dataclass(frozen=True, slots=True)
class Account:
    account_id: str
    person_id: str
    bank_id: str
    country: str
    account_type: str = "checking"
    risk_tier: str = "low"


@dataclass(frozen=True, slots=True)
class Transaction:
    transaction_id: str
    source_account_id: str
    destination_account_id: str
    amount_cents: int
    currency: str
    occurred_at: datetime
    origin_ip: str
    channel: str
    syndicate_id: str | None = None
    risk_indicators: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.source_account_id == self.destination_account_id:
            raise ValueError("A transaction must connect two different accounts.")
        if self.amount_cents <= 0:
            raise ValueError("A transaction amount must be positive.")
        if len(self.currency) != 3:
            raise ValueError("Currency must be an ISO-4217 code.")

    @property
    def amount(self) -> str:
        """Return a currency-safe decimal string without float rounding errors."""
        return f"{self.amount_cents // 100}.{self.amount_cents % 100:02d}"

    def event_fields(self) -> dict[str, object]:
        return {
            "transaction_id": self.transaction_id,
            "source_account_id": self.source_account_id,
            "destination_account_id": self.destination_account_id,
            "amount": self.amount,
            "currency": self.currency,
            "occurred_at": self.occurred_at.isoformat(),
            "origin_ip": self.origin_ip,
            "channel": self.channel,
            "syndicate_id": self.syndicate_id,
            "risk_indicators": list(self.risk_indicators),
        }


def account_event_fields(account: Account, person: Person, bank: Bank) -> dict[str, str]:
    """Flatten account ownership metadata for Kafka and Cypher map access."""
    payload = asdict(account)
    payload.update(
        {
            "person_name": person.name,
            "person_country": person.country,
            "person_type": person.entity_type,
            "bank_name": bank.name,
            "bank_country": bank.country,
        }
    )
    return payload
