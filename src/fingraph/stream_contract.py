"""Validation and canonicalisation for transaction events entering the stream."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import ipaddress
import re
from typing import Any


class EventValidationError(ValueError):
    """Raised when an inbound transaction event cannot safely enter the graph."""


EVENT_TYPE = "transaction.created"
EVENT_VERSION = 1
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_COUNTRY_PATTERN = re.compile(r"^[A-Z]{2}$")
_CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")
_ACCOUNT_TYPES = frozenset({"checking", "savings", "business"})
_ENTITY_TYPES = frozenset({"individual", "business", "shell_company"})
_RISK_TIERS = frozenset({"low", "medium", "high", "critical"})
_CHANNELS = frozenset({"api", "branch", "mobile", "web"})
_MAX_AMOUNT = Decimal("1000000000.00")


def normalise_transaction_event(event: Mapping[str, Any]) -> dict[str, object]:
    """Return a graph-safe canonical event or raise a descriptive validation error.

    Kafka is an untrusted boundary: the Flink job must not insert malformed
    account identities, invalid money values, or timestamps without timezones
    into Neo4j. This function intentionally returns ordinary dictionaries so
    it can be called from either a Python smoke test or a Flink map function.
    """
    root = _mapping(event, "event")
    event_type = _required_string(root, "event_type")
    if event_type != EVENT_TYPE:
        raise EventValidationError(
            f"event.event_type must be '{EVENT_TYPE}', received '{event_type}'."
        )
    event_version = root.get("event_version")
    if event_version != EVENT_VERSION:
        raise EventValidationError(
            f"event.event_version must be {EVENT_VERSION}, received {event_version!r}."
        )

    transaction = _normalise_transaction(_mapping(root.get("transaction"), "transaction"))
    source = _normalise_account(_mapping(root.get("source_account"), "source_account"))
    destination = _normalise_account(
        _mapping(root.get("destination_account"), "destination_account")
    )

    if transaction["source_account_id"] != source["account_id"]:
        raise EventValidationError(
            "transaction.source_account_id must match source_account.account_id."
        )
    if transaction["destination_account_id"] != destination["account_id"]:
        raise EventValidationError(
            "transaction.destination_account_id must match destination_account.account_id."
        )
    if source["account_id"] == destination["account_id"]:
        raise EventValidationError("source_account and destination_account must differ.")

    return {
        "event_type": EVENT_TYPE,
        "event_version": EVENT_VERSION,
        "transaction": transaction,
        "source_account": source,
        "destination_account": destination,
    }


def _normalise_transaction(transaction: Mapping[str, Any]) -> dict[str, object]:
    source_id = _identifier(transaction, "source_account_id", "transaction")
    destination_id = _identifier(transaction, "destination_account_id", "transaction")
    if source_id == destination_id:
        raise EventValidationError("transaction source and destination account IDs must differ.")
    currency = _required_string(transaction, "currency", "transaction").upper()
    if not _CURRENCY_PATTERN.fullmatch(currency):
        raise EventValidationError("transaction.currency must be a three-letter ISO-4217 code.")
    channel = _required_string(transaction, "channel", "transaction").lower()
    if channel not in _CHANNELS:
        raise EventValidationError(
            f"transaction.channel must be one of {sorted(_CHANNELS)}."
        )
    syndicate_id = transaction.get("syndicate_id")
    if syndicate_id is not None:
        syndicate_id = _normalise_identifier(syndicate_id, "transaction.syndicate_id")

    return {
        "transaction_id": _identifier(transaction, "transaction_id", "transaction"),
        "source_account_id": source_id,
        "destination_account_id": destination_id,
        "amount": _normalise_amount(transaction.get("amount")),
        "currency": currency,
        "occurred_at": _normalise_timestamp(transaction.get("occurred_at")),
        "origin_ip": _normalise_ip(transaction.get("origin_ip")),
        "channel": channel,
        "syndicate_id": syndicate_id,
        "risk_indicators": _normalise_indicators(transaction.get("risk_indicators")),
    }


def _normalise_account(account: Mapping[str, Any]) -> dict[str, str]:
    country = _required_string(account, "country", "account").upper()
    if not _COUNTRY_PATTERN.fullmatch(country):
        raise EventValidationError("account.country must be a two-letter ISO-3166 code.")
    person_country = _required_string(account, "person_country", "account").upper()
    if not _COUNTRY_PATTERN.fullmatch(person_country):
        raise EventValidationError("account.person_country must be a two-letter ISO-3166 code.")
    bank_country = _required_string(account, "bank_country", "account").upper()
    if not _COUNTRY_PATTERN.fullmatch(bank_country):
        raise EventValidationError("account.bank_country must be a two-letter ISO-3166 code.")
    account_type = _required_string(account, "account_type", "account").lower()
    if account_type not in _ACCOUNT_TYPES:
        raise EventValidationError(f"account.account_type must be one of {sorted(_ACCOUNT_TYPES)}.")
    person_type = _required_string(account, "person_type", "account").lower()
    if person_type not in _ENTITY_TYPES:
        raise EventValidationError(f"account.person_type must be one of {sorted(_ENTITY_TYPES)}.")
    risk_tier = _required_string(account, "risk_tier", "account").lower()
    if risk_tier not in _RISK_TIERS:
        raise EventValidationError(f"account.risk_tier must be one of {sorted(_RISK_TIERS)}.")

    return {
        "account_id": _identifier(account, "account_id", "account"),
        "person_id": _identifier(account, "person_id", "account"),
        "bank_id": _identifier(account, "bank_id", "account"),
        "country": country,
        "account_type": account_type,
        "risk_tier": risk_tier,
        "person_name": _required_string(account, "person_name", "account"),
        "person_country": person_country,
        "person_type": person_type,
        "bank_name": _required_string(account, "bank_name", "account"),
        "bank_country": bank_country,
    }


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EventValidationError(f"{field} must be an object.")
    return value


def _required_string(payload: Mapping[str, Any], field: str, context: str = "event") -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not (normalised := value.strip()):
        raise EventValidationError(f"{context}.{field} must be a non-empty string.")
    return normalised


def _identifier(payload: Mapping[str, Any], field: str, context: str) -> str:
    return _normalise_identifier(payload.get(field), f"{context}.{field}")


def _normalise_identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER_PATTERN.fullmatch(value.strip()):
        raise EventValidationError(
            f"{field} must be 1-128 characters using letters, digits, '.', '_', ':', or '-'."
        )
    return value.strip()


def _normalise_amount(value: Any) -> str:
    if isinstance(value, bool) or value is None:
        raise EventValidationError("transaction.amount must be a positive decimal value.")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise EventValidationError("transaction.amount must be a positive decimal value.") from exc
    if not amount.is_finite() or amount <= 0 or amount > _MAX_AMOUNT:
        raise EventValidationError("transaction.amount must be between 0.01 and 1000000000.00.")
    normalised = amount.quantize(Decimal("0.01"))
    if normalised != amount:
        raise EventValidationError("transaction.amount must have at most two decimal places.")
    return format(normalised, "f")


def _normalise_timestamp(value: Any) -> str:
    if not isinstance(value, str):
        raise EventValidationError("transaction.occurred_at must be an ISO-8601 timestamp.")
    try:
        timestamp = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise EventValidationError("transaction.occurred_at must be an ISO-8601 timestamp.") from exc
    if timestamp.tzinfo is None:
        raise EventValidationError("transaction.occurred_at must include a timezone offset.")
    return timestamp.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalise_ip(value: Any) -> str:
    if not isinstance(value, str):
        raise EventValidationError("transaction.origin_ip must be an IP address.")
    try:
        return str(ipaddress.ip_address(value.strip()))
    except ValueError as exc:
        raise EventValidationError("transaction.origin_ip must be an IP address.") from exc


def _normalise_indicators(value: Any) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise EventValidationError("transaction.risk_indicators must be a list of strings.")
    indicators = sorted({item.strip() for item in value if item.strip()})
    if len(indicators) != len(value):
        raise EventValidationError(
            "transaction.risk_indicators cannot contain blank or duplicate values."
        )
    return indicators
