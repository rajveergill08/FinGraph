"""Deterministic, graph-shaped mock transaction data for local development."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import random
from typing import Iterable

from .models import Account, Bank, Person, Transaction, account_event_fields


FIRST_NAMES = (
    "Aarav", "Anika", "Camila", "Darius", "Elena", "Farah", "Hiro", "Imani",
    "Jonas", "Kiara", "Lina", "Mateo", "Nadia", "Omar", "Priya", "Ravi",
    "Sofia", "Theo", "Uma", "Zara",
)
LAST_NAMES = (
    "Bennett", "Chen", "Das", "Evans", "Fernandez", "Gupta", "Haddad", "Ibrahim",
    "Khan", "Larsen", "Moreno", "Nguyen", "Okafor", "Patel", "Quinn", "Rossi",
    "Singh", "Taylor", "Varga", "Williams",
)


@dataclass(frozen=True, slots=True)
class SyndicateSummary:
    syndicate_id: str
    source_account_ids: tuple[str, ...]
    intermediary_account_ids: tuple[str, ...]
    shell_account_id: str


@dataclass(frozen=True, slots=True)
class TransactionFixture:
    """A complete in-memory network and its stream-ready transaction events."""

    banks: dict[str, Bank]
    people: dict[str, Person]
    accounts: dict[str, Account]
    transactions: tuple[Transaction, ...]
    syndicate: SyndicateSummary

    def events(self) -> Iterable[dict[str, object]]:
        for transaction in self.transactions:
            source = self.accounts[transaction.source_account_id]
            destination = self.accounts[transaction.destination_account_id]
            yield {
                "event_type": "transaction.created",
                "event_version": 1,
                "transaction": transaction.event_fields(),
                "source_account": account_event_fields(
                    source, self.people[source.person_id], self.banks[source.bank_id]
                ),
                "destination_account": account_event_fields(
                    destination,
                    self.people[destination.person_id],
                    self.banks[destination.bank_id],
                ),
            }

    def summary(self) -> dict[str, object]:
        return {
            "banks": len(self.banks),
            "people": len(self.people),
            "accounts": len(self.accounts),
            "transactions": len(self.transactions),
            "syndicate_id": self.syndicate.syndicate_id,
            "syndicate_source_accounts": len(self.syndicate.source_account_ids),
            "syndicate_intermediaries": len(self.syndicate.intermediary_account_ids),
            "shell_account_id": self.syndicate.shell_account_id,
        }


class TransactionNetworkSimulator:
    """Generate realistic normal activity plus a deliberately detectable AML pattern.

    The synthetic starburst is multi-hop: many unrelated, unique-IP source
    accounts submit USD 9,900 transfers to a small set of intermediaries, which
    funnel the aggregate onward to an offshore shell account.
    """

    def __init__(self, *, seed: int = 42, start_at: datetime | None = None) -> None:
        self._random = random.Random(seed)
        self._start_at = start_at or datetime.now(timezone.utc).replace(microsecond=0)
        if self._start_at.tzinfo is None:
            raise ValueError("start_at must be timezone-aware.")

    def generate(
        self,
        *,
        normal_transaction_count: int = 20,
        syndicate_source_count: int = 50,
        intermediary_count: int = 5,
    ) -> TransactionFixture:
        if normal_transaction_count < 0:
            raise ValueError("normal_transaction_count cannot be negative.")
        if syndicate_source_count < 2:
            raise ValueError("syndicate_source_count must be at least two.")
        if intermediary_count < 1:
            raise ValueError("intermediary_count must be at least one.")

        banks = self._create_banks()
        people: dict[str, Person] = {}
        accounts: dict[str, Account] = {}
        transactions: list[Transaction] = []

        normal_account_ids = self._create_normal_accounts(people, accounts, banks)
        for index in range(normal_transaction_count):
            source, destination = self._pick_distinct(normal_account_ids)
            transactions.append(
                Transaction(
                    transaction_id=f"normal-{index + 1:05d}",
                    source_account_id=source,
                    destination_account_id=destination,
                    amount_cents=self._random.randint(2_500, 350_000),
                    currency="USD",
                    occurred_at=self._timestamp(len(transactions)),
                    origin_ip=f"203.0.113.{(index % 200) + 1}",
                    channel=self._random.choice(("mobile", "web", "branch")),
                )
            )

        syndicate, syndicate_transactions = self._create_starburst_syndicate(
            people=people,
            accounts=accounts,
            banks=banks,
            source_count=syndicate_source_count,
            intermediary_count=intermediary_count,
            transaction_offset=len(transactions),
        )
        transactions.extend(syndicate_transactions)

        return TransactionFixture(
            banks=banks,
            people=people,
            accounts=accounts,
            transactions=tuple(transactions),
            syndicate=syndicate,
        )

    def _create_banks(self) -> dict[str, Bank]:
        bank_list = (
            Bank("bank-us-atlantic", "Atlantic Retail Bank", "US"),
            Bank("bank-gb-northstar", "Northstar Commercial", "GB"),
            Bank("bank-de-rhein", "Rhein Union Bank", "DE"),
            Bank("bank-ae-gulf", "Gulf Crescent Bank", "AE"),
            Bank("bank-pa-offshore", "Pacific Meridian Trust", "PA"),
        )
        return {bank.bank_id: bank for bank in bank_list}

    def _create_normal_accounts(
        self,
        people: dict[str, Person],
        accounts: dict[str, Account],
        banks: dict[str, Bank],
        *,
        count: int = 20,
    ) -> tuple[str, ...]:
        bank_ids = tuple(bank_id for bank_id in banks if bank_id != "bank-pa-offshore")
        account_ids: list[str] = []
        for index in range(count):
            person = Person(
                person_id=f"person-normal-{index + 1:03d}",
                name=self._person_name(index),
                country=("US", "GB", "DE", "AE")[index % 4],
            )
            account = Account(
                account_id=f"account-normal-{index + 1:03d}",
                person_id=person.person_id,
                bank_id=bank_ids[index % len(bank_ids)],
                country=person.country,
            )
            people[person.person_id] = person
            accounts[account.account_id] = account
            account_ids.append(account.account_id)
        return tuple(account_ids)

    def _create_starburst_syndicate(
        self,
        *,
        people: dict[str, Person],
        accounts: dict[str, Account],
        banks: dict[str, Bank],
        source_count: int,
        intermediary_count: int,
        transaction_offset: int,
    ) -> tuple[SyndicateSummary, list[Transaction]]:
        syndicate_id = "syndicate-starburst-001"
        source_account_ids: list[str] = []
        intermediary_account_ids: list[str] = []
        source_banks = ("bank-us-atlantic", "bank-gb-northstar", "bank-de-rhein")

        for index in range(source_count):
            person = Person(
                person_id=f"person-source-{index + 1:03d}",
                name=self._person_name(index + 100),
                country=("US", "GB", "DE")[index % 3],
            )
            account = Account(
                account_id=f"account-source-{index + 1:03d}",
                person_id=person.person_id,
                bank_id=source_banks[index % len(source_banks)],
                country=person.country,
                risk_tier="medium",
            )
            people[person.person_id] = person
            accounts[account.account_id] = account
            source_account_ids.append(account.account_id)

        for index in range(intermediary_count):
            person = Person(
                person_id=f"person-intermediary-{index + 1:03d}",
                name=f"Mercury Trading {index + 1:02d}",
                country="AE",
                entity_type="business",
            )
            account = Account(
                account_id=f"account-intermediary-{index + 1:03d}",
                person_id=person.person_id,
                bank_id="bank-ae-gulf",
                country="AE",
                account_type="business",
                risk_tier="high",
            )
            people[person.person_id] = person
            accounts[account.account_id] = account
            intermediary_account_ids.append(account.account_id)

        shell_person = Person(
            person_id="person-shell-001",
            name="Seabrook Holdings Ltd",
            country="PA",
            entity_type="shell_company",
        )
        shell_account = Account(
            account_id="account-shell-001",
            person_id=shell_person.person_id,
            bank_id="bank-pa-offshore",
            country="PA",
            account_type="business",
            risk_tier="critical",
        )
        people[shell_person.person_id] = shell_person
        accounts[shell_account.account_id] = shell_account

        transfers: list[Transaction] = []
        amount_cents = 990_000  # USD 9,900.00: below the common USD 10,000 threshold.
        indicators = (
            "sub_threshold_transfer",
            "distinct_source_ips",
            "funnel_to_offshore",
        )
        for index, source_account_id in enumerate(source_account_ids):
            transfers.append(
                Transaction(
                    transaction_id=f"starburst-in-{index + 1:05d}",
                    source_account_id=source_account_id,
                    destination_account_id=intermediary_account_ids[index % intermediary_count],
                    amount_cents=amount_cents,
                    currency="USD",
                    occurred_at=self._timestamp(transaction_offset + len(transfers)),
                    origin_ip=f"198.51.100.{index + 1}",
                    channel="web",
                    syndicate_id=syndicate_id,
                    risk_indicators=indicators,
                )
            )

        for index, intermediary_account_id in enumerate(intermediary_account_ids):
            source_count_for_intermediary = len(source_account_ids[index::intermediary_count])
            transfers.append(
                Transaction(
                    transaction_id=f"starburst-out-{index + 1:05d}",
                    source_account_id=intermediary_account_id,
                    destination_account_id=shell_account.account_id,
                    amount_cents=amount_cents * source_count_for_intermediary,
                    currency="USD",
                    occurred_at=self._timestamp(transaction_offset + len(transfers)),
                    origin_ip=f"192.0.2.{index + 1}",
                    channel="api",
                    syndicate_id=syndicate_id,
                    risk_indicators=("funnel_to_offshore", "aggregate_smurfing"),
                )
            )

        return (
            SyndicateSummary(
                syndicate_id=syndicate_id,
                source_account_ids=tuple(source_account_ids),
                intermediary_account_ids=tuple(intermediary_account_ids),
                shell_account_id=shell_account.account_id,
            ),
            transfers,
        )

    def _person_name(self, offset: int) -> str:
        return f"{FIRST_NAMES[offset % len(FIRST_NAMES)]} {LAST_NAMES[(offset * 3) % len(LAST_NAMES)]}"

    def _pick_distinct(self, account_ids: tuple[str, ...]) -> tuple[str, str]:
        source = self._random.choice(account_ids)
        destination = self._random.choice(account_ids)
        while destination == source:
            destination = self._random.choice(account_ids)
        return source, destination

    def _timestamp(self, offset: int) -> datetime:
        return self._start_at + timedelta(seconds=offset * 5)
