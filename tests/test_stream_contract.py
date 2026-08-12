from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import unittest

from fingraph.simulator import TransactionNetworkSimulator
from fingraph.stream_contract import EventValidationError, normalise_transaction_event


class StreamContractTests(unittest.TestCase):
    def setUp(self) -> None:
        fixture = TransactionNetworkSimulator(
            seed=14,
            start_at=datetime(2026, 8, 12, 9, 0, tzinfo=timezone.utc),
        ).generate(normal_transaction_count=1, syndicate_source_count=2, intermediary_count=1)
        self.event = next(fixture.events())

    def test_normalises_safe_event_fields(self) -> None:
        event = deepcopy(self.event)
        event["transaction"]["currency"] = " usd "
        event["transaction"]["channel"] = " WEB "
        event["transaction"]["occurred_at"] = "2026-08-12T14:30:00+05:30"
        event["transaction"]["risk_indicators"] = [" watchlist ", "structuring"]
        event["source_account"]["country"] = " us "

        cleaned = normalise_transaction_event(event)

        self.assertEqual("USD", cleaned["transaction"]["currency"])
        self.assertEqual("web", cleaned["transaction"]["channel"])
        self.assertEqual("2026-08-12T09:00:00Z", cleaned["transaction"]["occurred_at"])
        self.assertEqual(["structuring", "watchlist"], cleaned["transaction"]["risk_indicators"])
        self.assertEqual("US", cleaned["source_account"]["country"])

    def test_rejects_account_identity_mismatch(self) -> None:
        event = deepcopy(self.event)
        event["transaction"]["source_account_id"] = "account-other-001"

        with self.assertRaisesRegex(EventValidationError, "source_account_id"):
            normalise_transaction_event(event)

    def test_rejects_invalid_money_timezone_and_ip(self) -> None:
        for field, invalid_value, error in (
            ("amount", "14.999", "two decimal"),
            ("occurred_at", "2026-08-12T09:00:00", "timezone"),
            ("origin_ip", "not-an-ip", "IP address"),
        ):
            with self.subTest(field=field):
                event = deepcopy(self.event)
                event["transaction"][field] = invalid_value
                with self.assertRaisesRegex(EventValidationError, error):
                    normalise_transaction_event(event)

    def test_rejects_unsafe_identifier_and_duplicate_indicator(self) -> None:
        event = deepcopy(self.event)
        event["source_account"]["bank_id"] = "bank with spaces"
        with self.assertRaisesRegex(EventValidationError, "bank_id"):
            normalise_transaction_event(event)

        event = deepcopy(self.event)
        event["transaction"]["risk_indicators"] = ["structuring", "structuring"]
        with self.assertRaisesRegex(EventValidationError, "duplicate"):
            normalise_transaction_event(event)


if __name__ == "__main__":
    unittest.main()
