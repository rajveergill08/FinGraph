from __future__ import annotations

from datetime import datetime, timezone
import json
import unittest

from fingraph.simulator import TransactionNetworkSimulator


class TransactionNetworkSimulatorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = TransactionNetworkSimulator(
            seed=7,
            start_at=datetime(2026, 8, 10, 9, 0, tzinfo=timezone.utc),
        ).generate(normal_transaction_count=12, syndicate_source_count=50, intermediary_count=5)

    def test_starburst_has_distinct_sources_and_ips(self) -> None:
        syndrome = self.fixture.syndicate
        incoming = [
            transaction
            for transaction in self.fixture.transactions
            if transaction.transaction_id.startswith("starburst-in-")
        ]

        self.assertEqual(50, len(syndrome.source_account_ids))
        self.assertEqual(50, len({transaction.source_account_id for transaction in incoming}))
        self.assertEqual(50, len({transaction.origin_ip for transaction in incoming}))
        self.assertTrue(all(transaction.amount_cents == 990_000 for transaction in incoming))
        self.assertTrue(all(transaction.amount_cents < 1_000_000 for transaction in incoming))

    def test_intermediaries_funnel_all_starburst_value_to_the_shell(self) -> None:
        syndrome = self.fixture.syndicate
        incoming = [
            transaction
            for transaction in self.fixture.transactions
            if transaction.transaction_id.startswith("starburst-in-")
        ]
        outgoing = [
            transaction
            for transaction in self.fixture.transactions
            if transaction.transaction_id.startswith("starburst-out-")
        ]

        self.assertEqual(5, len(outgoing))
        self.assertTrue(
            all(
                transaction.destination_account_id == syndrome.shell_account_id
                for transaction in outgoing
            )
        )
        self.assertEqual(sum(transaction.amount_cents for transaction in incoming), sum(transaction.amount_cents for transaction in outgoing))

    def test_events_are_json_serialisable_and_graph_complete(self) -> None:
        event = next(self.fixture.events())
        encoded = json.dumps(event)

        self.assertIn('"event_type": "transaction.created"', encoded)
        self.assertEqual(1, event["event_version"])
        self.assertIn("transaction_id", event["transaction"])
        self.assertIn("person_id", event["source_account"])
        self.assertIn("bank_id", event["destination_account"])

    def test_fixture_contains_a_time_ordered_three_account_cycle(self) -> None:
        cycle = [
            transaction
            for transaction in self.fixture.transactions
            if transaction.transaction_id.startswith("circular-")
        ]

        self.assertEqual(3, len(cycle))
        self.assertEqual(
            [
                ("account-normal-001", "account-normal-002"),
                ("account-normal-002", "account-normal-003"),
                ("account-normal-003", "account-normal-001"),
            ],
            [
                (transaction.source_account_id, transaction.destination_account_id)
                for transaction in cycle
            ],
        )
        self.assertEqual(
            sorted(transaction.occurred_at for transaction in cycle),
            [transaction.occurred_at for transaction in cycle],
        )
        self.assertEqual(
            ("account-normal-001", "account-normal-002", "account-normal-003"),
            self.fixture.circular_flow_account_ids,
        )

    def test_rejects_an_invalid_syndicate_shape(self) -> None:
        simulator = TransactionNetworkSimulator(
            start_at=datetime(2026, 8, 10, 9, 0, tzinfo=timezone.utc)
        )
        with self.assertRaises(ValueError):
            simulator.generate(syndicate_source_count=1)

    def test_transaction_ids_are_replayable_per_seed_and_distinct_across_seeds(self) -> None:
        arguments = {
            "normal_transaction_count": 2,
            "syndicate_source_count": 2,
            "intermediary_count": 1,
        }
        first = TransactionNetworkSimulator(seed=1).generate(**arguments)
        replay = TransactionNetworkSimulator(seed=1).generate(**arguments)
        other = TransactionNetworkSimulator(seed=2).generate(**arguments)
        first_ids = {transaction.transaction_id for transaction in first.transactions}

        self.assertEqual(
            first_ids,
            {transaction.transaction_id for transaction in replay.transactions},
        )
        self.assertTrue(
            first_ids.isdisjoint(
                transaction.transaction_id for transaction in other.transactions
            )
        )


if __name__ == "__main__":
    unittest.main()
