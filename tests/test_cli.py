from unittest.mock import Mock, patch
import json
import unittest

from fingraph import cli
from fingraph.kafka_topic import TopicProvisioningResult


class ProvisionCommandTests(unittest.TestCase):
    @patch("builtins.print")
    @patch("fingraph.cli._topic_provisioners")
    def test_provision_creates_transaction_and_dead_letter_topics(self, provisioners, output):
        transaction = Mock()
        transaction.ensure_topic.return_value = TopicProvisioningResult(
            "fingraph.transactions.v1", True, 3, 1
        )
        dead_letter = Mock()
        dead_letter.ensure_topic.return_value = TopicProvisioningResult(
            "fingraph.transactions.dlq.v1", False, 3, 1
        )
        provisioners.return_value = [transaction, dead_letter]

        self.assertEqual(cli.main(["provision"]), 0)

        payload = json.loads(output.call_args.args[0])
        self.assertEqual([topic["topic"] for topic in payload["topics"]], [
            "fingraph.transactions.v1", "fingraph.transactions.dlq.v1"
        ])


if __name__ == "__main__":
    unittest.main()
