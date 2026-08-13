"""Command-line entry points for producing FinGraph's mock data stream."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import os
from typing import Sequence

from .kafka_publisher import KafkaPublishError, KafkaSettings, KafkaTransactionPublisher
from .kafka_topic import KafkaTopicError, KafkaTopicProvisioner
from .simulator import TransactionNetworkSimulator


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate and publish FinGraph's graph-shaped transaction events."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="Write generated events as JSON Lines.")
    publish = subparsers.add_parser("publish", help="Generate and publish events to Kafka.")
    subparsers.add_parser("provision", help="Create the FinGraph Kafka topics if missing.")
    for command in (generate, publish):
        command.add_argument("--seed", type=int, default=42, help="Seed for repeatable data.")
        command.add_argument(
            "--normal-transactions",
            type=int,
            default=20,
            help="Number of non-syndicate transactions to add.",
        )
        command.add_argument(
            "--syndicate-sources",
            type=int,
            default=50,
            help="Distinct source accounts in the starburst syndicate.",
        )
        command.add_argument(
            "--intermediaries",
            type=int,
            default=5,
            help="Intermediary accounts that funnel transfers to the shell company.",
        )
        command.add_argument(
            "--start-at",
            type=_parse_datetime,
            default=None,
            help="Timezone-aware ISO-8601 start timestamp for deterministic replay.",
        )
    generate.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Path for JSON Lines output; parent directories are created.",
    )
    return parser


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("--start-at must include a timezone, for example +00:00.")
    return parsed


def _fixture_from_args(args: argparse.Namespace):
    return TransactionNetworkSimulator(seed=args.seed, start_at=args.start_at).generate(
        normal_transaction_count=args.normal_transactions,
        syndicate_source_count=args.syndicate_sources,
        intermediary_count=args.intermediaries,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "provision":
        try:
            results = [provisioner.ensure_topic() for provisioner in _topic_provisioners()]
        except KafkaTopicError as exc:
            parser.error(str(exc))
        print(
            json.dumps(
                {
                    "status": "provisioned",
                    "topics": [
                        {
                            "status": "created" if result.created else "already_exists",
                            "topic": result.topic,
                            "partitions": result.partitions,
                            "replication_factor": result.replication_factor,
                        }
                        for result in results
                    ],
                }
            )
        )
        return 0

    fixture = _fixture_from_args(args)

    if args.command == "generate":
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("w", encoding="utf-8", newline="\n") as stream:
            for event in fixture.events():
                stream.write(json.dumps(event, sort_keys=True))
                stream.write("\n")
        print(json.dumps({"status": "generated", "output": str(args.output), **fixture.summary()}))
        return 0

    try:
        published = KafkaTransactionPublisher().publish(fixture.events())
    except KafkaPublishError as exc:
        parser.error(str(exc))
    print(json.dumps({"status": "published", "published_events": published, **fixture.summary()}))
    return 0


def _topic_provisioner() -> KafkaTopicProvisioner:
    return _topic_provisioners()[0]


def _topic_provisioners() -> list[KafkaTopicProvisioner]:
    settings = KafkaSettings.from_environment()
    topics = [
        settings.transaction_topic,
        os.getenv("KAFKA_DEAD_LETTER_TOPIC", "fingraph.transactions.dlq.v1"),
    ]
    return [
        KafkaTopicProvisioner(
            bootstrap_servers=settings.bootstrap_servers,
            topic=topic,
            partitions=settings.transaction_topic_partitions,
            replication_factor=settings.transaction_topic_replication_factor,
            client_id=settings.client_id,
        )
        for topic in dict.fromkeys(topics)
    ]


if __name__ == "__main__":
    raise SystemExit(main())
