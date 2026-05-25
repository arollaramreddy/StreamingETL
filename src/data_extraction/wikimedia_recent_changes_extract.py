import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# I need kafka-python so this script can publish messages to a Kafka broker.
try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError as exc:
    raise SystemExit(
        "Missing Kafka dependency. Install it with: uv add kafka-python"
    ) from exc

# I import my Wikimedia extractor file as a module, so I can reuse its stream code.
try:
    from . import wikimedia_recent_changes_extract
except ImportError:
    # This fallback keeps `python src/data_extraction/kafka_extraction.py` working.
    import wikimedia_recent_changes_extract


PROJECT_ROOT = Path(__file__).resolve().parents[2]

# I load my root .env first, so the os.getenv defaults below can use it.
load_dotenv(PROJECT_ROOT / ".env")


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


# I keep these in .env so local Kafka can become Event Hubs later without code edits.
DEFAULT_BOOTSTRAP_SERVERS = get_required_env("KAFKA_BOOTSTRAP_SERVERS")
DEFAULT_TOPIC = get_required_env("KAFKA_TOPIC")
DEFAULT_PRODUCER_ACKS = os.getenv("KAFKA_PRODUCER_ACKS", "all")
DEFAULT_PRODUCER_RETRIES = int(os.getenv("KAFKA_PRODUCER_RETRIES", "5"))
DEFAULT_PRODUCER_LINGER_MS = int(os.getenv("KAFKA_PRODUCER_LINGER_MS", "100"))


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    # I allow multiple brokers here, like "localhost:9092,localhost:9093".
    servers = [
        server.strip()
        for server in bootstrap_servers.split(",")
        if server.strip()
    ]

    producer_config: dict[str, Any] = {
        "bootstrap_servers": servers,
        # Kafka expects bytes, so I turn each Python dict into JSON bytes.
        "value_serializer": lambda value: json.dumps(value).encode("utf-8"),
        # I use a key when I can, so related edits land on the same partition.
        "key_serializer": lambda key: key.encode("utf-8") if key else None,
        # I wait for Kafka to acknowledge the message before treating it as sent.
        "acks": DEFAULT_PRODUCER_ACKS,
        "retries": DEFAULT_PRODUCER_RETRIES,
        # I give Kafka a tiny window to batch nearby events together.
        "linger_ms": DEFAULT_PRODUCER_LINGER_MS,
    }

    # If I later use a secured Kafka cluster, I keep those secrets in .env.
    kafka_security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL")
    kafka_sasl_mechanism = os.getenv("KAFKA_SASL_MECHANISM")
    kafka_username = os.getenv("KAFKA_USERNAME")
    kafka_password = os.getenv("KAFKA_PASSWORD")

    if kafka_security_protocol:
        producer_config["security_protocol"] = kafka_security_protocol
    if kafka_sasl_mechanism:
        producer_config["sasl_mechanism"] = kafka_sasl_mechanism
    if kafka_username:
        producer_config["sasl_plain_username"] = kafka_username
    if kafka_password:
        producer_config["sasl_plain_password"] = kafka_password

    return KafkaProducer(**producer_config)


def get_message_key(change: dict[str, Any]) -> str | None:
    # I try to build a readable key, so edits for the same page stay grouped.
    wiki = change.get("wiki")
    title = change.get("title")

    if wiki and title:
        return f"{wiki}:{title}"

    if wiki:
        return str(wiki)

    meta = change.get("meta")
    if isinstance(meta, dict) and meta.get("id"):
        return str(meta["id"])

    return None


def on_send_success(record_metadata: Any) -> None:
    # I log where Kafka stored the message after the broker accepts it.
    logging.info(
        "Produced message to %s partition=%s offset=%s",
        record_metadata.topic,
        record_metadata.partition,
        record_metadata.offset,
    )


def on_send_error(error: KafkaError) -> None:
    logging.error("Failed to produce Kafka message: %s", error)


def produce_recent_changes(
    producer: KafkaProducer,
    topic: str,
    stream_url: str,
    headers: dict[str, str],
    limit: int | None = None,
) -> int:
    produced_count = 0

    # I reuse my Wikimedia extractor here, so this file only handles Kafka work.
    for change in wikimedia_recent_changes_extract.iter_recent_changes(
        stream_url,
        headers,
    ):
        message_key = get_message_key(change)

        # producer.send() is async, so these callbacks tell me what happened.
        future = producer.send(topic, key=message_key, value=change)
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)

        produced_count += 1

        # I use --limit for quick tests; without it, the stream keeps running.
        if limit is not None and produced_count >= limit:
            break

    # I flush before returning so any buffered messages get sent to Kafka.
    producer.flush()
    return produced_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce Wikimedia recent-change events to a Kafka topic."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help=(
            "Comma-separated Kafka bootstrap servers. "
            "Use localhost:9092 for Docker Compose."
        ),
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help="Kafka topic that receives Wikimedia recent-change events.",
    )
    parser.add_argument(
        "--stream-url",
        default=wikimedia_recent_changes_extract.STREAM_URL,
        help="Wikimedia SSE stream URL.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after producing this many events. Use 0 or omit to run continuously.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="Logging level.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.limit is not None and args.limit <= 0:
        args.limit = None

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # I create one producer and reuse it for the whole stream.
    producer = create_kafka_producer(args.bootstrap_servers)

    try:
        produced_count = produce_recent_changes(
            producer=producer,
            topic=args.topic,
            stream_url=args.stream_url,
            headers=wikimedia_recent_changes_extract.HEADERS,
            limit=args.limit,
        )
        logging.info("Produced %s Wikimedia event(s).", produced_count)
    except KeyboardInterrupt:
        logging.info("Stopping Wikimedia Kafka producer.")
    finally:
        # I always flush and close so I do not leave messages stuck in memory.
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
