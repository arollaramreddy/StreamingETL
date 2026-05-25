import json
import logging
import os
from pathlib import Path
from typing import Any, Iterator

from dotenv import load_dotenv
from requests_sse import EventSource


PROJECT_ROOT = Path(__file__).resolve().parents[2]

# I load my root .env so I can change source settings without editing code.
load_dotenv(PROJECT_ROOT / ".env")

STREAM_URL = os.getenv(
    "WIKIMEDIA_STREAM_URL",
    "https://stream.wikimedia.org/v2/stream/recentchange",
)

# I send a User-Agent because Wikimedia asks stream clients to identify themselves.
HEADERS = {
    "User-Agent": os.getenv(
        "WIKIMEDIA_USER_AGENT",
        "StreamingETL/0.1 (educational ETL pipeline)",
    ),
}


def iter_recent_changes(
    url: str = STREAM_URL,
    headers: dict[str, str] = HEADERS,
) -> Iterator[dict[str, Any]]:
    # I keep this HTTP stream open and yield each valid Wikimedia event as a dict.
    with EventSource(url, headers=headers) as event_source:
        for event in event_source:
            if not event.data:
                logging.debug("Received an event without data.")
                continue

            try:
                yield json.loads(event.data)
            except json.JSONDecodeError as exc:
                logging.warning("Failed to decode JSON event: %s", exc)


def extract_data(url: str, headers: dict[str, str]) -> None:
    # I keep this function as a quick way to inspect raw events in the terminal.
    for change in iter_recent_changes(url, headers):
        print(change)


def main() -> None:
    extract_data(STREAM_URL, HEADERS)


if __name__ == "__main__":
    main()
