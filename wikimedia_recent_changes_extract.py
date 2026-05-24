import argparse
import json
from collections.abc import Iterator
from itertools import islice
from typing import Any

from requests_sse import EventSource


STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {
    # Wikimedia asks clients to identify themselves with a User-Agent.
    "User-Agent": "StreamingETL/0.1 (educational ETL pipeline)",
}


def normalize_change(change: dict[str, Any]) -> dict[str, Any]:
    # Keep only the columns we want for the raw extraction view.
    meta = change.get("meta", {})
    return {
        "event_id": meta.get("id", ""),
        "event_time": meta.get("dt", ""),
        "domain": meta.get("domain", ""),
        "wiki": change.get("wiki", ""),
        "user": change.get("user", ""),
        "title": change.get("title", ""),
        "change_type": change.get("type", ""),
        "bot": change.get("bot", False),
    }


def extract_recent_changes() -> Iterator[dict[str, Any]]:
    # EventSource keeps the HTTP connection open and yields server-sent events.
    with EventSource(STREAM_URL, headers=HEADERS) as stream:
        for event in stream:
            if event.type != "message":
                continue

            try:
                change = json.loads(event.data)
            except ValueError:
                # Skip malformed JSON instead of stopping the stream reader.
                continue

            # Canary events are Wikimedia test events, not real user changes.
            if change.get("meta", {}).get("domain") == "canary":
                continue

            yield normalize_change(change)


def collect_recent_changes(row_count: int) -> list[dict[str, Any]]:
    return list(islice(extract_recent_changes(), row_count))


def print_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("No rows extracted.")
        return

    # Print a compact table so the first extracted records are easy to inspect.
    columns = ["event_time", "domain", "wiki", "user", "title", "change_type", "bot"]
    widths = {
        column: min(
            max(len(column), *(len(str(row.get(column, ""))) for row in rows)),
            42,
        )
        for column in columns
    }

    header = " | ".join(column.ljust(widths[column]) for column in columns)
    separator = "-+-".join("-" * widths[column] for column in columns)
    print(header)
    print(separator)

    for row in rows:
        values = []
        for column in columns:
            value = str(row.get(column, ""))
            if len(value) > widths[column]:
                # Long page titles can make the table hard to read in a terminal.
                value = value[: widths[column] - 3] + "..."
            values.append(value.ljust(widths[column]))
        print(" | ".join(values))


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Wikimedia recent-change events.")
    parser.add_argument(
        "--rows",
        type=int,
        help="Number of stream rows to preview before exiting.",
    )
    args = parser.parse_args()

    if args.rows:
        print_rows(collect_recent_changes(args.rows))
        return

    for row in extract_recent_changes():
        print(json.dumps(row, ensure_ascii=False))


if __name__ == "__main__":
    main()
