import argparse
import json
from itertools import islice
from typing import Any, Iterator

from requests_sse import EventSource


STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
HEADERS = {
    # Wikimedia asks streaming clients to identify themselves.
    "User-Agent": "StreamingETL/0.1 (learning project)",
}


def read_raw_recent_changes() -> Iterator[dict[str, Any]]:
    """Read raw JSON events from the Wikimedia stream."""
    with EventSource(STREAM_URL, headers=HEADERS) as stream:
        for event in stream:
            if event.type != "message":
                continue

            try:
                change = json.loads(event.data)
            except ValueError:
                continue

            # Canary events are Wikimedia test events, not real user changes.
            if change.get("meta", {}).get("domain") == "canary":
                continue

            yield change


def flatten_schema(value: Any, prefix: str = "") -> dict[str, set[str]]:
    """Flatten a nested JSON object into field paths and Python type names."""
    if isinstance(value, dict):
        schema: dict[str, set[str]] = {}
        for key, child_value in value.items():
            child_prefix = f"{prefix}.{key}" if prefix else key
            child_schema = flatten_schema(child_value, child_prefix)
            for field, types in child_schema.items():
                schema.setdefault(field, set()).update(types)
        return schema

    if isinstance(value, list):
        list_prefix = f"{prefix}[]"
        if not value:
            return {list_prefix: {"empty_list"}}

        schema: dict[str, set[str]] = {}
        for item in value:
            child_schema = flatten_schema(item, list_prefix)
            for field, types in child_schema.items():
                schema.setdefault(field, set()).update(types)
        return schema

    return {prefix: {type(value).__name__}}


def infer_schema(records: list[dict[str, Any]]) -> dict[str, list[str]]:
    """Infer schema from a small sample of raw stream records."""
    schema: dict[str, set[str]] = {}
    for record in records:
        for field, types in flatten_schema(record).items():
            schema.setdefault(field, set()).update(types)

    return {field: sorted(types) for field, types in sorted(schema.items())}


def normalize_change(change: dict[str, Any]) -> dict[str, Any]:
    """Select the first columns we want for this raw ETL pipeline."""
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
    """Read raw events and yield normalized records."""
    for change in read_raw_recent_changes():
        yield normalize_change(change)


def print_schema(schema: dict[str, list[str]]) -> None:
    if not schema:
        print("No schema inferred.")
        return

    width = min(max(len(field) for field in schema), 70)
    print("field".ljust(width), "| types")
    print("-" * width, "+", "-" * 20)

    for field, types in schema.items():
        display_field = field if len(field) <= width else field[: width - 3] + "..."
        print(display_field.ljust(width), "|", ", ".join(types))


def print_rows(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("No rows extracted.")
        return

    columns = ["event_time", "domain", "wiki", "user", "title", "change_type", "bot"]
    widths = {
        column: min(
            max(len(column), *(len(str(row.get(column, ""))) for row in rows)),
            42,
        )
        for column in columns
    }

    print(" | ".join(column.ljust(widths[column]) for column in columns))
    print("-+-".join("-" * widths[column] for column in columns))

    for row in rows:
        values = []
        for column in columns:
            value = str(row.get(column, ""))
            if len(value) > widths[column]:
                value = value[: widths[column] - 3] + "..."
            values.append(value.ljust(widths[column]))
        print(" | ".join(values))


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview Wikimedia recent changes.")
    parser.add_argument("--schema-rows", type=int, help="Number of raw rows for schema inference.")
    parser.add_argument("--rows", type=int, help="Number of normalized rows to preview.")
    args = parser.parse_args()

    if args.schema_rows:
        raw_records = list(islice(read_raw_recent_changes(), args.schema_rows))
        print_schema(infer_schema(raw_records))
        return

    if args.rows:
        rows = list(islice(extract_recent_changes(), args.rows))
        print_rows(rows)
        return

    for row in extract_recent_changes():
        print(json.dumps(row, ensure_ascii=False))


if __name__ == "__main__":
    main()
