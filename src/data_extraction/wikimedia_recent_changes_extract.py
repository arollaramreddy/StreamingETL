import json
from typing import Any

from requests_sse import EventSource


STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

HEADERS = {
    "User-Agent": "StreamingETL/0.1 (educational ETL pipeline)",
}


def extract_data(url: str, headers: dict[str, str]) -> None:
    with EventSource(url, headers=headers) as event_source:
        for event in event_source:
            if event.data:
                try:
                    change = json.loads(event.data)
                    print(change)
                except json.JSONDecodeError as e:
                    print(f"Failed to decode JSON: {e}")
            else:
                print("Received an event without data.")

def main() -> None:
    extract_data(STREAM_URL, HEADERS)
    

if __name__ == "__main__":
    main()
