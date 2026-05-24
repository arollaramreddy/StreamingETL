# StreamingETL

Step-by-step streaming ETL learning project.

Current focus:

1. Connect to the Wikimedia recent-change streaming source.
2. Inspect the raw event schema.
3. Normalize a few useful fields.
4. Preview extracted rows.

## Files

```text
main.py
wikimedia_recent_changes_extract.py
pyproject.toml
uv.lock
README.md
```

## Inspect Source Schema

The source emits data continuously. First, sample a few raw events and inspect the fields.

```bash
uv run python wikimedia_recent_changes_extract.py --schema-rows 3
```

## Preview Extracted Rows

Preview normalized rows:

```bash
uv run python wikimedia_recent_changes_extract.py --rows 5
```

Run continuously:

```bash
uv run python wikimedia_recent_changes_extract.py
```

Stop the stream with `Ctrl + C`.
