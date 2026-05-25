# StreamingETL

Step-by-step streaming ETL learning project.

Current focus:

1. Connect to the Wikimedia recent-change streaming source.
2. Produce raw Wikimedia events to Kafka.
3. Use Docker Compose for a local Kafka broker.

## Files

```text
main.py
docker-compose.yaml
.env.example
src/data_extraction/wikimedia_recent_changes_extract.py
src/data_extraction/kafka_extraction.py
pyproject.toml
uv.lock
README.md
```

## Environment

Local settings and secrets live in `.env`. The committed `.env.example` file shows the expected keys.

```bash
cp .env.example .env
```

Docker Compose reads `.env` automatically. The Python Kafka producer also loads it through `python-dotenv`.

## Local Kafka

Start a local Kafka broker:

```bash
docker compose up -d
```

Kafka UI is available at:

```text
http://localhost:9091
```

Produce Wikimedia recent-change events to Kafka:

```bash
uv run python src/data_extraction/kafka_extraction.py --limit 10
```

The default Kafka topic is `wikimedia.recent_changes`, and the default broker is `localhost:9092`.

Inspect produced Kafka messages:

```bash
docker compose exec kafka kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic wikimedia.recent_changes --from-beginning --max-messages 10
```

## Inspect Source Events

The source emits data continuously. Print raw Wikimedia events:

```bash
uv run python src/data_extraction/wikimedia_recent_changes_extract.py
```

Stop the stream with `Ctrl + C`.
