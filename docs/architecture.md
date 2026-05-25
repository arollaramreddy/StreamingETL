# Architecture Notes

## Pipeline Layers

### Source

The source is Wikimedia Recent Changes, a public Server-Sent Events stream. It
provides a realistic continuous event source without needing synthetic data.

### Kafka

Kafka decouples source ingestion from downstream processing. The producer can
run continuously while Spark consumers process at their own speed.

### Bronze

Bronze stores raw Kafka records with metadata such as topic, partition, offset,
message key, raw payload, and ingestion timestamp. This keeps the source of
truth replayable.

### Silver

Silver parses the raw JSON, applies schema validation, normalizes fields,
handles nulls, deduplicates events, identifies late-arriving records, and writes
bad records to quarantine.

### Gold

Gold contains dimensional models and dashboard aggregates. The fact table keeps
one row per recent-change event, while dimensions model dates, wikis, pages,
users, and event types.

### Dashboard

The dashboard reads aggregate Gold tables instead of scanning raw or Silver data.
Redis is used as an optional shared cache to keep the dashboard responsive.

## Reliability Features

- Kafka topic retention protects messages during downstream outages.
- Spark Structured Streaming checkpointing tracks Kafka offsets for Bronze.
- Bronze preserves raw payloads for replay.
- Silver quarantine preserves bad records for debugging.
- Airflow retries failed transformation tasks.
- Docker restart policies restart long-running services after crashes.

## Migration Path

Local portfolio setup:

```text
Kafka -> Spark on Docker -> Azure Storage -> Airflow on Docker -> Streamlit
```

Cloud production setup:

```text
Azure Event Hubs -> Databricks Structured Streaming -> ADLS Gen2 -> Databricks Workflows or Managed Airflow -> Power BI / Databricks SQL
```
