# Project Brief

## Summary

StreamingETL is an end-to-end data engineering project that ingests live
Wikimedia recent-change events, streams them through Kafka, processes them with
Spark, stores curated lakehouse layers in Azure Storage, orchestrates recurring
jobs with Airflow, and serves analytics through a cached Streamlit dashboard.

## Business Framing

The project simulates a production analytics pipeline for monitoring high-volume
event activity. The same architecture pattern can be applied to clickstream
analytics, product telemetry, audit logs, fraud monitoring, IoT events, and
near-real-time operational dashboards.

## Engineering Highlights

- Built a Kafka producer for live Wikimedia events.
- Used Spark Structured Streaming to land raw Kafka events into Bronze storage.
- Implemented Bronze, Silver, and Gold lakehouse layers.
- Added schema validation, required-field checks, bad-record quarantine, and late-arriving data flags.
- Built Gold dimensional models and dashboard aggregates.
- Orchestrated recurring transformations and validations with Airflow.
- Added Redis caching for dashboard responsiveness.
- Containerized the full local runtime with Docker Compose.
- Added GitHub Actions CI/CD validation.

## Resume Bullets

- Built an end-to-end streaming ETL pipeline using Kafka, PySpark, Airflow, Azure Storage, Docker, and Streamlit to process live Wikimedia event data.
- Designed Bronze, Silver, and Gold lakehouse layers with schema validation, quarantine handling, late-arriving data support, and backfill-ready processing.
- Created Gold star-schema models and dashboard aggregate tables consumed by a Redis-cached Streamlit analytics dashboard.
- Containerized Kafka, Spark jobs, Airflow, Redis, and dashboard services with Docker Compose and added GitHub Actions CI validation.

## Interview Talking Points

- Why Kafka is used between source ingestion and downstream consumers.
- Why Bronze stores raw Kafka payloads before transformation.
- How Spark checkpoints protect the streaming Bronze writer from duplicate processing after restarts.
- How Silver handles malformed records, nulls, schema issues, and late data.
- Why Gold separates facts, dimensions, and dashboard aggregates.
- How Airflow separates continuous ingestion from scheduled transformations.
- How Redis improves dashboard latency without changing the lakehouse source of truth.
