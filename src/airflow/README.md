# Airflow Orchestration

This folder contains the local Airflow orchestration for the StreamingETL pipeline.

The streaming part runs as Docker Compose services:

```text
Wikimedia producer -> Kafka topic -> Bronze container
```

The Airflow DAG runs on a schedule and handles the batch-style layers:

```text
Bronze -> Silver -> Gold -> Lakehouse quality checks -> Dashboard validation
```

Start the continuous streaming ingestion:

```bash
docker compose --profile streaming up -d --build
```

Start Airflow UI:

```bash
docker compose --profile airflow up -d --build airflow
```

Start the dashboard too when dashboard validation is enabled:

```bash
docker compose --profile dashboard up -d --build redis dashboard
```

Or start the full local project:

```bash
docker compose --profile streaming --profile airflow --profile dashboard up -d --build
```

Open:

```text
http://localhost:8080
```

Default local login values come from `.env.example`:

```text
username: admin
password: admin
```

The `streamingetl_silver_gold` DAG runs every 5 minutes by default. Change
`AIRFLOW_TRANSFORM_SCHEDULE` in `.env` if you want a different interval.
The final DAG task validates the Gold tables used by Streamlit and checks
`DASHBOARD_HEALTH_URL` when `AIRFLOW_DASHBOARD_HEALTHCHECK_ENABLED=true`.
The dashboard reads pre-aggregated Gold tables and uses Redis for shared cache.

For backfills, trigger `streamingetl_silver_gold` manually and set params:

```text
backfill_start_date = 2026-05-25
backfill_end_date = 2026-05-25
```

For normal scheduled runs, `AIRFLOW_SILVER_LOOKBACK_DAYS` controls how many recent
event-date partitions are reprocessed so late-arriving records can be picked up.
Bad records are written to the quarantine path configured in `.env`.
