# Operations Runbook

This runbook keeps the project easy to operate during a demo or local
production-style run.

## Start

```bash
docker compose --profile streaming --profile airflow --profile dashboard up -d --build
```

## Stop

```bash
docker compose --profile streaming --profile airflow --profile dashboard down
```

## Status

```bash
docker compose ps
```

## Logs

```bash
docker compose logs -f wikimedia-producer-stream
docker compose logs -f kafka-to-bronze-stream
docker compose logs -f airflow
docker compose logs -f dashboard
```

## Rerun Silver And Gold Manually

```bash
uv run python src/data_transformation/Spark_transformations.py --write-silver
uv run python src/data_transformation/silver_to_gold.py
```

## Backfill Silver

```bash
uv run python src/data_transformation/Spark_transformations.py \
  --write-silver \
  --backfill-start-date 2026-05-01 \
  --backfill-end-date 2026-05-24
```

## Quality Checks

```bash
uv run python src/data_transformation/lakehouse_quality_checks.py
uv run python src/data_visualization/dashboard_data_check.py
```

## Common Issues

### Docker Compose cannot parse `.env`

Check for malformed lines. Each non-comment line must look like:

```text
KEY=value
```

Comments must start with `#`.

### Azure container does not exist

Create these containers in the storage account:

```text
bronze
silver
gold
quarantine
```

### Dashboard says a Gold table is missing

Run the Gold job:

```bash
uv run python src/data_transformation/silver_to_gold.py
```

### Bronze stream restarts

The Bronze Spark stream uses the checkpoint path configured by:

```text
AZURE_BRONZE_CHECKPOINT_PATH
```

Keep that checkpoint path stable so Spark can resume Kafka offsets correctly.
