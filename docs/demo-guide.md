# Demo Guide

Use this guide for a recruiter screen, portfolio walkthrough, or project video.

## 90 Second Version

1. Start with the architecture diagram in the README.
2. Open Kafka UI and show the Wikimedia topic receiving events.
3. Open Airflow and show the `streamingetl_silver_gold` DAG.
4. Open the Streamlit dashboard and show KPIs, trends, breakdowns, and recent events.
5. Open the code briefly and point to:
   - `src/data_extraction/kafka_extraction.py`
   - `src/data_transformation/kafka_to_bronze.py`
   - `src/data_transformation/Spark_transformations.py`
   - `src/data_transformation/silver_to_gold.py`
   - `src/airflow/dags/streamingetl_pipeline_dag.py`
   - `src/data_visualization/gold_dashboard.py`

## What To Say

This is a streaming data engineering project. It ingests live Wikimedia events,
publishes them into Kafka, lands raw messages into a Bronze lake layer with
Spark Structured Streaming, cleans and validates data into Silver, builds Gold
star-schema models and dashboard aggregates, then serves the data through a
Streamlit dashboard.

The important production ideas are separation of concerns, replayability,
checkpointing, quality checks, quarantine records, backfills, orchestration, and
containerized deployment.

## Commands For A Live Demo

Start the full stack:

```bash
docker compose --profile streaming --profile airflow --profile dashboard up -d --build
```

Check containers:

```bash
docker compose ps
```

Follow logs:

```bash
docker compose logs -f wikimedia-producer-stream kafka-to-bronze-stream airflow dashboard
```

Open:

```text
Kafka UI:   http://localhost:9091
Airflow:    http://localhost:8080
Dashboard:  http://localhost:8501
```

## Suggested Screenshots

Add these screenshots to your GitHub README or LinkedIn post:

- Kafka UI topic page showing `wikimedia.recent_changes`.
- Airflow DAG graph view.
- Azure Storage containers for Bronze, Silver, Gold, and Quarantine.
- Streamlit dashboard Overview tab.
- Streamlit Operations tab showing readiness checks.
- GitHub Actions passing build.

## What Recruiters Usually Notice

- The project uses real streaming data.
- The architecture is easy to explain.
- The code is organized by pipeline layer.
- It has operational features beyond a notebook.
- It has Docker, Airflow, CI/CD, and dashboarding.
- The README shows business value and engineering tradeoffs.
