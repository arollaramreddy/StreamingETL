from __future__ import annotations

from datetime import datetime, timedelta
import os

from airflow.decorators import dag, task


PROJECT_DIR = "/opt/airflow/project"
DEFAULT_SPARK_LIMIT = os.getenv("SPARK_PREVIEW_LIMIT", "5")
TRANSFORM_SCHEDULE = os.getenv("AIRFLOW_TRANSFORM_SCHEDULE", "*/5 * * * *")


def project_command(command: str) -> str:
    # I always run tasks from the project root so relative paths and .env loading work.
    return f"cd {PROJECT_DIR} && {command}"


def env_or_default(name: str, default_value: str) -> str:
    # I ignore placeholder values so Airflow can still use safe local defaults.
    value = os.getenv(name)
    if not value or value.strip().startswith("<"):
        return default_value
    return value


common_task_env = {
    # I override the host Kafka address because Airflow runs inside Docker.
    "KAFKA_BOOTSTRAP_SERVERS": os.getenv(
        "KAFKA_DOCKER_BOOTSTRAP_SERVERS",
        "kafka:29092",
    ),
    # I use overwrite for scheduled reprocessing so reruns replace old Silver/Gold data.
    "AZURE_SILVER_WRITE_MODE": os.getenv("AIRFLOW_SILVER_WRITE_MODE", "overwrite"),
    "AZURE_GOLD_WRITE_MODE": os.getenv("AIRFLOW_GOLD_WRITE_MODE", "overwrite"),
    "AIRFLOW_DASHBOARD_HEALTHCHECK_ENABLED": os.getenv(
        "AIRFLOW_DASHBOARD_HEALTHCHECK_ENABLED",
        "true",
    ),
    "DASHBOARD_HEALTH_URL": os.getenv(
        "DASHBOARD_HEALTH_URL",
        "http://dashboard:8501/_stcore/health",
    ),
    "DASHBOARD_MIN_FACT_ROWS": os.getenv("DASHBOARD_MIN_FACT_ROWS", "1"),
    "DATA_QUALITY_STRICT_MODE": env_or_default("DATA_QUALITY_STRICT_MODE", "false"),
    "LATE_ARRIVAL_DAYS": env_or_default("LATE_ARRIVAL_DAYS", "2"),
    "AIRFLOW_SILVER_LOOKBACK_DAYS": env_or_default("AIRFLOW_SILVER_LOOKBACK_DAYS", ""),
    "PYTHONPATH": f"{PROJECT_DIR}/src",
    "SPARK_LOCAL_IP": "127.0.0.1",
    "SPARK_IVY_DIR": "/tmp/spark-ivy-cache",
    "SPARK_PARTITION_OVERWRITE_MODE": env_or_default(
        "SPARK_PARTITION_OVERWRITE_MODE",
        "dynamic",
    ),
    "SPARK_SQL_SHUFFLE_PARTITIONS": env_or_default(
        "SPARK_SQL_SHUFFLE_PARTITIONS",
        "8",
    ),
    "SPARK_ADAPTIVE_EXECUTION_ENABLED": env_or_default(
        "SPARK_ADAPTIVE_EXECUTION_ENABLED",
        "true",
    ),
    "SPARK_ADAPTIVE_COALESCE_PARTITIONS_ENABLED": env_or_default(
        "SPARK_ADAPTIVE_COALESCE_PARTITIONS_ENABLED",
        "true",
    ),
    "AZURE_QUARANTINE_CONTAINER_NAME": env_or_default(
        "AZURE_QUARANTINE_CONTAINER_NAME",
        "quarantine",
    ),
    "AZURE_QUARANTINE_OUTPUT_PATH": env_or_default(
        "AZURE_QUARANTINE_OUTPUT_PATH",
        "wikimedia/recent_changes/bad_records",
    ),
    "AZURE_QUARANTINE_OUTPUT_FORMAT": env_or_default(
        "AZURE_QUARANTINE_OUTPUT_FORMAT",
        "parquet",
    ),
    "AZURE_QUARANTINE_WRITE_MODE": env_or_default(
        "AZURE_QUARANTINE_WRITE_MODE",
        "overwrite",
    ),
    "AZURE_QUARANTINE_PARTITION_COLUMNS": env_or_default(
        "AZURE_QUARANTINE_PARTITION_COLUMNS",
        "processing_window_date,quality_error_category",
    ),
}


default_args = {
    "owner": "streamingetl",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="streamingetl_silver_gold",
    description="Periodic Bronze to Silver and Silver to Gold orchestration.",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=TRANSFORM_SCHEDULE,
    catchup=False,
    max_active_runs=1,
    params={
        "backfill_start_date": "",
        "backfill_end_date": "",
        "lookback_days": env_or_default("AIRFLOW_SILVER_LOOKBACK_DAYS", ""),
        "late_arrival_days": env_or_default("LATE_ARRIVAL_DAYS", "2"),
        "strict_quality": env_or_default("DATA_QUALITY_STRICT_MODE", "false"),
    },
    tags=["streamingetl", "wikimedia", "lakehouse"],
)
def streamingetl_silver_gold():
    @task.bash(
        task_id="transform_bronze_to_silver",
        env=common_task_env,
        append_env=True,
    )
    def transform_bronze_to_silver() -> str:
        # I run this repeatedly because Bronze is being filled by the streaming job.
        return project_command(
            'normalize_optional_param() { '
            'case "$1" in ""|None|none|null|NULL) echo "";; *) echo "$1";; esac; '
            '}; '
            'BACKFILL_START_DATE="$(normalize_optional_param "{{ params.backfill_start_date }}")"; '
            'BACKFILL_END_DATE="$(normalize_optional_param "{{ params.backfill_end_date }}")"; '
            'LOOKBACK_DAYS="$(normalize_optional_param "{{ params.lookback_days }}")"; '
            'LATE_ARRIVAL_DAYS="$(normalize_optional_param "{{ params.late_arrival_days }}")"; '
            'STRICT_QUALITY="$(normalize_optional_param "{{ params.strict_quality }}")"; '
            'LATE_ARRIVAL_DAYS="${LATE_ARRIVAL_DAYS:-2}"; '
            'STRICT_QUALITY="${STRICT_QUALITY:-false}"; '
            'case "$STRICT_QUALITY" in true|True|1|yes|Yes) '
            'STRICT_ARG="--strict-quality";; *) STRICT_ARG="";; esac; '
            "python src/data_transformation/Spark_transformations.py "
            f"--limit {DEFAULT_SPARK_LIMIT} "
            "--write-silver "
            "--no-preview "
            '--late-arrival-days "$LATE_ARRIVAL_DAYS" '
            '${BACKFILL_START_DATE:+--backfill-start-date "$BACKFILL_START_DATE"} '
            '${BACKFILL_END_DATE:+--backfill-end-date "$BACKFILL_END_DATE"} '
            '${LOOKBACK_DAYS:+--lookback-days "$LOOKBACK_DAYS"} '
            "$STRICT_ARG"
        )

    @task.bash(
        task_id="model_silver_to_gold",
        env=common_task_env,
        append_env=True,
    )
    def model_silver_to_gold() -> str:
        # I rebuild Gold after Silver refreshes so the dashboard sees current models.
        return project_command(
            "python src/data_transformation/silver_to_gold.py "
            f"--limit {DEFAULT_SPARK_LIMIT} "
            "--no-preview"
        )

    @task.bash(
        task_id="validate_lakehouse_quality",
        env=common_task_env,
        append_env=True,
    )
    def validate_lakehouse_quality() -> str:
        # I run post-write checks so duplicate keys/nulls fail close to the pipeline.
        return project_command(
            "python src/data_transformation/lakehouse_quality_checks.py --skip-bronze"
        )

    @task.bash(
        task_id="validate_dashboard",
        env=common_task_env,
        append_env=True,
    )
    def validate_dashboard() -> str:
        # I make the dashboard part of orchestration by checking its data and health.
        return project_command(
            "python src/data_visualization/dashboard_data_check.py "
            '&& if [ "$AIRFLOW_DASHBOARD_HEALTHCHECK_ENABLED" = "true" ]; then '
            'curl --fail --silent --show-error "$DASHBOARD_HEALTH_URL"; '
            "else echo 'Dashboard service healthcheck disabled.'; fi"
        )

    (
        transform_bronze_to_silver()
        >> model_silver_to_gold()
        >> validate_lakehouse_quality()
        >> validate_dashboard()
    )


streamingetl_silver_gold()
