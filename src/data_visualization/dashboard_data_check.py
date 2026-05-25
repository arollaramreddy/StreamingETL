from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

from pyspark.sql import DataFrame, SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from data_transformation import azure_storage_utils  # noqa: E402
from data_transformation import spark_data_extraction_fromKafka  # noqa: E402


DASHBOARD_TABLES = (
    "agg_dashboard_kpis",
    "agg_events_by_date",
    "agg_events_by_window",
    "agg_events_by_type",
    "agg_events_by_wiki",
    "agg_top_users",
    "agg_top_pages",
    "recent_events_snapshot",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the Gold tables needed by the Streamlit dashboard."
    )
    parser.add_argument(
        "--min-fact-rows",
        type=int,
        default=int(os.getenv("DASHBOARD_MIN_FACT_ROWS", "1")),
        help="Minimum number of fact rows required before I call the dashboard ready.",
    )
    return parser.parse_args()


def read_gold_table(spark: SparkSession, table_name: str) -> DataFrame:
    # I read the same Gold table folders that the Streamlit dashboard reads.
    azure_storage_utils.configure_azure_storage_access(spark)
    table_path = azure_storage_utils.build_abfss_path(
        "AZURE_GOLD_CONTAINER_NAME",
        "AZURE_GOLD_OUTPUT_PATH",
        child_path=table_name,
    )
    table_format = azure_storage_utils.get_optional_runtime_env(
        "AZURE_GOLD_OUTPUT_FORMAT",
        "parquet",
    )

    if table_format == "parquet" and not azure_storage_utils.lake_path_has_data_files(
        spark,
        table_path,
    ):
        raise RuntimeError(
            f"Gold table '{table_name}' is missing or empty at {table_path}. "
            "Run src/data_transformation/silver_to_gold.py before opening the dashboard."
        )

    return spark.read.format(table_format).load(table_path)


def require_columns(
    dataframe: DataFrame,
    table_name: str,
    required_columns: set[str],
) -> None:
    # I fail fast if a dashboard query would break because a column is missing.
    missing_columns = required_columns - set(dataframe.columns)
    if missing_columns:
        raise RuntimeError(
            f"{table_name} is missing columns: {', '.join(sorted(missing_columns))}"
        )


def validate_dashboard_tables(
    spark: SparkSession,
    min_fact_rows: int,
) -> None:
    tables = {
        table_name: read_gold_table(spark, table_name)
        for table_name in DASHBOARD_TABLES
    }

    require_columns(
        tables["agg_dashboard_kpis"],
        "agg_dashboard_kpis",
        {
            "events",
            "bot_events",
            "unique_users",
            "unique_pages",
        },
    )
    require_columns(
        tables["agg_events_by_date"],
        "agg_events_by_date",
        {"event_date", "events"},
    )
    require_columns(
        tables["agg_events_by_window"],
        "agg_events_by_window",
        {
            "window_start",
            "window_end",
            "event_date",
            "event_hour",
            "events",
        },
    )
    require_columns(
        tables["agg_events_by_type"],
        "agg_events_by_type",
        {"event_type", "events"},
    )
    require_columns(
        tables["agg_events_by_wiki"],
        "agg_events_by_wiki",
        {"wiki", "events"},
    )
    require_columns(
        tables["agg_top_users"],
        "agg_top_users",
        {"event_user", "is_bot", "events"},
    )
    require_columns(
        tables["agg_top_pages"],
        "agg_top_pages",
        {"page_title", "namespace", "events"},
    )
    require_columns(
        tables["recent_events_snapshot"],
        "recent_events_snapshot",
        {
            "event_datetime",
            "event_type",
            "wiki",
            "page_title",
            "event_user",
            "is_bot",
            "namespace",
            "comment",
        },
    )

    kpi_row = tables["agg_dashboard_kpis"].collect()[0]

    total_events = int(kpi_row["events"] or 0)
    if total_events < min_fact_rows:
        raise RuntimeError(
            f"Dashboard is not ready. Expected at least {min_fact_rows} fact row(s), "
            f"but found {total_events}."
        )

    for table_name, table_df in tables.items():
        row_count = table_df.count()
        if row_count == 0:
            raise RuntimeError(f"Dashboard table {table_name} has no rows.")
        print(f"dashboard_{table_name}_rows: {row_count}")

    print("Dashboard data is ready.")
    print(f"dashboard_total_events: {total_events}")
    print(f"dashboard_bot_events: {int(kpi_row['bot_events'] or 0)}")
    print(f"dashboard_unique_users: {int(kpi_row['unique_users'] or 0)}")
    print(f"dashboard_unique_pages: {int(kpi_row['unique_pages'] or 0)}")


def main() -> None:
    args = parse_args()
    spark_data_extraction_fromKafka.configure_java_home()

    spark = spark_data_extraction_fromKafka.create_spark_session(
        include_azure_storage=True
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        validate_dashboard_tables(spark, args.min_fact_rows)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
