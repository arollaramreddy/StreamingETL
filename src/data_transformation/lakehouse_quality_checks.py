from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

try:
    from . import azure_storage_utils
    from . import spark_data_extraction_fromKafka
except ImportError:
    import azure_storage_utils
    import spark_data_extraction_fromKafka


GOLD_TABLES = (
    "dim_date",
    "dim_wiki",
    "dim_page",
    "dim_user",
    "dim_event_type",
    "fact_recent_changes",
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
        description="Check lakehouse row counts and duplicate keys."
    )
    parser.add_argument(
        "--skip-bronze",
        action="store_true",
        help="Skip Bronze checks if the streaming writer is actively writing.",
    )
    return parser.parse_args()


def read_gold_table(spark: SparkSession, table_name: str) -> DataFrame:
    # I read each Gold table from its own folder under the Gold base path.
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
    return spark.read.format(table_format).load(table_path)


def duplicate_key_count(dataframe: DataFrame, key_columns: list[str]) -> int:
    # I count duplicate business keys, not duplicate physical files.
    return (
        dataframe.groupBy(*key_columns)
        .count()
        .where(col("count") > 1)
        .count()
    )


def null_count(dataframe: DataFrame, column_name: str) -> int:
    # I count required nulls so data quality failures are visible after each run.
    return dataframe.where(col(column_name).isNull()).count()


def print_metric(name: str, value: int | str) -> None:
    print(f"{name}: {value}")


def lake_path_exists(spark: SparkSession, path: str) -> bool:
    # I check the path before reading so a missing quarantine folder is a clean metric.
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    hadoop_conf = spark._jsc.hadoopConfiguration()

    try:
        return hadoop_path.getFileSystem(hadoop_conf).exists(hadoop_path)
    except Exception:
        return False


def check_bronze(spark: SparkSession, failures: list[str]) -> None:
    bronze_df = azure_storage_utils.read_dataframe_from_lake(
        spark=spark,
        container_env_name="AZURE_BRONZE_CONTAINER_NAME",
        path_env_name="AZURE_BRONZE_OUTPUT_PATH",
        format_env_name="AZURE_BRONZE_OUTPUT_FORMAT",
    )

    row_count = bronze_df.count()
    duplicate_offsets = duplicate_key_count(
        bronze_df,
        ["kafka_topic", "kafka_partition", "kafka_offset"],
    )

    print_metric("bronze_rows", row_count)
    print_metric("bronze_duplicate_kafka_offsets", duplicate_offsets)

    if row_count == 0:
        failures.append("Bronze has no rows.")
    if duplicate_offsets:
        failures.append(f"Bronze has {duplicate_offsets} duplicate Kafka offsets.")


def check_silver(spark: SparkSession, failures: list[str]) -> None:
    silver_df = azure_storage_utils.read_dataframe_from_lake(
        spark=spark,
        container_env_name="AZURE_SILVER_CONTAINER_NAME",
        path_env_name="AZURE_SILVER_OUTPUT_PATH",
        format_env_name="AZURE_SILVER_OUTPUT_FORMAT",
    )

    row_count = silver_df.count()
    duplicate_meta_ids = duplicate_key_count(silver_df, ["meta_id"])

    print_metric("silver_rows", row_count)
    print_metric("silver_duplicate_meta_ids", duplicate_meta_ids)

    if row_count == 0:
        failures.append("Silver has no rows.")
    if duplicate_meta_ids:
        failures.append(f"Silver has {duplicate_meta_ids} duplicate meta_id values.")

    for column_name in [
        "meta_id",
        "event_id",
        "event_type",
        "event_date",
        "wiki",
        "page_title",
    ]:
        required_nulls = null_count(silver_df, column_name)
        print_metric(f"silver_null_{column_name}", required_nulls)

        if required_nulls:
            failures.append(f"Silver column {column_name} has {required_nulls} nulls.")

    if "is_late_arriving" in silver_df.columns:
        print_metric(
            "silver_late_arriving_rows",
            silver_df.where(col("is_late_arriving")).count(),
        )


def check_quarantine(spark: SparkSession) -> None:
    azure_storage_utils.configure_azure_storage_access(spark)
    quarantine_path = azure_storage_utils.build_abfss_path(
        "AZURE_QUARANTINE_CONTAINER_NAME",
        "AZURE_QUARANTINE_OUTPUT_PATH",
    )
    quarantine_format = azure_storage_utils.get_optional_runtime_env(
        "AZURE_QUARANTINE_OUTPUT_FORMAT",
        "parquet",
    )

    if not lake_path_exists(spark, quarantine_path):
        print_metric("quarantine_rows", "unavailable/path not found")
        return

    quarantine_df = spark.read.format(quarantine_format).load(quarantine_path)
    print_metric("quarantine_rows", quarantine_df.count())
    if "quality_error_category" in quarantine_df.columns:
        print("quarantine_errors_by_category")
        quarantine_df.groupBy("quality_error_category").count().show(truncate=False)


def check_gold(spark: SparkSession, failures: list[str]) -> None:
    for table_name in GOLD_TABLES:
        table_df = read_gold_table(spark, table_name)
        row_count = table_df.count()
        print_metric(f"gold_{table_name}_rows", row_count)

        if row_count == 0:
            failures.append(f"Gold table {table_name} has no rows.")

        if table_name == "fact_recent_changes":
            duplicate_recent_change_keys = duplicate_key_count(
                table_df,
                ["recent_change_key"],
            )
            print_metric(
                "gold_fact_duplicate_recent_change_keys",
                duplicate_recent_change_keys,
            )
            if duplicate_recent_change_keys:
                failures.append(
                    "Gold fact_recent_changes has "
                    f"{duplicate_recent_change_keys} duplicate recent_change_key values."
                )
            if "is_late_arriving" in table_df.columns:
                print_metric(
                    "gold_fact_late_arriving_rows",
                    table_df.where(col("is_late_arriving")).count(),
                )


def main() -> None:
    args = parse_args()
    spark_data_extraction_fromKafka.configure_java_home()

    spark = spark_data_extraction_fromKafka.create_spark_session(
        include_azure_storage=True
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Lakehouse quality checks")
        failures = []

        if not args.skip_bronze:
            check_bronze(spark, failures)

        check_silver(spark, failures)
        check_quarantine(spark)
        check_gold(spark, failures)

        if failures:
            failure_message = "\n".join(f"- {failure}" for failure in failures)
            raise RuntimeError(f"Lakehouse quality checks failed:\n{failure_message}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
