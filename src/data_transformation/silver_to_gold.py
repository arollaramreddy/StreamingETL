import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    date_format,
    dayofmonth,
    dayofweek,
    hour,
    lit,
    month,
    quarter,
    sha2,
    year,
)

try:
    from . import azure_storage_utils
    from . import spark_data_extraction_fromKafka
except ImportError:
    import azure_storage_utils
    import spark_data_extraction_fromKafka


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Gold star-schema tables from the Silver Wikimedia data."
    )
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument(
        "--preview-only",
        action="store_true",
        help="Show the Gold tables without writing them to Azure Storage.",
    )
    return parser.parse_args()


def stable_key(*column_names: str):
    # I use deterministic hash keys so the same source values create the same keys.
    key_parts = [
        coalesce(col(column_name).cast("string"), lit("__null__"))
        for column_name in column_names
    ]
    return sha2(concat_ws("||", *key_parts), 256)


def read_silver_dataframe(spark: SparkSession) -> DataFrame:
    # I read the cleaned Silver layer as the source for Gold data models.
    return azure_storage_utils.read_dataframe_from_lake(
        spark=spark,
        container_env_name="AZURE_SILVER_CONTAINER_NAME",
        path_env_name="AZURE_SILVER_OUTPUT_PATH",
        format_env_name="AZURE_SILVER_OUTPUT_FORMAT",
    )


def add_star_schema_keys(silver_df: DataFrame) -> DataFrame:
    # I add all dimension keys once so every dimension and fact uses identical logic.
    return (
        silver_df.withColumn("date_key", date_format(col("event_date"), "yyyyMMdd").cast("int"))
        .withColumn("wiki_key", stable_key("wiki", "domain", "server_name"))
        .withColumn("page_key", stable_key("wiki", "namespace", "page_title", "page_url"))
        .withColumn("user_key", stable_key("event_user", "is_bot"))
        .withColumn("event_type_key", stable_key("event_type"))
    )


def build_dim_date(modeled_df: DataFrame) -> DataFrame:
    # I create one row per event date for date-based reporting.
    return (
        modeled_df.select("date_key", "event_date")
        .where(col("date_key").isNotNull())
        .dropDuplicates(["date_key"])
        .withColumn("year", year(col("event_date")))
        .withColumn("quarter", quarter(col("event_date")))
        .withColumn("month", month(col("event_date")))
        .withColumn("day", dayofmonth(col("event_date")))
        .withColumn("day_of_week", dayofweek(col("event_date")))
    )


def build_dim_wiki(modeled_df: DataFrame) -> DataFrame:
    # I keep wiki/site fields together because analysts will group by wiki often.
    return (
        modeled_df.select(
            "wiki_key",
            "wiki",
            "domain",
            "server_name",
            "stream_name",
            "source_system",
        )
        .where(col("wiki_key").isNotNull())
        .dropDuplicates(["wiki_key"])
    )


def build_dim_page(modeled_df: DataFrame) -> DataFrame:
    # I model pages separately so facts can stay smaller.
    return (
        modeled_df.select(
            "page_key",
            "wiki_key",
            "namespace",
            "page_title",
            "page_url",
        )
        .where(col("page_key").isNotNull())
        .dropDuplicates(["page_key"])
    )


def build_dim_user(modeled_df: DataFrame) -> DataFrame:
    # I include is_bot here because bot/user analysis is a common Wikimedia question.
    return (
        modeled_df.select(
            "user_key",
            coalesce(col("event_user"), lit("unknown")).alias("event_user"),
            "is_bot",
        )
        .where(col("user_key").isNotNull())
        .dropDuplicates(["user_key"])
    )


def build_dim_event_type(modeled_df: DataFrame) -> DataFrame:
    # I keep event type as a tiny dimension instead of repeating text in every fact.
    return (
        modeled_df.select("event_type_key", "event_type")
        .where(col("event_type_key").isNotNull())
        .dropDuplicates(["event_type_key"])
    )


def build_fact_recent_changes(modeled_df: DataFrame) -> DataFrame:
    # I keep one fact row per Wikimedia change event.
    return modeled_df.select(
        stable_key("meta_id").alias("recent_change_key"),
        "meta_id",
        "event_id",
        "date_key",
        "wiki_key",
        "page_key",
        "user_key",
        "event_type_key",
        "event_datetime",
        "event_date",
        hour(col("event_datetime")).alias("event_hour"),
        "namespace",
        "is_bot",
        "is_minor",
        "is_patrolled",
        "comment",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "bronze_ingested_at",
    ).dropDuplicates(["recent_change_key"])


def build_star_schema(silver_df: DataFrame) -> dict[str, DataFrame]:
    modeled_df = add_star_schema_keys(silver_df)

    return {
        "dim_date": build_dim_date(modeled_df),
        "dim_wiki": build_dim_wiki(modeled_df),
        "dim_page": build_dim_page(modeled_df),
        "dim_user": build_dim_user(modeled_df),
        "dim_event_type": build_dim_event_type(modeled_df),
        "fact_recent_changes": build_fact_recent_changes(modeled_df),
    }


def write_gold_table(
    spark: SparkSession,
    table_name: str,
    table_df: DataFrame,
) -> str:
    # I write each Gold model as its own folder under the Gold base path.
    default_partitions_by_table = {
        "dim_date": "",
        "dim_wiki": "",
        "dim_page": "",
        "dim_user": "",
        "dim_event_type": "",
        "fact_recent_changes": "event_date",
    }

    return azure_storage_utils.write_dataframe_to_lake_path(
        spark=spark,
        dataframe=table_df,
        container_env_name="AZURE_GOLD_CONTAINER_NAME",
        path_env_name="AZURE_GOLD_OUTPUT_PATH",
        format_env_name="AZURE_GOLD_OUTPUT_FORMAT",
        mode_env_name="AZURE_GOLD_WRITE_MODE",
        partition_columns_env_name=f"AZURE_GOLD_{table_name.upper()}_PARTITION_COLUMNS",
        default_partition_columns=default_partitions_by_table[table_name],
        child_path=table_name,
    )


def write_star_schema_to_gold(
    spark: SparkSession,
    star_schema_tables: dict[str, DataFrame],
) -> dict[str, str]:
    written_paths = {}

    for table_name, table_df in star_schema_tables.items():
        written_paths[table_name] = write_gold_table(spark, table_name, table_df)

    return written_paths


def preview_tables(star_schema_tables: dict[str, DataFrame], limit: int) -> None:
    for table_name, table_df in star_schema_tables.items():
        print(f"\n{table_name}")
        table_df.printSchema()
        table_df.show(limit, truncate=False)


def main() -> None:
    spark_data_extraction_fromKafka.configure_java_home()
    args = parse_args()

    spark = spark_data_extraction_fromKafka.create_spark_session(
        include_azure_storage=True
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        silver_df = read_silver_dataframe(spark)
        star_schema_tables = build_star_schema(silver_df)

        preview_tables(star_schema_tables, args.limit)

        if not args.preview_only:
            written_paths = write_star_schema_to_gold(spark, star_schema_tables)
            for table_name, output_path in written_paths.items():
                print(f"{table_name} written to: {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
