import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    count,
    countDistinct,
    date_format,
    dayofmonth,
    dayofweek,
    desc,
    hour,
    lit,
    month,
    quarter,
    sha2,
    sum as spark_sum,
    when,
    window,
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
    parser.add_argument(
        "--no-preview",
        action="store_true",
        help="Skip printing sample rows. Useful inside Airflow.",
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
    fact_columns = [
        stable_key("meta_id").alias("recent_change_key"),
        col("meta_id"),
        col("event_id"),
        col("date_key"),
        col("wiki_key"),
        col("page_key"),
        col("user_key"),
        col("event_type_key"),
        col("event_datetime"),
        col("event_date"),
        hour(col("event_datetime")).alias("event_hour"),
        col("namespace"),
        col("is_bot"),
        col("is_minor"),
        col("is_patrolled"),
        col("comment"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col("bronze_ingested_at"),
    ]

    for optional_column in [
        "arrival_lag_days",
        "is_late_arriving",
        "processing_window_date",
        "quality_checked_at",
        "quality_rule_version",
    ]:
        if optional_column in modeled_df.columns:
            fact_columns.append(col(optional_column))

    return modeled_df.select(*fact_columns).dropDuplicates(["recent_change_key"])


def build_dashboard_base(
    fact_df: DataFrame,
    dim_wiki_df: DataFrame,
    dim_page_df: DataFrame,
    dim_user_df: DataFrame,
    dim_event_type_df: DataFrame,
) -> DataFrame:
    # I create one small joined view so dashboard aggregates all use the same labels.
    wiki_lookup_df = dim_wiki_df.select("wiki_key", col("wiki").alias("wiki_name"))
    page_lookup_df = dim_page_df.select(
        "page_key",
        "page_title",
        col("namespace").alias("page_namespace"),
    )
    user_lookup_df = dim_user_df.select(
        "user_key",
        col("event_user").alias("user_name"),
        col("is_bot").alias("user_is_bot"),
    )

    return (
        fact_df.join(wiki_lookup_df, "wiki_key", "left")
        .join(page_lookup_df, "page_key", "left")
        .join(user_lookup_df, "user_key", "left")
        .join(dim_event_type_df, "event_type_key", "left")
        .select(
            "event_datetime",
            "event_date",
            "event_hour",
            coalesce(col("event_type"), lit("unknown")).alias("event_type"),
            coalesce(col("wiki_name"), lit("unknown")).alias("wiki"),
            coalesce(col("page_title"), lit("unknown")).alias("page_title"),
            coalesce(col("page_namespace"), lit(-1)).alias("namespace"),
            coalesce(col("user_name"), lit("unknown")).alias("event_user"),
            coalesce(col("user_is_bot"), col("is_bot"), lit(False)).alias("is_bot"),
            "comment",
            "user_key",
            "page_key",
        )
    )


def build_dashboard_aggregates(
    fact_df: DataFrame,
    dim_wiki_df: DataFrame,
    dim_page_df: DataFrame,
    dim_user_df: DataFrame,
    dim_event_type_df: DataFrame,
) -> dict[str, DataFrame]:
    # I pre-aggregate dashboard tables in Gold so Streamlit does less work.
    dashboard_base_df = build_dashboard_base(
        fact_df,
        dim_wiki_df,
        dim_page_df,
        dim_user_df,
        dim_event_type_df,
    )

    return {
        "agg_dashboard_kpis": fact_df.agg(
            count("*").alias("events"),
            spark_sum(when(col("is_bot"), lit(1)).otherwise(lit(0))).alias("bot_events"),
            countDistinct("user_key").alias("unique_users"),
            countDistinct("page_key").alias("unique_pages"),
        ),
        "agg_events_by_date": fact_df.groupBy("event_date")
        .agg(count("*").alias("events"))
        .orderBy("event_date"),
        "agg_events_by_window": fact_df.groupBy(
            window(col("event_datetime"), "15 minutes").alias("event_window"),
            "event_date",
            "event_hour",
        )
        .agg(count("*").alias("events"))
        .select(
            col("event_window.start").alias("window_start"),
            col("event_window.end").alias("window_end"),
            "event_date",
            "event_hour",
            "events",
        )
        .orderBy("window_start"),
        "agg_events_by_type": dashboard_base_df.groupBy("event_type")
        .agg(count("*").alias("events"))
        .orderBy(desc("events")),
        "agg_events_by_wiki": dashboard_base_df.groupBy("wiki")
        .agg(count("*").alias("events"))
        .orderBy(desc("events")),
        "agg_top_users": dashboard_base_df.groupBy("event_user", "is_bot")
        .agg(count("*").alias("events"))
        .orderBy(desc("events")),
        "agg_top_pages": dashboard_base_df.groupBy("page_title", "namespace")
        .agg(count("*").alias("events"))
        .orderBy(desc("events")),
        "recent_events_snapshot": dashboard_base_df.select(
            "event_datetime",
            "event_date",
            "event_type",
            "wiki",
            "page_title",
            "event_user",
            "is_bot",
            "namespace",
            "comment",
        )
        .orderBy(desc("event_datetime"))
        .limit(100),
    }


def build_star_schema(silver_df: DataFrame) -> dict[str, DataFrame]:
    modeled_df = add_star_schema_keys(silver_df)
    dim_date_df = build_dim_date(modeled_df)
    dim_wiki_df = build_dim_wiki(modeled_df)
    dim_page_df = build_dim_page(modeled_df)
    dim_user_df = build_dim_user(modeled_df)
    dim_event_type_df = build_dim_event_type(modeled_df)
    fact_recent_changes_df = build_fact_recent_changes(modeled_df)

    star_schema_tables = {
        "dim_date": dim_date_df,
        "dim_wiki": dim_wiki_df,
        "dim_page": dim_page_df,
        "dim_user": dim_user_df,
        "dim_event_type": dim_event_type_df,
        "fact_recent_changes": fact_recent_changes_df,
    }

    star_schema_tables.update(
        build_dashboard_aggregates(
            fact_recent_changes_df,
            dim_wiki_df,
            dim_page_df,
            dim_user_df,
            dim_event_type_df,
        )
    )

    return star_schema_tables


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
        "agg_dashboard_kpis": "",
        "agg_events_by_date": "event_date",
        "agg_events_by_window": "event_date",
        "agg_events_by_type": "",
        "agg_events_by_wiki": "",
        "agg_top_users": "",
        "agg_top_pages": "",
        "recent_events_snapshot": "event_date",
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

        if not args.no_preview:
            preview_tables(star_schema_tables, args.limit)

        if not args.preview_only:
            written_paths = write_star_schema_to_gold(spark, star_schema_tables)
            for table_name, output_path in written_paths.items():
                print(f"{table_name} written to: {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
