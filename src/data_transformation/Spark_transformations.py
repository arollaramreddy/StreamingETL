import argparse

from pyspark.sql import DataFrame, SparkSession

# I import Spark SQL functions instead of normal Python functions because these run
# across the Spark dataframe columns, not on one local Python value at a time.
from pyspark.sql.functions import (
    col,
    coalesce,
    from_unixtime,
    length,
    lit,
    lower,
    regexp_replace,
    to_date,
    to_timestamp,
    trim,
    when,
)

# I use this try/except so the file works in both cases:
# 1. running as a package/module
# 2. running directly from this folder
try:
    from . import azure_storage_utils
    from . import data_loading_Spark
    from . import spark_data_extraction_fromKafka
except ImportError:
    import azure_storage_utils
    import data_loading_Spark
    import spark_data_extraction_fromKafka


def parse_args() -> argparse.Namespace:
    # I keep the CLI simple for testing, so I can quickly preview a few clean rows.
    parser = argparse.ArgumentParser(
        description="Clean Wikimedia Kafka data with Spark."
    )
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument(
        "--write-silver",
        action="store_true",
        help="Write the cleaned dataframe to the Azure Silver container.",
    )
    parser.add_argument(
        "--write-azure",
        action="store_true",
        help="Same as --write-silver. Kept so older commands still work.",
    )
    return parser.parse_args()


def clean_text(text_column):
    # I replace nulls with empty strings first so regexp_replace does not return null.
    text_without_html = regexp_replace(coalesce(text_column, lit("")), r"<[^>]*>", " ")

    # I remove the common HTML entities that appear in Wikimedia parsed comments.
    text_without_entities = regexp_replace(text_without_html, r"&nbsp;|&#160;", " ")
    text_without_entities = regexp_replace(text_without_entities, r"&quot;", '"')
    text_without_entities = regexp_replace(text_without_entities, r"&#39;|&apos;", "'")
    text_without_entities = regexp_replace(text_without_entities, r"&amp;", "&")

    # I collapse repeated spaces/newlines/tabs into one clean space.
    return trim(regexp_replace(text_without_entities, r"\s+", " "))


def blank_to_null(text_column):
    # I keep empty strings as nulls because null is easier to filter/count in Spark.
    cleaned_column = clean_text(text_column)
    return when(length(cleaned_column) == 0, lit(None)).otherwise(cleaned_column)


def transform_dataframe(loaded_df: DataFrame) -> DataFrame:
    # I remove rows that did not parse into the expected Wikimedia JSON structure.
    # This protects the cleaning logic from test messages or malformed Kafka records.
    valid_events_df = loaded_df.filter(
        col("event_id").isNotNull()
        & col("meta_id").isNotNull()
        & col("event_type").isNotNull()
        & col("wiki").isNotNull()
        & col("page_title").isNotNull()
    )

    cleaned_df = (
        # I deduplicate by meta_id because Wikimedia sends it as the stream event id.
        valid_events_df.dropDuplicates(["meta_id"])

        # I prefer meta.dt, but I fall back to the Unix timestamp if meta.dt is missing.
        .withColumn(
            "event_datetime",
            coalesce(
                to_timestamp(col("event_datetime")),
                to_timestamp(from_unixtime(col("event_unix_timestamp"))),
            ),
        )
        # I remove records where both timestamp options failed.
        .filter(col("event_datetime").isNotNull())

        # I create a date column so daily grouping is easier later.
        .withColumn("event_date", to_date(col("event_datetime")))

        # I standardize these fields to lowercase so joins/grouping do not split values.
        .withColumn("event_type", lower(trim(col("event_type"))))
        .withColumn("wiki", lower(trim(col("wiki"))))
        .withColumn("domain", lower(trim(col("domain"))))
        .withColumn("stream_name", lower(trim(col("stream_name"))))
        .withColumn("server_name", lower(trim(col("server_name"))))

        # I clean text fields and convert blanks to nulls.
        .withColumn("page_title", blank_to_null(col("page_title")))
        .withColumn("page_url", blank_to_null(col("page_url")))
        .withColumn("event_user", blank_to_null(col("event_user")))

        # I use parsed_comment first because it usually has cleaner text than raw comment.
        .withColumn(
            "comment",
            blank_to_null(coalesce(col("parsed_comment"), col("comment"))),
        )

        # I fill missing boolean values with False so they behave like real flags.
        .withColumn("is_bot", coalesce(col("is_bot"), lit(False)))
        .withColumn("is_minor", coalesce(col("is_minor"), lit(False)))
        .withColumn("is_patrolled", coalesce(col("is_patrolled"), lit(False)))
    )

    # I return only the columns I want to use downstream as my cleaned table.
    return cleaned_df.select(
        "event_id",
        "meta_id",
        "event_type",
        "event_datetime",
        "event_date",
        "wiki",
        "domain",
        "stream_name",
        "namespace",
        "page_title",
        "page_url",
        "event_user",
        "is_bot",
        "is_minor",
        "is_patrolled",
        "server_name",
        "comment",
        "bronze_ingested_at",
        "source_system",
        "message_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
    )


def load_transformed_dataframe() -> DataFrame:
    # I use this helper in notebooks when I want one call to load and clean the data.
    loaded_df = data_loading_Spark.load_dataframe()
    return transform_dataframe(loaded_df)


def write_dataframe_to_silver_storage(
    spark: SparkSession,
    cleaned_df: DataFrame,
) -> str:
    # I write cleaned records to Silver after Bronze has preserved the raw data.
    return azure_storage_utils.write_dataframe_to_lake(
        spark=spark,
        dataframe=cleaned_df,
        container_env_name="AZURE_SILVER_CONTAINER_NAME",
        path_env_name="AZURE_SILVER_OUTPUT_PATH",
        format_env_name="AZURE_SILVER_OUTPUT_FORMAT",
        mode_env_name="AZURE_SILVER_WRITE_MODE",
        partition_columns_env_name="AZURE_SILVER_PARTITION_COLUMNS",
        default_partition_columns="event_date",
    )


def main() -> None:
    # I configure Java before creating Spark because Spark needs a supported JVM.
    spark_data_extraction_fromKafka.configure_java_home()
    args = parse_args()

    spark = spark_data_extraction_fromKafka.create_spark_session(
        include_azure_storage=True
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # I load Bronze messages, clean them, then print the schema and sample rows.
        loaded_df = data_loading_Spark.load_dataframe(spark)
        transformed_df = transform_dataframe(loaded_df)

        transformed_df.printSchema()
        transformed_df.show(args.limit, truncate=False)

        if args.write_silver or args.write_azure:
            output_path = write_dataframe_to_silver_storage(spark, transformed_df)
            print(f"Silver dataframe written to: {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
