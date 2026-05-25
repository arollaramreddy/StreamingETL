import argparse
import os

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date

try:
    from . import azure_storage_utils
    from . import data_loading_Spark
    from . import spark_data_extraction_fromKafka
except ImportError:
    import azure_storage_utils
    import data_loading_Spark
    import spark_data_extraction_fromKafka


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Land raw Kafka messages into the Azure Bronze container."
    )
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument(
        "--stream",
        action="store_true",
        help="Run continuously with Spark Structured Streaming.",
    )
    parser.add_argument(
        "--preview-only",
        action="store_true",
        help="Show the Bronze dataframe without writing it to Azure Storage.",
    )
    return parser.parse_args()


def create_bronze_dataframe(kafka_df: DataFrame) -> DataFrame:
    # I store raw Kafka messages first so Bronze stays close to the source data.
    return data_loading_Spark.create_raw_dataframe_from_kafka(kafka_df).withColumn(
        "bronze_ingested_date",
        to_date(col("bronze_ingested_at")),
    )


def write_bronze_dataframe_to_azure(spark, bronze_df: DataFrame) -> str:
    # I write raw records to the Bronze container before any cleaning happens.
    return azure_storage_utils.write_dataframe_to_lake(
        spark=spark,
        dataframe=bronze_df,
        container_env_name="AZURE_BRONZE_CONTAINER_NAME",
        path_env_name="AZURE_BRONZE_OUTPUT_PATH",
        format_env_name="AZURE_BRONZE_OUTPUT_FORMAT",
        mode_env_name="AZURE_BRONZE_WRITE_MODE",
        partition_columns_env_name="AZURE_BRONZE_PARTITION_COLUMNS",
        default_partition_columns="bronze_ingested_date,kafka_topic",
    )


def write_bronze_stream_to_azure(spark, bronze_df: DataFrame) -> str:
    # I checkpoint streaming offsets so restarting the container does not reread old Kafka data.
    azure_storage_utils.configure_azure_storage_access(spark)

    output_path = azure_storage_utils.build_abfss_path(
        "AZURE_BRONZE_CONTAINER_NAME",
        "AZURE_BRONZE_OUTPUT_PATH",
    )
    account_name = azure_storage_utils.get_required_runtime_env(
        "AZURE_STORAGE_ACCOUNT_NAME"
    )
    container_name = azure_storage_utils.get_required_runtime_env(
        "AZURE_BRONZE_CONTAINER_NAME"
    )
    checkpoint_relative_path = azure_storage_utils.get_optional_runtime_env(
        "AZURE_BRONZE_CHECKPOINT_PATH",
        "wikimedia/recent_changes/checkpoints/bronze",
    ).strip("/")
    checkpoint_path = (
        f"abfss://{container_name}@{account_name}.dfs.core.windows.net/"
        f"{checkpoint_relative_path}"
    )
    output_format = azure_storage_utils.get_optional_runtime_env(
        "AZURE_BRONZE_OUTPUT_FORMAT",
        "parquet",
    )
    partition_columns = azure_storage_utils.get_partition_columns(
        "AZURE_BRONZE_PARTITION_COLUMNS",
        "bronze_ingested_date,kafka_topic",
    )
    trigger_interval = os.getenv("BRONZE_STREAM_TRIGGER_INTERVAL", "30 seconds")

    writer = (
        bronze_df.writeStream.format(output_format)
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
    )

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    query = writer.start()
    print(f"Bronze stream started. Output: {output_path}")
    print(f"Bronze stream checkpoint: {checkpoint_path}")
    query.awaitTermination()
    return output_path


def main() -> None:
    spark_data_extraction_fromKafka.configure_java_home()
    args = parse_args()

    spark = spark_data_extraction_fromKafka.create_spark_session(
        include_azure_storage=True
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # I read Kafka, shape the raw records, then land them as Bronze files.
        if args.stream:
            kafka_df = data_loading_Spark.read_kafka_stream(spark)
        else:
            kafka_df = data_loading_Spark.read_kafka_messages(spark)

        bronze_df = create_bronze_dataframe(kafka_df)

        bronze_df.printSchema()

        if args.stream:
            if args.preview_only:
                print("Preview is skipped in streaming mode because the query runs 24/7.")
            else:
                write_bronze_stream_to_azure(spark, bronze_df)
            return

        bronze_df.show(args.limit, truncate=False)

        if not args.preview_only:
            output_path = write_bronze_dataframe_to_azure(spark, bronze_df)
            print(f"Bronze dataframe written to: {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
