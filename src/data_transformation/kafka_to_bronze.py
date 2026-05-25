import argparse

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


def main() -> None:
    spark_data_extraction_fromKafka.configure_java_home()
    args = parse_args()

    spark = spark_data_extraction_fromKafka.create_spark_session(
        include_azure_storage=True
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # I read Kafka, shape the raw records, then land them as Bronze files.
        kafka_df = data_loading_Spark.read_kafka_messages(spark)
        bronze_df = create_bronze_dataframe(kafka_df)

        bronze_df.printSchema()
        bronze_df.show(args.limit, truncate=False)

        if not args.preview_only:
            output_path = write_bronze_dataframe_to_azure(spark, bronze_df)
            print(f"Bronze dataframe written to: {output_path}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
