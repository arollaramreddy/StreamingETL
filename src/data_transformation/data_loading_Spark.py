import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType

try:
    from . import azure_storage_utils
    from . import spark_data_extraction_fromKafka
except ImportError:
    import azure_storage_utils
    import spark_data_extraction_fromKafka


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a Spark DataFrame from Kafka messages."
    )
    parser.add_argument("--limit", type=int, default=5)
    return parser.parse_args()


WIKIMEDIA_SCHEMA = StructType(
    [
        StructField("id", LongType()),
        StructField("type", StringType()),
        StructField("namespace", LongType()),
        StructField("title", StringType()),
        StructField("title_url", StringType()),
        StructField("comment", StringType()),
        StructField("parsedcomment", StringType()),
        StructField("timestamp", LongType()),
        StructField("user", StringType()),
        StructField("bot", BooleanType()),
        StructField("minor", BooleanType()),
        StructField("patrolled", BooleanType()),
        StructField("server_name", StringType()),
        StructField("wiki", StringType()),
        StructField(
            "meta",
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("domain", StringType()),
                    StructField("stream", StringType()),
                    StructField("dt", StringType()),
                ]
            ),
        ),
    ]
)


def read_kafka_messages(spark: SparkSession) -> DataFrame:
    return (
        spark.read.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            spark_data_extraction_fromKafka.get_required_env("KAFKA_BOOTSTRAP_SERVERS"),
        )
        .option(
            "subscribe",
            spark_data_extraction_fromKafka.get_required_env("KAFKA_TOPIC"),
        )
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )


def read_kafka_stream(spark: SparkSession) -> DataFrame:
    # I use Spark Structured Streaming here so Kafka offsets are checkpointed.
    return (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            spark_data_extraction_fromKafka.get_required_env("KAFKA_BOOTSTRAP_SERVERS"),
        )
        .option(
            "subscribe",
            spark_data_extraction_fromKafka.get_required_env("KAFKA_TOPIC"),
        )
        .option(
            "startingOffsets",
            spark_data_extraction_fromKafka.get_optional_env(
                "KAFKA_STREAM_STARTING_OFFSETS"
            )
            or "earliest",
        )
        .option("failOnDataLoss", "false")
        .load()
    )


def create_raw_dataframe_from_kafka(kafka_df: DataFrame) -> DataFrame:
    # I keep Bronze close to the Kafka record so I can replay/parse it again later.
    return kafka_df.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("raw_message"),
        current_timestamp().alias("bronze_ingested_at"),
        lit("wikimedia_recent_changes").alias("source_system"),
    )


def read_bronze_messages(spark: SparkSession) -> DataFrame:
    # I read raw Kafka messages from the Bronze container, not directly from Kafka.
    azure_storage_utils.configure_azure_storage_access(spark)
    bronze_path = azure_storage_utils.build_abfss_path(
        "AZURE_BRONZE_CONTAINER_NAME",
        "AZURE_BRONZE_OUTPUT_PATH",
    )
    bronze_format = azure_storage_utils.get_optional_runtime_env(
        "AZURE_BRONZE_OUTPUT_FORMAT",
        "parquet",
    )

    return spark.read.format(bronze_format).load(bronze_path)


def create_dataframe_from_raw_messages(
    spark: SparkSession,
    raw_messages_df: DataFrame,
) -> DataFrame:
    # I reuse the raw schema first, then parse the JSON payload into real columns.
    dataframe = spark.createDataFrame(raw_messages_df.rdd, schema=raw_messages_df.schema)

    parsed_dataframe = dataframe.select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "message_key",
        "raw_message",
        "bronze_ingested_at",
        "source_system",
    ).withColumn("event", from_json(col("raw_message"), WIKIMEDIA_SCHEMA))

    return parsed_dataframe.select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "message_key",
        "raw_message",
        "bronze_ingested_at",
        "source_system",
        col("event.meta.id").alias("meta_id"),
        col("event.meta.domain").alias("domain"),
        col("event.meta.stream").alias("stream_name"),
        col("event.meta.dt").alias("event_datetime"),
        col("event.id").alias("event_id"),
        col("event.timestamp").alias("event_unix_timestamp"),
        col("event.type").alias("event_type"),
        col("event.namespace").alias("namespace"),
        col("event.title").alias("page_title"),
        col("event.title_url").alias("page_url"),
        col("event.user").alias("event_user"),
        col("event.bot").alias("is_bot"),
        col("event.server_name").alias("server_name"),
        col("event.wiki").alias("wiki"),
        col("event.comment").alias("comment"),
        col("event.parsedcomment").alias("parsed_comment"),
        col("event.minor").alias("is_minor"),
        col("event.patrolled").alias("is_patrolled"),
    )


def create_dataframe_from_kafka_schema(
    spark: SparkSession,
    kafka_df: DataFrame,
) -> DataFrame:
    # I keep this helper for local debugging, but the main flow now reads Bronze.
    raw_messages_df = create_raw_dataframe_from_kafka(kafka_df)
    return create_dataframe_from_raw_messages(spark, raw_messages_df)


def load_dataframe(spark: SparkSession | None = None) -> DataFrame:
    if spark is None:
        spark_data_extraction_fromKafka.configure_java_home()
        spark = spark_data_extraction_fromKafka.create_spark_session(
            include_azure_storage=True
        )
        spark.sparkContext.setLogLevel("WARN")

    bronze_df = read_bronze_messages(spark)
    return create_dataframe_from_raw_messages(spark, bronze_df)


def main() -> None:
    spark_data_extraction_fromKafka.configure_java_home()
    args = parse_args()

    spark = spark_data_extraction_fromKafka.create_spark_session(
        include_azure_storage=True
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        loaded_df = load_dataframe(spark)

        loaded_df.printSchema()
        loaded_df.show(args.limit, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
