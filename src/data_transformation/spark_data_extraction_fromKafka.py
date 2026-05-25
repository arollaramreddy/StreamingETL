import argparse
import os
import subprocess
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def get_optional_env(name: str) -> str | None:
    value = os.getenv(name)
    if not value or value.strip().startswith("<"):
        return None
    return value.strip()


def configure_java_home() -> None:
    spark_java_home = os.getenv("SPARK_JAVA_HOME")
    if spark_java_home:
        os.environ["JAVA_HOME"] = spark_java_home
        os.environ["PATH"] = f"{spark_java_home}/bin:{os.environ.get('PATH', '')}"
        return

    try:
        java_home = subprocess.check_output(
            ["/usr/libexec/java_home", "-v", "17"],
            text=True,
        ).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return

    os.environ["JAVA_HOME"] = java_home
    os.environ["PATH"] = f"{java_home}/bin:{os.environ.get('PATH', '')}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read raw messages from Kafka.")
    parser.add_argument("--limit", type=int, default=5)
    return parser.parse_args()


def create_spark_session(include_azure_storage: bool = False) -> SparkSession:
    spark_packages = [get_required_env("SPARK_KAFKA_CONNECTOR_PACKAGE")]
    azure_storage_package = get_optional_env("SPARK_AZURE_STORAGE_CONNECTOR_PACKAGE")

    if include_azure_storage and not azure_storage_package:
        raise RuntimeError(
            "Azure writes need the ABFS connector. Add this to .env: "
            "SPARK_AZURE_STORAGE_CONNECTOR_PACKAGE=org.apache.hadoop:hadoop-azure:3.4.2"
        )

    if include_azure_storage or azure_storage_package:
        spark_packages.append(azure_storage_package)

    spark_builder = SparkSession.builder.appName("StreamingETL Kafka Reader").config(
        "spark.jars.packages",
        ",".join(spark_packages),
    )

    optional_spark_configs = {
        "spark.jars.ivy": os.getenv("SPARK_IVY_DIR"),
        "spark.sql.shuffle.partitions": os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS"),
        "spark.sql.adaptive.enabled": os.getenv("SPARK_ADAPTIVE_EXECUTION_ENABLED"),
        "spark.sql.adaptive.coalescePartitions.enabled": os.getenv(
            "SPARK_ADAPTIVE_COALESCE_PARTITIONS_ENABLED"
        ),
        "spark.sql.sources.partitionOverwriteMode": os.getenv(
            "SPARK_PARTITION_OVERWRITE_MODE"
        ),
    }

    for config_name, config_value in optional_spark_configs.items():
        if config_value:
            spark_builder = spark_builder.config(config_name, config_value)

    return spark_builder.getOrCreate()


def main() -> None:
    configure_java_home()
    args = parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", get_required_env("KAFKA_BOOTSTRAP_SERVERS"))
            .option("subscribe", get_required_env("KAFKA_TOPIC"))
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )

        kafka_df.printSchema()

        '''kafka_df.select(
            col("key").cast("string").alias("key"),
            col("value").cast("string").alias("value"),
            "topic",
            "partition",
            "offset",
            "timestamp",
        ).show(args.limit, truncate=False)'''
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
