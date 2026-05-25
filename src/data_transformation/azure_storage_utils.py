import os

from pyspark.sql import DataFrame, SparkSession


def get_required_runtime_env(name: str) -> str:
    # I stop early if a required Azure value is empty or still a placeholder.
    value = os.getenv(name)
    if not value or value.strip().startswith("<"):
        raise RuntimeError(f"Replace the placeholder for {name} in .env first.")
    return value.strip()


def get_optional_runtime_env(name: str, default_value: str) -> str:
    # I use a default so local testing needs fewer settings in .env.
    value = os.getenv(name)
    if not value or value.strip().startswith("<"):
        return default_value
    return value.strip()


def get_partition_columns(name: str, default_value: str) -> list[str]:
    # I keep partition columns configurable because Bronze and Silver have different needs.
    raw_columns = get_optional_runtime_env(name, default_value)
    return [column.strip() for column in raw_columns.split(",") if column.strip()]


def configure_azure_storage_access(spark: SparkSession) -> None:
    # I use account-key auth for local testing. Later this can change to SAS/OAuth.
    account_name = get_required_runtime_env("AZURE_STORAGE_ACCOUNT_NAME")
    account_key = get_required_runtime_env("AZURE_STORAGE_ACCOUNT_KEY")

    spark.conf.set(
        f"fs.azure.account.key.{account_name}.dfs.core.windows.net",
        account_key,
    )


def build_abfss_path(
    container_env_name: str,
    path_env_name: str,
    child_path: str | None = None,
) -> str:
    # I build an ADLS Gen2 path from .env values so containers can change later.
    account_name = get_required_runtime_env("AZURE_STORAGE_ACCOUNT_NAME")
    container_name = get_required_runtime_env(container_env_name)
    output_path = get_required_runtime_env(path_env_name).strip("/")

    if child_path:
        output_path = f"{output_path}/{child_path.strip('/')}"

    return (
        f"abfss://{container_name}@{account_name}.dfs.core.windows.net/{output_path}"
    )


def read_dataframe_from_lake(
    spark: SparkSession,
    container_env_name: str,
    path_env_name: str,
    format_env_name: str,
    default_format: str = "parquet",
) -> DataFrame:
    configure_azure_storage_access(spark)

    input_path = build_abfss_path(container_env_name, path_env_name)
    input_format = get_optional_runtime_env(format_env_name, default_format)
    return spark.read.format(input_format).load(input_path)


def write_dataframe_to_lake_path(
    spark: SparkSession,
    dataframe: DataFrame,
    container_env_name: str,
    path_env_name: str,
    format_env_name: str,
    mode_env_name: str,
    partition_columns_env_name: str,
    default_partition_columns: str,
    child_path: str | None = None,
) -> str:
    configure_azure_storage_access(spark)

    output_path = build_abfss_path(container_env_name, path_env_name, child_path)
    output_format = get_optional_runtime_env(format_env_name, "parquet")
    write_mode = get_optional_runtime_env(mode_env_name, "append")
    partition_columns = get_partition_columns(
        partition_columns_env_name,
        default_partition_columns,
    )

    # I write Parquet by default because it keeps schema and is efficient for Spark.
    writer = dataframe.write.mode(write_mode).format(output_format)

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    try:
        writer.save(output_path)
    except Exception as exc:
        error_message = str(exc)
        if (
            "FilesystemNotFound" in error_message
            or "specified filesystem does not exist" in error_message
        ):
            container_name = get_required_runtime_env(container_env_name)
            raise RuntimeError(
                f"Azure container/filesystem '{container_name}' does not exist. "
                "Create it in the storage account first, then rerun the job."
            ) from None
        raise

    return output_path


def write_dataframe_to_lake(
    spark: SparkSession,
    dataframe: DataFrame,
    container_env_name: str,
    path_env_name: str,
    format_env_name: str,
    mode_env_name: str,
    partition_columns_env_name: str,
    default_partition_columns: str,
) -> str:
    return write_dataframe_to_lake_path(
        spark=spark,
        dataframe=dataframe,
        container_env_name=container_env_name,
        path_env_name=path_env_name,
        format_env_name=format_env_name,
        mode_env_name=mode_env_name,
        partition_columns_env_name=partition_columns_env_name,
        default_partition_columns=default_partition_columns,
    )
