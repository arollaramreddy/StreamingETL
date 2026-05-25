from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    current_date,
    current_timestamp,
    date_sub,
    datediff,
    from_unixtime,
    length,
    lit,
    to_date,
    to_timestamp,
    trim,
    when,
)


QUALITY_RULE_VERSION = "wikimedia_recent_change_v1"

EXPECTED_LOADED_COLUMNS = {
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "message_key",
    "raw_message",
    "bronze_ingested_at",
    "source_system",
    "meta_id",
    "domain",
    "stream_name",
    "event_datetime",
    "event_id",
    "event_unix_timestamp",
    "event_type",
    "namespace",
    "page_title",
    "page_url",
    "event_user",
    "is_bot",
    "server_name",
    "wiki",
    "comment",
    "parsed_comment",
    "is_minor",
    "is_patrolled",
}

REQUIRED_BUSINESS_COLUMNS = (
    "meta_id",
    "event_id",
    "event_type",
    "wiki",
    "namespace",
    "page_title",
)


def validate_loaded_schema(loaded_df: DataFrame) -> None:
    # I fail fast if my Bronze parser no longer produces the columns Silver expects.
    missing_columns = EXPECTED_LOADED_COLUMNS - set(loaded_df.columns)
    if missing_columns:
        raise RuntimeError(
            "Loaded dataframe schema is missing columns: "
            f"{', '.join(sorted(missing_columns))}"
        )


def missing_required_fields_condition():
    # I treat blank strings like nulls for required text fields.
    condition = lit(False)

    for column_name in REQUIRED_BUSINESS_COLUMNS:
        column = col(column_name)
        missing_column = column.isNull()

        if column_name in {"meta_id", "event_type", "wiki", "page_title"}:
            missing_column = missing_column | (length(trim(column.cast("string"))) == 0)

        condition = condition | missing_column

    return condition


def missing_required_field_names():
    # I store the exact missing fields so quarantine records are easier to debug.
    missing_columns = []

    for column_name in REQUIRED_BUSINESS_COLUMNS:
        column = col(column_name)
        missing_column = column.isNull()

        if column_name in {"meta_id", "event_type", "wiki", "page_title"}:
            missing_column = missing_column | (length(trim(column.cast("string"))) == 0)

        missing_columns.append(
            when(missing_column, lit(column_name)).otherwise(lit(None))
        )

    return concat_ws(",", *missing_columns)


def prepare_for_quality_checks(
    loaded_df: DataFrame,
    late_arrival_days: int,
) -> DataFrame:
    validate_loaded_schema(loaded_df)

    raw_message_missing = (
        col("raw_message").isNull()
        | (length(trim(col("raw_message").cast("string"))) == 0)
    )
    schema_parse_failed = (
        ~raw_message_missing
        & col("meta_id").isNull()
        & col("event_id").isNull()
        & col("event_type").isNull()
        & col("wiki").isNull()
        & col("page_title").isNull()
    )

    prepared_df = (
        loaded_df.withColumn(
            "parsed_event_datetime",
            coalesce(
                to_timestamp(col("event_datetime")),
                to_timestamp(from_unixtime(col("event_unix_timestamp"))),
            ),
        )
        .withColumn("event_date", to_date(col("parsed_event_datetime")))
        .withColumn("bronze_ingested_date", to_date(col("bronze_ingested_at")))
        .withColumn(
            "arrival_lag_days",
            datediff(col("bronze_ingested_date"), col("event_date")),
        )
        .withColumn(
            "is_late_arriving",
            coalesce(col("arrival_lag_days") > lit(late_arrival_days), lit(False)),
        )
        .withColumn("quality_checked_at", current_timestamp())
        .withColumn("quality_checked_date", current_date())
        .withColumn(
            "processing_window_date",
            coalesce(
                col("event_date"),
                col("bronze_ingested_date"),
                col("quality_checked_date"),
            ),
        )
        .withColumn("quality_rule_version", lit(QUALITY_RULE_VERSION))
        .withColumn(
            "missing_required_fields",
            missing_required_field_names(),
        )
    )

    return prepared_df.withColumn(
        "quality_error_category",
        when(raw_message_missing, lit("raw_message_missing"))
        .when(schema_parse_failed, lit("schema_parse_failed"))
        .when(missing_required_fields_condition(), lit("missing_required_field"))
        .when(col("parsed_event_datetime").isNull(), lit("invalid_event_timestamp"))
        .otherwise(lit(None)),
    ).withColumn(
        "quality_error_reason",
        when(
            col("quality_error_category") == "raw_message_missing",
            lit("Raw Kafka message is null or blank."),
        )
        .when(
            col("quality_error_category") == "schema_parse_failed",
            lit("Raw message did not parse into the expected Wikimedia schema."),
        )
        .when(
            col("quality_error_category") == "missing_required_field",
            coalesce(
                col("missing_required_fields"),
                lit("One or more required business fields are missing."),
            ),
        )
        .when(
            col("quality_error_category") == "invalid_event_timestamp",
            lit("Event timestamp could not be parsed from meta.dt or timestamp."),
        )
        .otherwise(lit(None)),
    )


def apply_processing_window(
    dataframe: DataFrame,
    backfill_start_date: str | None = None,
    backfill_end_date: str | None = None,
    lookback_days: int | None = None,
) -> DataFrame:
    # I use event_date for normal records, then fall back to Bronze ingestion date
    # so malformed records still reach quarantine instead of disappearing.
    windowed_df = dataframe

    if backfill_start_date:
        windowed_df = windowed_df.where(
            col("processing_window_date") >= lit(backfill_start_date)
        )

    if backfill_end_date:
        windowed_df = windowed_df.where(
            col("processing_window_date") <= lit(backfill_end_date)
        )

    if not backfill_start_date and not backfill_end_date and lookback_days is not None:
        windowed_df = windowed_df.where(
            col("processing_window_date") >= date_sub(current_date(), lookback_days)
        )

    return windowed_df


def split_valid_and_quarantine(dataframe: DataFrame) -> tuple[DataFrame, DataFrame]:
    # I keep good rows moving to Silver and send bad rows to quarantine for replay/debugging.
    valid_df = dataframe.where(col("quality_error_category").isNull())
    quarantine_df = dataframe.where(col("quality_error_category").isNotNull())
    return valid_df, quarantine_df


def select_quarantine_columns(quarantine_df: DataFrame) -> DataFrame:
    # I store enough raw and parsed context to explain why a record failed validation.
    return quarantine_df.select(
        "quality_error_category",
        "quality_error_reason",
        "quality_checked_at",
        "quality_checked_date",
        "quality_rule_version",
        "missing_required_fields",
        "raw_message",
        "source_system",
        "message_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "bronze_ingested_at",
        "bronze_ingested_date",
        "processing_window_date",
        "meta_id",
        "event_id",
        "event_type",
        "event_datetime",
        "event_unix_timestamp",
        "parsed_event_datetime",
        "event_date",
        "wiki",
        "domain",
        "namespace",
        "page_title",
        "event_user",
    )
