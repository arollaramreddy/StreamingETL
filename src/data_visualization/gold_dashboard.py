from pathlib import Path
import sys

import pandas as pd
import streamlit as st
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    count,
    countDistinct,
    desc,
    lit,
    sum as spark_sum,
    when,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from data_transformation import azure_storage_utils  # noqa: E402
from data_transformation import spark_data_extraction_fromKafka  # noqa: E402


st.set_page_config(
    page_title="StreamingETL Gold Dashboard",
    page_icon="",
    layout="wide",
)


def read_gold_table(spark: SparkSession, table_name: str) -> DataFrame:
    # I read each Gold table from its own folder under the Gold base path.
    azure_storage_utils.configure_azure_storage_access(spark)
    table_path = azure_storage_utils.build_abfss_path(
        "AZURE_GOLD_CONTAINER_NAME",
        "AZURE_GOLD_OUTPUT_PATH",
        table_name,
    )
    table_format = azure_storage_utils.get_optional_runtime_env(
        "AZURE_GOLD_OUTPUT_FORMAT",
        "parquet",
    )
    return spark.read.format(table_format).load(table_path)


@st.cache_resource
def get_spark_session() -> SparkSession:
    # I keep one Spark session alive for the dashboard instead of recreating it.
    spark_data_extraction_fromKafka.configure_java_home()
    spark = spark_data_extraction_fromKafka.create_spark_session(
        include_azure_storage=True
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def to_pandas(dataframe: DataFrame) -> pd.DataFrame:
    # I convert small aggregated Spark dataframes into pandas for Streamlit charts.
    return dataframe.toPandas()


def get_gold_dataframes(spark: SparkSession) -> dict[str, DataFrame]:
    # I load the star-schema tables created by silver_to_gold.py.
    return {
        "fact_recent_changes": read_gold_table(spark, "fact_recent_changes"),
        "dim_wiki": read_gold_table(spark, "dim_wiki"),
        "dim_page": read_gold_table(spark, "dim_page"),
        "dim_user": read_gold_table(spark, "dim_user"),
        "dim_event_type": read_gold_table(spark, "dim_event_type"),
    }


def build_kpis(fact_df: DataFrame) -> pd.DataFrame:
    # I keep the KPI query small so the dashboard loads quickly.
    return to_pandas(
        fact_df.agg(
            count("*").alias("total_events"),
            spark_sum(when(col("is_bot"), lit(1)).otherwise(lit(0))).alias("bot_events"),
            countDistinct("user_key").alias("unique_users"),
            countDistinct("page_key").alias("unique_pages"),
        )
    )


def build_events_by_date(fact_df: DataFrame) -> pd.DataFrame:
    return to_pandas(
        fact_df.groupBy("event_date")
        .agg(count("*").alias("events"))
        .orderBy("event_date")
    )


def build_events_by_hour(fact_df: DataFrame) -> pd.DataFrame:
    return to_pandas(
        fact_df.groupBy("event_hour")
        .agg(count("*").alias("events"))
        .orderBy("event_hour")
    )


def build_events_by_type(
    fact_df: DataFrame,
    event_type_df: DataFrame,
    top_n: int,
) -> pd.DataFrame:
    return to_pandas(
        fact_df.join(event_type_df, "event_type_key", "left")
        .groupBy(coalesce(col("event_type"), lit("unknown")).alias("event_type"))
        .agg(count("*").alias("events"))
        .orderBy(desc("events"))
        .limit(top_n)
    )


def build_events_by_wiki(
    fact_df: DataFrame,
    wiki_df: DataFrame,
    top_n: int,
) -> pd.DataFrame:
    return to_pandas(
        fact_df.join(wiki_df, "wiki_key", "left")
        .groupBy(coalesce(col("wiki"), lit("unknown")).alias("wiki"))
        .agg(count("*").alias("events"))
        .orderBy(desc("events"))
        .limit(top_n)
    )


def build_top_users(
    fact_df: DataFrame,
    user_df: DataFrame,
    top_n: int,
) -> pd.DataFrame:
    return to_pandas(
        fact_df.join(user_df, "user_key", "left")
        .groupBy(
            coalesce(col("event_user"), lit("unknown")).alias("event_user"),
            coalesce(col("is_bot"), lit(False)).alias("is_bot"),
        )
        .agg(count("*").alias("events"))
        .orderBy(desc("events"))
        .limit(top_n)
    )


def build_top_pages(
    fact_df: DataFrame,
    page_df: DataFrame,
    top_n: int,
) -> pd.DataFrame:
    return to_pandas(
        fact_df.join(page_df, "page_key", "left")
        .groupBy(
            coalesce(col("page_title"), lit("unknown")).alias("page_title"),
            coalesce(col("namespace"), lit(-1)).alias("namespace"),
        )
        .agg(count("*").alias("events"))
        .orderBy(desc("events"))
        .limit(top_n)
    )


def build_recent_events(
    fact_df: DataFrame,
    wiki_df: DataFrame,
    page_df: DataFrame,
    user_df: DataFrame,
    event_type_df: DataFrame,
    row_limit: int,
) -> pd.DataFrame:
    return to_pandas(
        fact_df.join(wiki_df, "wiki_key", "left")
        .join(page_df, "page_key", "left")
        .join(user_df, "user_key", "left")
        .join(event_type_df, "event_type_key", "left")
        .select(
            "event_datetime",
            "event_type",
            "wiki",
            "page_title",
            "event_user",
            "is_bot",
            "namespace",
            "comment",
        )
        .orderBy(desc("event_datetime"))
        .limit(row_limit)
    )


@st.cache_data(ttl=300, show_spinner=False)
def load_dashboard_data(_refresh_token: int, top_n: int, row_limit: int) -> dict[str, pd.DataFrame]:
    # I cache the final pandas tables so the app feels responsive while exploring.
    spark = get_spark_session()
    tables = get_gold_dataframes(spark)
    fact_df = tables["fact_recent_changes"]

    return {
        "kpis": build_kpis(fact_df),
        "events_by_date": build_events_by_date(fact_df),
        "events_by_hour": build_events_by_hour(fact_df),
        "events_by_type": build_events_by_type(
            fact_df,
            tables["dim_event_type"],
            top_n,
        ),
        "events_by_wiki": build_events_by_wiki(fact_df, tables["dim_wiki"], top_n),
        "top_users": build_top_users(fact_df, tables["dim_user"], top_n),
        "top_pages": build_top_pages(fact_df, tables["dim_page"], top_n),
        "recent_events": build_recent_events(
            fact_df,
            tables["dim_wiki"],
            tables["dim_page"],
            tables["dim_user"],
            tables["dim_event_type"],
            row_limit,
        ),
    }


def render_metric_cards(kpis: pd.DataFrame) -> None:
    if kpis.empty:
        st.warning("No Gold fact records found.")
        return

    row = kpis.iloc[0]
    total_events = int(row["total_events"] or 0)
    bot_events = int(row["bot_events"] or 0)
    bot_share = (bot_events / total_events * 100) if total_events else 0

    metric_columns = st.columns(4)
    metric_columns[0].metric("Events", f"{total_events:,}")
    metric_columns[1].metric("Bot Events", f"{bot_events:,}", f"{bot_share:.1f}%")
    metric_columns[2].metric("Users", f"{int(row['unique_users'] or 0):,}")
    metric_columns[3].metric("Pages", f"{int(row['unique_pages'] or 0):,}")


def render_line_chart(dataframe: pd.DataFrame, index_column: str, value_column: str) -> None:
    if dataframe.empty:
        st.info("No rows to chart.")
        return

    chart_df = dataframe.set_index(index_column)[[value_column]]
    st.line_chart(chart_df)


def render_bar_chart(dataframe: pd.DataFrame, index_column: str, value_column: str) -> None:
    if dataframe.empty:
        st.info("No rows to chart.")
        return

    chart_df = dataframe.set_index(index_column)[[value_column]]
    st.bar_chart(chart_df)


def main() -> None:
    st.title("Wikimedia Recent Changes")

    with st.sidebar:
        top_n = st.slider("Top N", min_value=5, max_value=30, value=10, step=5)
        row_limit = st.slider("Recent Rows", min_value=10, max_value=100, value=25, step=5)
        refresh_clicked = st.button("Refresh")

    refresh_token = 1 if refresh_clicked else 0

    try:
        with st.spinner("Loading Gold tables from Azure Storage..."):
            dashboard_data = load_dashboard_data(refresh_token, top_n, row_limit)
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    render_metric_cards(dashboard_data["kpis"])

    trend_tab, breakdown_tab, detail_tab = st.tabs(
        ["Trends", "Breakdowns", "Recent Events"]
    )

    with trend_tab:
        left_column, right_column = st.columns(2)
        with left_column:
            st.subheader("Events By Date")
            render_line_chart(dashboard_data["events_by_date"], "event_date", "events")
        with right_column:
            st.subheader("Events By Hour")
            render_bar_chart(dashboard_data["events_by_hour"], "event_hour", "events")

    with breakdown_tab:
        left_column, right_column = st.columns(2)
        with left_column:
            st.subheader("Event Types")
            render_bar_chart(dashboard_data["events_by_type"], "event_type", "events")
            st.subheader("Top Users")
            st.dataframe(dashboard_data["top_users"], use_container_width=True)
        with right_column:
            st.subheader("Wikis")
            render_bar_chart(dashboard_data["events_by_wiki"], "wiki", "events")
            st.subheader("Top Pages")
            st.dataframe(dashboard_data["top_pages"], use_container_width=True)

    with detail_tab:
        st.dataframe(dashboard_data["recent_events"], use_container_width=True)


if __name__ == "__main__":
    main()
