from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import os
from pathlib import Path
import sys
from typing import Any

import pandas as pd
import streamlit as st
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import desc


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"

if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from data_transformation import azure_storage_utils  # noqa: E402
from data_transformation import spark_data_extraction_fromKafka  # noqa: E402

try:
    from . import redis_cache
except ImportError:
    import redis_cache


st.set_page_config(
    page_title="StreamingETL Executive Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


@dataclass(frozen=True)
class DashboardConfig:
    default_top_n: int
    max_top_n: int
    default_recent_rows: int
    max_recent_rows: int
    stale_after_minutes: int
    cache_ttl_seconds: int
    show_debug_errors: bool


def env_int(name: str, default_value: int) -> int:
    value = os.getenv(name)
    if not value or value.strip().startswith("<"):
        return default_value
    try:
        return int(value)
    except ValueError:
        return default_value


def env_bool(name: str, default_value: bool) -> bool:
    value = os.getenv(name)
    if value is None or value.strip().startswith("<"):
        return default_value
    return value.lower() in {"1", "true", "yes"}


def clamp(value: int, minimum: int, maximum: int) -> int:
    return max(minimum, min(value, maximum))


def get_dashboard_config() -> DashboardConfig:
    return DashboardConfig(
        default_top_n=env_int("DASHBOARD_DEFAULT_TOP_N", 10),
        max_top_n=env_int("DASHBOARD_MAX_TOP_N", 30),
        default_recent_rows=env_int("DASHBOARD_DEFAULT_RECENT_ROWS", 25),
        max_recent_rows=env_int("DASHBOARD_MAX_RECENT_ROWS", 100),
        stale_after_minutes=env_int("DASHBOARD_STALE_AFTER_MINUTES", 30),
        cache_ttl_seconds=env_int("REDIS_CACHE_TTL_SECONDS", 300),
        show_debug_errors=env_bool("DASHBOARD_SHOW_DEBUG_ERRORS", False),
    )


def read_gold_table(spark: SparkSession, table_name: str) -> DataFrame:
    # I read curated Gold tables, so the dashboard behaves like a BI serving layer.
    azure_storage_utils.configure_azure_storage_access(spark)
    table_path = azure_storage_utils.build_abfss_path(
        "AZURE_GOLD_CONTAINER_NAME",
        "AZURE_GOLD_OUTPUT_PATH",
        child_path=table_name,
    )
    table_format = azure_storage_utils.get_optional_runtime_env(
        "AZURE_GOLD_OUTPUT_FORMAT",
        "parquet",
    )

    if table_format == "parquet" and not azure_storage_utils.lake_path_has_data_files(
        spark,
        table_path,
    ):
        raise RuntimeError(
            f"Gold table '{table_name}' is missing or empty. "
            "Run the Silver to Gold pipeline step before opening the dashboard."
        )

    return spark.read.format(table_format).load(table_path)


@st.cache_resource
def get_spark_session() -> SparkSession:
    # I keep one Spark session alive for the Streamlit process.
    spark_data_extraction_fromKafka.configure_java_home()
    spark = spark_data_extraction_fromKafka.create_spark_session(
        include_azure_storage=True
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def to_pandas(dataframe: DataFrame) -> pd.DataFrame:
    # I only collect Gold aggregate tables, not raw Bronze or Silver data.
    return dataframe.toPandas()


def load_gold_pandas_table(
    spark: SparkSession,
    table_name: str,
    order_by: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    dataframe = read_gold_table(spark, table_name)

    if order_by:
        dataframe = dataframe.orderBy(desc(order_by))

    if limit:
        dataframe = dataframe.limit(limit)

    return to_pandas(dataframe)


def normalize_dashboard_data(
    dashboard_data: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    # I normalize date columns once so filters and charts behave consistently.
    datetime_columns = {
        "events_by_window": ["window_start", "window_end"],
        "recent_events": ["event_datetime"],
    }
    date_columns = {
        "events_by_date": ["event_date"],
        "events_by_window": ["event_date"],
        "recent_events": ["event_date"],
    }

    for table_name, column_names in datetime_columns.items():
        dataframe = dashboard_data.get(table_name)
        if dataframe is None:
            continue
        for column_name in column_names:
            if column_name in dataframe.columns:
                dataframe[column_name] = pd.to_datetime(
                    dataframe[column_name],
                    errors="coerce",
                )

    for table_name, column_names in date_columns.items():
        dataframe = dashboard_data.get(table_name)
        if dataframe is None:
            continue
        for column_name in column_names:
            if column_name in dataframe.columns:
                dataframe[column_name] = pd.to_datetime(
                    dataframe[column_name],
                    errors="coerce",
                ).dt.date

    return dashboard_data


@st.cache_data(
    ttl=env_int("DASHBOARD_STREAMLIT_CACHE_TTL_SECONDS", 300),
    show_spinner=False,
)
def load_dashboard_data(
    refresh_token: int,
    max_top_n: int,
    max_recent_rows: int,
) -> dict[str, pd.DataFrame]:
    cache_key = redis_cache.build_cache_key(
        "gold_dashboard",
        refresh_token,
        max_top_n,
        max_recent_rows,
    )
    cached_data = redis_cache.get_cached_value(cache_key)
    if cached_data is not None:
        return cached_data

    spark = get_spark_session()
    dashboard_data = {
        "kpis": load_gold_pandas_table(spark, "agg_dashboard_kpis"),
        "events_by_date": load_gold_pandas_table(
            spark,
            "agg_events_by_date",
            order_by="event_date",
        ),
        "events_by_window": load_gold_pandas_table(
            spark,
            "agg_events_by_window",
            order_by="window_start",
        ),
        "events_by_type": load_gold_pandas_table(
            spark,
            "agg_events_by_type",
            order_by="events",
            limit=max_top_n,
        ),
        "events_by_wiki": load_gold_pandas_table(
            spark,
            "agg_events_by_wiki",
            order_by="events",
            limit=max_top_n,
        ),
        "top_users": load_gold_pandas_table(
            spark,
            "agg_top_users",
            order_by="events",
            limit=max_top_n,
        ),
        "top_pages": load_gold_pandas_table(
            spark,
            "agg_top_pages",
            order_by="events",
            limit=max_top_n,
        ),
        "recent_events": load_gold_pandas_table(
            spark,
            "recent_events_snapshot",
            order_by="event_datetime",
            limit=max_recent_rows,
        ),
    }
    dashboard_data = normalize_dashboard_data(dashboard_data)

    redis_cache.set_cached_value(cache_key, dashboard_data)
    return dashboard_data


def safe_int(value: Any) -> int:
    if pd.isna(value):
        return 0
    return int(value)


def safe_percent(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator * 100


def get_date_bounds(events_by_date: pd.DataFrame) -> tuple[date, date]:
    if events_by_date.empty or "event_date" not in events_by_date.columns:
        today = date.today()
        return today, today

    dates = events_by_date["event_date"].dropna()
    if dates.empty:
        today = date.today()
        return today, today

    return dates.min(), dates.max()


def filter_by_date_range(
    dataframe: pd.DataFrame,
    column_name: str,
    selected_range: tuple[date, date],
) -> pd.DataFrame:
    if dataframe.empty or column_name not in dataframe.columns:
        return dataframe

    start_date, end_date = selected_range
    return dataframe[
        (dataframe[column_name] >= start_date)
        & (dataframe[column_name] <= end_date)
    ]


def calculate_selected_event_count(events_by_date: pd.DataFrame) -> int:
    if events_by_date.empty or "events" not in events_by_date.columns:
        return 0
    return safe_int(events_by_date["events"].sum())


def latest_event_timestamp(recent_events: pd.DataFrame) -> pd.Timestamp | None:
    if recent_events.empty or "event_datetime" not in recent_events.columns:
        return None

    latest_value = recent_events["event_datetime"].max()
    if pd.isna(latest_value):
        return None

    return latest_value


def freshness_status(
    recent_events: pd.DataFrame,
    stale_after_minutes: int,
) -> tuple[str, str]:
    latest_event = latest_event_timestamp(recent_events)
    if latest_event is None:
        return "Unknown", "No event timestamp available"

    age = datetime.now(latest_event.tzinfo) - latest_event.to_pydatetime()
    status = "Fresh" if age <= timedelta(minutes=stale_after_minutes) else "Stale"
    age_minutes = max(int(age.total_seconds() // 60), 0)
    return status, f"{age_minutes} min old"


def redis_status_label() -> str:
    return "Connected" if redis_cache.get_redis_client() is not None else "Unavailable"


def dataframe_to_csv(dataframe: pd.DataFrame) -> bytes:
    return dataframe.to_csv(index=False).encode("utf-8")


def render_page_header(config: DashboardConfig) -> None:
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 1.4rem;
            padding-bottom: 2rem;
        }
        div[data-testid="stMetric"] {
            border: 1px solid #e5e7eb;
            border-radius: 8px;
            padding: 0.85rem 1rem;
            background: #ffffff;
        }
        div[data-testid="stMetricValue"] {
            font-size: 1.65rem;
        }
        .small-muted {
            color: #6b7280;
            font-size: 0.85rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    title_column, status_column = st.columns([0.72, 0.28])
    with title_column:
        st.title("Wikimedia Recent Changes")
        st.caption("Gold serving layer dashboard")
    with status_column:
        st.caption(f"Cache TTL: {config.cache_ttl_seconds}s")
        st.caption(f"Redis: {redis_status_label()}")


def render_sidebar(
    config: DashboardConfig,
    events_by_date: pd.DataFrame,
) -> tuple[tuple[date, date], int, int]:
    min_date, max_date = get_date_bounds(events_by_date)

    with st.sidebar:
        st.header("Controls")
        selected_dates = st.date_input(
            "Event Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        if not isinstance(selected_dates, (tuple, list)) or len(selected_dates) != 2:
            selected_dates = (min_date, max_date)

        max_top_n = max(5, config.max_top_n)
        default_top_n = clamp(config.default_top_n, 5, max_top_n)
        top_n = st.slider(
            "Top N",
            min_value=5,
            max_value=max_top_n,
            value=default_top_n,
            step=5,
        )
        max_recent_rows = max(10, config.max_recent_rows)
        default_recent_rows = clamp(config.default_recent_rows, 10, max_recent_rows)
        row_limit = st.slider(
            "Recent Rows",
            min_value=10,
            max_value=max_recent_rows,
            value=default_recent_rows,
            step=5,
        )

        refresh_clicked = st.button("Refresh Data", use_container_width=True)
        clear_cache_clicked = st.button("Clear Local Cache", use_container_width=True)

    if refresh_clicked:
        st.session_state.refresh_token += 1
        st.rerun()

    if clear_cache_clicked:
        st.cache_data.clear()
        st.session_state.refresh_token += 1
        st.rerun()

    return selected_dates, top_n, row_limit


def render_metric_cards(
    kpis: pd.DataFrame,
    selected_events: int,
    recent_events: pd.DataFrame,
    config: DashboardConfig,
) -> None:
    if kpis.empty:
        st.warning("No Gold KPI records found.")
        return

    row = kpis.iloc[0]
    total_events = safe_int(row.get("events"))
    bot_events = safe_int(row.get("bot_events"))
    unique_users = safe_int(row.get("unique_users"))
    unique_pages = safe_int(row.get("unique_pages"))
    bot_share = safe_percent(bot_events, total_events)
    freshness, freshness_detail = freshness_status(
        recent_events,
        config.stale_after_minutes,
    )

    metric_columns = st.columns(5)
    metric_columns[0].metric("Selected Events", f"{selected_events:,}")
    metric_columns[1].metric("Total Events", f"{total_events:,}")
    metric_columns[2].metric("Bot Share", f"{bot_share:.1f}%")
    metric_columns[3].metric("Users", f"{unique_users:,}")
    metric_columns[4].metric("Freshness", freshness, freshness_detail)

    st.caption(f"Unique pages: {unique_pages:,}")


def render_line_chart(
    dataframe: pd.DataFrame,
    index_column: str,
    title: str,
) -> None:
    st.subheader(title)
    if dataframe.empty:
        st.info("No rows available for this view.")
        return

    chart_df = dataframe.sort_values(index_column).set_index(index_column)[["events"]]
    st.line_chart(chart_df, height=320)


def render_bar_chart(
    dataframe: pd.DataFrame,
    index_column: str,
    title: str,
    top_n: int,
) -> None:
    st.subheader(title)
    if dataframe.empty:
        st.info("No rows available for this view.")
        return

    chart_df = (
        dataframe.sort_values("events", ascending=False)
        .head(top_n)
        .set_index(index_column)[["events"]]
    )
    st.bar_chart(chart_df, height=320)


def render_download_button(
    label: str,
    dataframe: pd.DataFrame,
    file_name: str,
) -> None:
    st.download_button(
        label=label,
        data=dataframe_to_csv(dataframe),
        file_name=file_name,
        mime="text/csv",
        use_container_width=True,
    )


def render_recent_events(recent_events: pd.DataFrame, row_limit: int) -> None:
    st.subheader("Recent Events")
    if recent_events.empty:
        st.info("No recent events available.")
        return

    visible_columns = [
        "event_datetime",
        "event_type",
        "wiki",
        "page_title",
        "event_user",
        "is_bot",
        "namespace",
        "comment",
    ]
    existing_columns = [
        column_name
        for column_name in visible_columns
        if column_name in recent_events.columns
    ]

    st.dataframe(
        recent_events[existing_columns].head(row_limit),
        use_container_width=True,
        hide_index=True,
        column_config={
            "event_datetime": st.column_config.DatetimeColumn(
                "Event Time",
                format="YYYY-MM-DD HH:mm:ss",
            ),
            "is_bot": st.column_config.CheckboxColumn("Bot"),
            "comment": st.column_config.TextColumn("Comment", width="large"),
        },
    )
    render_download_button(
        "Download Recent Events",
        recent_events[existing_columns].head(row_limit),
        "recent_wikimedia_events.csv",
    )


def render_quality_panel(
    dashboard_data: dict[str, pd.DataFrame],
    config: DashboardConfig,
) -> None:
    latest_event = latest_event_timestamp(dashboard_data["recent_events"])
    freshness, freshness_detail = freshness_status(
        dashboard_data["recent_events"],
        config.stale_after_minutes,
    )

    status_rows = pd.DataFrame(
        [
            {"check": "Gold KPI table", "status": "Ready", "detail": "Loaded"},
            {
                "check": "Recent event timestamp",
                "status": freshness,
                "detail": freshness_detail,
            },
            {
                "check": "Latest event time",
                "status": "Available" if latest_event is not None else "Missing",
                "detail": str(latest_event) if latest_event is not None else "",
            },
            {"check": "Redis cache", "status": redis_status_label(), "detail": ""},
        ]
    )

    st.dataframe(status_rows, use_container_width=True, hide_index=True)


def main() -> None:
    config = get_dashboard_config()
    render_page_header(config)

    if "refresh_token" not in st.session_state:
        st.session_state.refresh_token = 0

    try:
        with st.spinner("Loading Gold serving tables..."):
            dashboard_data = load_dashboard_data(
                st.session_state.refresh_token,
                config.max_top_n,
                config.max_recent_rows,
            )
    except Exception as exc:
        st.error(str(exc))
        if config.show_debug_errors:
            st.exception(exc)
        st.stop()

    selected_dates, top_n, row_limit = render_sidebar(
        config,
        dashboard_data["events_by_date"],
    )

    filtered_events_by_date = filter_by_date_range(
        dashboard_data["events_by_date"],
        "event_date",
        selected_dates,
    )
    filtered_events_by_window = filter_by_date_range(
        dashboard_data["events_by_window"],
        "event_date",
        selected_dates,
    )
    filtered_recent_events = filter_by_date_range(
        dashboard_data["recent_events"],
        "event_date",
        selected_dates,
    )
    selected_event_count = calculate_selected_event_count(filtered_events_by_date)

    render_metric_cards(
        dashboard_data["kpis"],
        selected_event_count,
        filtered_recent_events,
        config,
    )

    overview_tab, breakdown_tab, detail_tab, ops_tab = st.tabs(
        ["Overview", "Breakdowns", "Recent Events", "Operations"]
    )

    with overview_tab:
        left_column, right_column = st.columns(2)
        with left_column:
            render_line_chart(
                filtered_events_by_date,
                "event_date",
                "Events By Date",
            )
        with right_column:
            render_line_chart(
                filtered_events_by_window,
                "window_start",
                "15 Minute Event Windows",
            )

    with breakdown_tab:
        left_column, right_column = st.columns(2)
        with left_column:
            render_bar_chart(
                dashboard_data["events_by_type"],
                "event_type",
                "Event Types",
                top_n,
            )
            st.subheader("Top Users")
            st.dataframe(
                dashboard_data["top_users"].head(top_n),
                use_container_width=True,
                hide_index=True,
            )
        with right_column:
            render_bar_chart(
                dashboard_data["events_by_wiki"],
                "wiki",
                "Wikis",
                top_n,
            )
            st.subheader("Top Pages")
            st.dataframe(
                dashboard_data["top_pages"].head(top_n),
                use_container_width=True,
                hide_index=True,
            )

    with detail_tab:
        render_recent_events(filtered_recent_events, row_limit)

    with ops_tab:
        st.subheader("Dashboard Readiness")
        render_quality_panel(dashboard_data, config)
        st.subheader("Exports")
        export_columns = st.columns(3)
        with export_columns[0]:
            render_download_button(
                "Download Events By Date",
                filtered_events_by_date,
                "events_by_date.csv",
            )
        with export_columns[1]:
            render_download_button(
                "Download Event Types",
                dashboard_data["events_by_type"].head(top_n),
                "events_by_type.csv",
            )
        with export_columns[2]:
            render_download_button(
                "Download Top Pages",
                dashboard_data["top_pages"].head(top_n),
                "top_pages.csv",
            )


if __name__ == "__main__":
    main()
