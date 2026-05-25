# Data Visualization

This folder contains the Streamlit dashboard for the Gold dashboard tables.
It is built as a serving-layer dashboard: Streamlit reads pre-aggregated Gold
tables, uses Redis when available, exposes freshness checks, and provides CSV
exports for operations and analysis.

Run it from the project root:

```bash
uv run streamlit run src/data_visualization/gold_dashboard.py
```

The dashboard reads these Gold aggregate tables from Azure Storage:

- `agg_dashboard_kpis`
- `agg_events_by_date`
- `agg_events_by_window`
- `agg_events_by_type`
- `agg_events_by_wiki`
- `agg_top_users`
- `agg_top_pages`
- `recent_events_snapshot`
