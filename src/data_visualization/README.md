# Data Visualization

This folder contains the Streamlit dashboard for the Gold star-schema layer.

Run it from the project root:

```bash
uv run streamlit run src/data_visualization/gold_dashboard.py
```

The dashboard reads these Gold tables from Azure Storage:

- `dim_date`
- `dim_wiki`
- `dim_page`
- `dim_user`
- `dim_event_type`
- `fact_recent_changes`
