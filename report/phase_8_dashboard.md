# Phase 8: Streamlit Analytics Dashboard

## Issue Addressed
- #8: Streamlit analytics dashboard over OLAP layer

## What Was Built

Interactive analytics dashboard reading from DuckDB:

```bash
uv run streamlit run dashboard/app.py
```

Opens at: `http://localhost:8501`

## Dashboard Sections

### 1. Key Metrics (Top Row)
- Total Applications
- Approved count
- Rejected count
- Fraud Flags (highlighted in red if high)

### 2. Overview Tab
- Application status distribution (bar chart)
- Rules triggered by application (table)

### 3. Fraud Rules Tab
- Rules summary table (triggered vs total)
- Average score by rule
- Triggered count by rule (bar chart)

### 4. Transactions Tab
- Recent transactions table (last 50)
- Transaction categories (bar chart)

## Data Source

Reads from DuckDB OLAP layer:
```python
conn = duckdb.connect("data/fraud_analytics.duckdb", read_only=True)
```

Shows warning if ETL hasn't been run:
> "No analytics data found. Run the ETL pipeline first"

## Dependencies Added
- `streamlit` - Web dashboard framework
- `pandas` - Required by Streamlit
- `pyarrow` - Parquet support

## Files Created
- `dashboard/app.py` (new)

## Prerequisites

```bash
# Run ETL first to populate DuckDB
uv run python scripts/etl.py

# Then start dashboard
uv run streamlit run dashboard/app.py
```
