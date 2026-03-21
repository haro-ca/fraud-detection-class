# Phase 7: Batch ETL Pipeline

## Issue Addressed
- #3: Batch ETL pipeline — Neon → DuckDB (local) / Databricks (prod)

## What Was Built

Incremental ETL pipeline for analytics workloads:

```bash
# Load last 7 days (default)
uv run python scripts/etl.py

# Load last 30 days
uv run python scripts/etl.py 30
```

## Architecture

```
Neon (OLTP)  ──load──>  DuckDB (OLAP)
                            │
                            └── data/fraud_analytics.duckdb
                                    │
                                    ├── analytics.applications
                                    ├── analytics.transactions
                                    └── analytics.fraud_results
```

## Key Features

### Incremental Loading
- Loads only data from last N days (default: 7)
- Tracks sync timestamps in DuckDB
- Upsert logic prevents duplicates

### Sync Tracking
```python
def get_last_sync_timestamp() -> Optional[datetime]:
    # Returns last ETL run timestamp

def update_sync_timestamp() -> None:
    # Records current ETL run
```

### Tables Loaded
1. **analytics.applications** - Credit applications
2. **analytics.transactions** - Transaction history
3. **analytics.fraud_results** - Fraud detection results

## Usage

```bash
# Via standalone script
uv run python scripts/etl.py 7

# Via CLI
uv run python scripts/cli.py etl --days 30

# In Python code
from scripts.etl import run_etl
run_etl(days=7)
```

## Databricks Migration Path

The ETL interface is designed for easy migration:
- Same `run_etl()` function signature
- Only `write_to_duckdb()` needs replacement
- Swap for Spark DataFrames or Databricks SQL

## Dependencies Added
- `duckdb` - Local analytics database

## Files Created
- `scripts/etl.py` (new)
