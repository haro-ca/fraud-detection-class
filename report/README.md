# Fraud Detection Pipeline - Project Report

## Overview

This project extracts and productionalizes fraud detection logic from a Jupyter notebook into a modular, production-ready Python application.

## Issues Resolved

| # | Title | Phase | Status |
|---|-------|-------|--------|
| 1 | Epic: credit approvals must show in app after fraud pipeline | 10 | ✅ |
| 2 | chore: extract fraud rules from notebook into src/ modules | 1 | ✅ |
| 3 | feat: batch ETL pipeline — Neon → DuckDB / Databricks | 7 | ✅ |
| 4 | feat: fraud status API endpoint + show results in Django app | 5 | ✅ |
| 5 | feat: CLI for pipeline operations via Rich/Typer | 6 | ✅ |
| 6 | feat: fraud review queue in Django admin | 5 | ✅ |
| 7 | feat: MCP server for application status and fraud detection | 9 | ✅ |
| 8 | feat: Streamlit analytics dashboard over OLAP layer | 8 | ✅ |
| 9 | chore: extract each fraud rule into a callable function | 1 | ✅ |
| 10 | test: add tests for extracted fraud rules | 2 | ✅ |
| 11 | chore: create entry point and wire notebook to extracted modules | 3 | ✅ |
| 12 | chore: add logging to the fraud pipeline | 4 | ✅ |

## Project Structure

```
fraud-detection-class/
├── config/                  # Django settings
├── creditapp/              # Django app
│   ├── models.py           # CreditApplication, Transaction, FraudResult
│   ├── views.py            # API endpoints
│   ├── admin.py            # Admin with fraud review queue
│   ├── urls.py             # URL routing
│   └── management/commands/
│       └── run_fraud_pipeline.py
├── src/
│   └── rules/              # Extracted fraud rules
│       ├── __init__.py    # run_all()
│       ├── velocity.py     # Rule 1
│       ├── income.py      # Rule 2
│       ├── geo.py         # Rule 3
│       ├── high_risk.py   # Rule 4
│       └── unusual_hours.py # Rule 5
├── scripts/
│   ├── pipeline.py         # Main pipeline script
│   ├── cli.py              # Rich CLI interface
│   └── etl.py              # ETL to DuckDB
├── dashboard/
│   └── app.py              # Streamlit dashboard
├── mcp_server/
│   ├── server.py           # MCP tools
│   └── main.py             # Server entry point
├── tests/                  # Test suite (34 tests)
├── data/                   # DuckDB analytics database
├── report/                  # This report
└── notebooks/
    └── fraud_analysis.ipynb # Original notebook
```

## Fraud Detection Rules

1. **velocity_check** - >3 transactions in 4 hours
2. **income_ratio** - Spending >50% of annual income
3. **geo_anomaly** - 3+ countries in 48 hours
4. **high_risk_merchant** - >30% in gambling/crypto/cash
5. **unusual_hours** - >50% between midnight-5AM

## Usage

### Run Pipeline
```bash
uv run python scripts/pipeline.py
# or
uv run python manage.py run_fraud_pipeline
# or
uv run python scripts/cli.py run
```

### CLI
```bash
uv run python scripts/cli.py --help
uv run python scripts/cli.py run
uv run python scripts/cli.py status
uv run python scripts/cli.py etl --days 30
```

### Dashboard
```bash
uv run streamlit run dashboard/app.py
```

### MCP Server
```bash
uv run python mcp_server/main.py
```

### Run Tests
```bash
uv run pytest tests/ -v
```

## Technology Stack

- **Framework**: Django 5.1
- **Data Processing**: Polars
- **Database**: PostgreSQL (Neon)
- **Analytics**: DuckDB
- **CLI**: Rich + Typer
- **Dashboard**: Streamlit
- **MCP**: MCP SDK
- **Testing**: pytest
- **Linting**: ruff
