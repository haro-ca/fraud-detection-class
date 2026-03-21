# CLAUDE.md

## Project overview

Santandors is a Django credit application system with a fraud detection pipeline. The fraud logic currently lives in a messy Jupyter notebook (`notebooks/fraud_analysis.ipynb`) and needs to be productionized into proper Python modules.

## Commands

```bash
# install dependencies
uv sync

# run django app
uv run python manage.py runserver

# run transaction simulator
uv run python scripts/transactions.py

# run tests
uv run pytest

# lint
uv run ruff check .

# format
uv run ruff format .
```

## Conventions

- **uv** for all dependency management — never use pip, poetry, or pipenv
- **polars** for all dataframe operations — never use pandas
- **Python 3.12+** — do not downgrade
- Keep Django models with `managed = False` — tables are managed by Neon, not Django migrations
- Connection string comes from `DATABASE_URL` in `.env`

## Architecture

- `config/` — Django project settings, urls, wsgi
- `creditapp/` — Django app with models, views, templates, admin
- `notebooks/` — Jupyter notebooks (the messy fraud analysis lives here)
- `scripts/` — Standalone scripts (transaction simulator)

## Database (Neon PostgreSQL)

Three tables:
- `credit_applications` — applicant info, requested amount, status (pending/approved/rejected)
- `transactions` — transaction history per applicant (amount, merchant, category, country, timestamp)
- `fraud_results` — output of fraud rules (rule_name, triggered, score, details)

## Fraud rules (in notebook, to be extracted)

1. **velocity_check** — >3 transactions in 4h window
2. **income_ratio** — total spending >50% of annual income
3. **geo_anomaly** — transactions in 3+ countries within 48h
4. **high_risk_merchant** — >30% of transactions in gambling/crypto/cash
5. **unusual_hours** — >50% of transactions between midnight–5AM

## What NOT to do

- Do not add pandas as a dependency
- Do not change the database schema without discussing first
- Do not hardcode connection strings — always use environment variables
- Do not remove the `managed = False` from Django models
