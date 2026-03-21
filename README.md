# Santandors — Fraud Detection Class

A real-world Python engineering class built around a credit application fraud detection system. Students work through progressive issues to transform a messy data science notebook into production-grade software.

## The scenario

Santandors is a Django app where people apply for credit. A fraud pipeline analyzes their transaction history and determines whether applications should be approved or rejected. Right now the fraud logic lives in a messy Jupyter notebook — your job is to productionize it.

## Architecture

- **Django app** — credit application form, status page, admin
- **Neon (PostgreSQL)** — OLTP database with `credit_applications`, `transactions`, `fraud_results`
- **Jupyter notebook** — messy fraud analysis with 5 rules (to be refactored)
- **Transaction simulator** — generates live transaction data at ~1/sec

## Setup

```bash
# clone and enter your branch
git clone https://github.com/haro-ca/fraud-detection-class.git
cd fraud-detection-class
git checkout -b new-applications-approval<YOUR_TEAM_NUMBER>

# install dependencies
uv sync

# configure environment
cp .env.example .env
# edit .env with your Neon connection string and Django secret key

# run the app
uv run python manage.py runserver

# run the transaction simulator (separate terminal)
uv run python scripts/transactions.py
```

## Project structure

```
├── config/                 # Django settings, urls, wsgi
├── creditapp/              # Django app (models, views, templates, admin)
├── notebooks/
│   └── fraud_analysis.ipynb  # Messy notebook — your starting point
├── scripts/
│   └── transactions.py     # Transaction feed simulator
├── manage.py
└── pyproject.toml
```

## The main issue

**feat: credit approvals must show in app after they've completed fraud pipeline**

The status page currently always shows "pending". Your job is to build the full pipeline so applications get approved or rejected based on fraud analysis results.

## Tech stack

- Python 3.12+
- Django
- PostgreSQL (Neon)
- Polars (not pandas)
- uv for dependency management
