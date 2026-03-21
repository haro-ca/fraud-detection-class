# Phase 3: Create Entry Point and Wire Notebook to Modules

## Issue Addressed
- #11: Create entry point and wire notebook to extracted modules

## What Was Built

Two entry points for running the fraud detection pipeline:

### 1. Standalone Script (`scripts/pipeline.py`)

```bash
uv run python scripts/pipeline.py
```

Functions:
- `load_transactions()` - Load from Neon DB to Polars DataFrame
- `load_applications()` - Load from Neon DB to Polars DataFrame
- `write_fraud_results()` - Write results to `fraud_results` table
- `update_application_statuses()` - Set approved/rejected based on flags
- `run_pipeline()` - Orchestrates the full pipeline

### 2. Django Management Command

```bash
uv run python manage.py run_fraud_pipeline
```

Located at: `creditapp/management/commands/run_fraud_pipeline.py`

## Pipeline Workflow

1. Load transactions and applications from Neon PostgreSQL
2. Run all 5 fraud rules via `run_all()`
3. Write results to `fraud_results` table
4. Update application statuses:
   - 2+ rules triggered → "rejected"
   - Otherwise → "approved"

## Key Design Decisions

1. **Separation of concerns** - Pipeline logic in scripts/, Django integration separate
2. **Type hints** - Full type annotations throughout
3. **Django setup in function** - Avoids import errors at module level
4. **Data conversion** - Django ORM → Polars DataFrames for rule processing

## Files Created/Modified
- `scripts/pipeline.py` (new)
- `creditapp/management/commands/run_fraud_pipeline.py` (new)
- `src/__init__.py` (new)
