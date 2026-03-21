# Phase 4: Add Logging to the Fraud Pipeline

## Issue Addressed
- #12: Add logging to the fraud pipeline

## What Was Built

Replaced print statements with proper Python logging:

### Logging Configuration

```python
logger = logging.getLogger("fraud_pipeline")

def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
```

### Log Levels

- **INFO** (default) - Pipeline progress, counts, completion
- **DEBUG** (-v/--verbose flag) - Detailed execution info

### Log Messages

```
[15:30:01] [INFO] fraud_pipeline: Starting fraud pipeline
[15:30:01] [INFO] fraud_pipeline: Loading data from database
[15:30:02] [INFO] fraud_pipeline: Loaded 135 transactions and 24 applications
[15:30:02] [INFO] fraud_pipeline: Running fraud detection rules
[15:30:03] [INFO] fraud_pipeline: Fraud rules complete: 92 flags total, 29 triggered
[15:30:03] [INFO] fraud_pipeline: Writing results to database
[15:30:03] [INFO] fraud_pipeline: Pipeline complete
```

## Usage

```bash
# Normal (INFO level)
uv run python scripts/pipeline.py

# Verbose (DEBUG level)
uv run python scripts/pipeline.py -v

# Django command respects verbosity
uv run python manage.py run_fraud_pipeline -v 2
```

## Files Modified
- `scripts/pipeline.py` - Added logging throughout
- `creditapp/management/commands/run_fraud_pipeline.py` - Added -v flag
