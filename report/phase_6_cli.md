# Phase 6: CLI for Pipeline Operations

## Issue Addressed
- #5: CLI for pipeline operations via Rich/Typer

## What Was Built

Rich-powered command-line interface with Typer:

```bash
uv run python scripts/cli.py --help
```

```
Usage: cli.py [OPTIONS] COMMAND [ARGS]...
  Fraud detection CLI

Commands:
  run     Run the fraud detection pipeline
  status  Show fraud detection status summary
  etl     Run the ETL pipeline to load data from Neon to DuckDB
```

## Commands

### `run` - Run Pipeline

```bash
uv run python scripts/cli.py run
uv run python scripts/cli.py run --verbose
```

Features:
- Rich-colored output
- Progress spinner during execution
- Summary table after completion

```
┌──────────────────────┬───────┐
│ Metric              │ Value │
├──────────────────────┼───────┤
│ Total Flags         │ 92    │
│ Triggered           │ 29    │
│ Clean               │ 63    │
│ Applications        │ 24    │
└──────────────────────┴───────┘
```

### `status` - Quick Overview

```bash
uv run python scripts/cli.py status
```

Shows:
- Total applications
- Approved/Rejected/Pending counts
- Total fraud flags

### `etl` - Run ETL Pipeline

```bash
uv run python scripts/cli.py etl --days 30
```

Runs the ETL pipeline with configurable lookback period.

## Dependencies Added
- `rich` - Terminal styling and tables
- `typer` - CLI framework

## Files Created
- `scripts/cli.py` (new)
