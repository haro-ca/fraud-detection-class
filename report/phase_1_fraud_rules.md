# Phase 1: Extract Fraud Rules from Notebook

## Issues Addressed
- #2: Extract fraud rules from notebook into `src/` modules
- #9: Extract each fraud rule into a callable function

## What Was Built

Extracted all 5 fraud detection rules from `notebooks/fraud_analysis.ipynb` into modular Python modules:

```
src/
  rules/
    __init__.py      # Exports run_all() and all rule modules
    velocity.py      # Rule 1: >3 transactions in 4 hours
    income.py        # Rule 2: Spending >50% of annual income
    geo.py           # Rule 3: 3+ countries in 48 hours
    high_risk.py     # Rule 4: >30% in gambling/crypto/cash
    unusual_hours.py # Rule 5: >50% between midnight-5AM
```

## Each Rule Module

- Exports a `check()` function that takes DataFrames and returns a Polars DataFrame
- Returns columns: `application_id`, `rule_name`, `triggered`, `score`, `details`
- Consistent interface across all rules
- Follows the same logic as the original notebook

## Key Design Decisions

1. **One module per rule** - Maximum modularity and testability
2. **Polars DataFrames** - No pandas, as per project conventions
3. **Unified result schema** - All rules return the same columns for consistent processing
4. **Type hints** - Full type annotations for IDE support

## How to Use

```python
from src.rules import run_all

# Run all rules
results = run_all(transactions_df, applications_df)

# Or run individual rules
from src.rules.velocity import check
velocity_results = check(transactions_df)
```

## Files Created
- `src/rules/__init__.py`
- `src/rules/velocity.py`
- `src/rules/income.py`
- `src/rules/geo.py`
- `src/rules/high_risk.py`
- `src/rules/unusual_hours.py`
- `src/__init__.py`
