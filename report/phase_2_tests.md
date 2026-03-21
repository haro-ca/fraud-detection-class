# Phase 2: Add Tests for Extracted Fraud Rules

## Issue Addressed
- #10: Add tests for extracted fraud rules

## What Was Built

Comprehensive test suite for all 5 fraud rules plus integration tests:

```
tests/
  conftest.py              # Shared fixtures
  test_velocity.py          # 6 tests for velocity rule
  test_income.py           # 5 tests for income rule
  test_geo.py              # 6 tests for geo rule
  test_high_risk.py        # 7 tests for high-risk rule
  test_unusual_hours.py    # 5 tests for unusual-hours rule
  test_run_all.py          # 4 tests for combined pipeline
```

## Test Fixtures (conftest.py)

```python
@pytest.fixture
def sample_transactions():
    # Returns Polars DataFrame with test transaction data
    # 3 applicants with varying patterns

@pytest.fixture
def sample_applications():
    # Returns Polars DataFrame with test application data
    # 3 applicants with different income levels
```

## Test Coverage

Each rule module has tests for:
- **Schema validation** - Returns correct columns
- **Detection logic** - Correctly identifies violations
- **Clean cases** - Handles non-violating data
- **Edge cases** - Empty DataFrames, missing data
- **Score calculation** - Scores reflect severity
- **Details format** - Human-readable details present

## Test Results

```
34 tests passing
- test_velocity.py: 6 passed
- test_income.py: 5 passed
- test_geo.py: 6 passed
- test_high_risk.py: 7 passed
- test_unusual_hours.py: 5 passed
- test_run_all.py: 4 passed
```

## How to Run Tests

```bash
# Run all tests
uv run pytest tests/ -v

# Run specific test file
uv run pytest tests/test_velocity.py -v

# Run with coverage
uv run pytest tests/ --cov=src/rules
```

## Files Created/Modified
- `tests/conftest.py` (new)
- `tests/test_velocity.py` (new)
- `tests/test_income.py` (new)
- `tests/test_geo.py` (new)
- `tests/test_high_risk.py` (new)
- `tests/test_unusual_hours.py` (new)
- `tests/test_run_all.py` (new)
- `pyproject.toml` (updated - added package discovery)
