import polars as pl
from polars import DataFrame

from src.rules import geo, high_risk, income, unusual_hours, velocity

__all__ = [
    "geo",
    "high_risk",
    "income",
    "unusual_hours",
    "velocity",
    "run_all",
]

RESULTS_SCHEMA = {
    "application_id": pl.Int64,
    "rule_name": pl.String,
    "triggered": pl.Boolean,
    "score": pl.Float64,
    "details": pl.String,
}


def run_all(transactions: DataFrame, applications: DataFrame) -> DataFrame:
    """
    Run all fraud rules and return combined results.
    """
    results = pl.concat(
        [
            velocity.check(transactions),
            income.check(transactions, applications),
            geo.check(transactions),
            high_risk.check(transactions),
            unusual_hours.check(transactions),
        ]
    )
    return results
