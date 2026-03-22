import polars as pl

from .constants import REJECTION_THRESHOLD
from .geo_anomaly import geo_anomaly
from .high_risk_merchant import high_risk_merchant
from .income_ratio import income_ratio
from .unusual_hours import unusual_hours
from .velocity_check import velocity_check


def run_all(transactions: pl.DataFrame, applications: pl.DataFrame) -> list[dict]:
    results = []
    results.extend(velocity_check(transactions))
    results.extend(income_ratio(transactions, applications))
    results.extend(geo_anomaly(transactions))
    results.extend(high_risk_merchant(transactions))
    results.extend(unusual_hours(transactions))
    return results


def score_applications(results: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(results)
    summary = df.group_by("application_id").agg(
        pl.col("triggered").sum().alias("rules_triggered"),
        pl.col("score").mean().alias("avg_score"),
        pl.col("score").max().alias("max_score"),
    )
    summary = summary.with_columns(
        pl.when(pl.col("rules_triggered") >= REJECTION_THRESHOLD)
        .then(pl.lit("rejected"))
        .otherwise(pl.lit("approved"))
        .alias("decision")
    )
    return summary
