import logging

import polars as pl

from .constants import REJECTION_THRESHOLD
from .geo_anomaly import geo_anomaly
from .high_risk_merchant import high_risk_merchant
from .income_ratio import income_ratio
from .unusual_hours import unusual_hours
from .velocity_check import velocity_check

logger = logging.getLogger(__name__)

_RULES = [
    ("velocity_check", velocity_check),
    ("income_ratio", income_ratio),
    ("geo_anomaly", geo_anomaly),
    ("high_risk_merchant", high_risk_merchant),
    ("unusual_hours", unusual_hours),
]


def _run_rule(name, func, *args):
    rule_results = func(*args)
    triggered = sum(1 for r in rule_results if r["triggered"])
    logger.info(
        "Rule %-20s: %d evaluated, %d triggered, %d clean",
        name,
        len(rule_results),
        triggered,
        len(rule_results) - triggered,
    )
    return rule_results


def run_all(transactions: pl.DataFrame, applications: pl.DataFrame) -> list[dict]:
    logger.info("Running %d fraud rules", len(_RULES))
    results = []
    for name, func in _RULES:
        if name == "income_ratio":
            results.extend(_run_rule(name, func, transactions, applications))
        else:
            results.extend(_run_rule(name, func, transactions))

    total_triggered = sum(1 for r in results if r["triggered"])
    logger.info(
        "All rules complete: %d total results, %d triggered",
        len(results),
        total_triggered,
    )
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
    logger.info(
        "Scored %d applications (threshold=%d rules to reject)",
        len(summary),
        REJECTION_THRESHOLD,
    )
    return summary
