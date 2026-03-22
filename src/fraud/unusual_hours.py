import polars as pl

from .constants import UNUSUAL_HOURS_END, UNUSUAL_HOURS_START, UNUSUAL_HOURS_THRESHOLD


def unusual_hours(transactions: pl.DataFrame) -> list[dict]:
    results = []
    for app_id in transactions["applicant_id"].unique().to_list():
        app_txns = transactions.filter(pl.col("applicant_id") == app_id)
        total = len(app_txns)

        hours = app_txns.with_columns(
            pl.col("transaction_time").dt.hour().alias("hour")
        )
        night_txns = len(
            hours.filter(
                (pl.col("hour") >= UNUSUAL_HOURS_START)
                & (pl.col("hour") < UNUSUAL_HOURS_END)
            )
        )

        ratio = night_txns / total if total > 0 else 0
        triggered = ratio > UNUSUAL_HOURS_THRESHOLD
        score = min(ratio * 100, 100)

        results.append(
            {
                "application_id": app_id,
                "rule_name": "unusual_hours",
                "triggered": triggered,
                "score": round(score, 2),
                "details": f"{night_txns}/{total} transactions between midnight-5AM ({ratio:.1%})",
            }
        )
    return results
