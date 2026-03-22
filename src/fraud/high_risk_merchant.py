import polars as pl

from .constants import HIGH_RISK_CATEGORIES, HIGH_RISK_THRESHOLD


def high_risk_merchant(transactions: pl.DataFrame) -> list[dict]:
    results = []
    for app_id in transactions["applicant_id"].unique().to_list():
        app_txns = transactions.filter(pl.col("applicant_id") == app_id)
        total_count = len(app_txns)
        high_risk_count = len(
            app_txns.filter(pl.col("category").is_in(HIGH_RISK_CATEGORIES))
        )

        ratio = high_risk_count / total_count if total_count > 0 else 0
        triggered = ratio > HIGH_RISK_THRESHOLD
        score = min(ratio * 100, 100)

        results.append(
            {
                "application_id": app_id,
                "rule_name": "high_risk_merchant",
                "triggered": triggered,
                "score": round(score, 2),
                "details": f"{high_risk_count}/{total_count} transactions in high-risk categories ({ratio:.1%})",
            }
        )
    return results
