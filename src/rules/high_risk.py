import polars as pl

HIGH_RISK_CATEGORIES = ["gambling", "crypto", "cash"]


def check(transactions: pl.DataFrame) -> pl.DataFrame:
    """
    Rule 4: High-risk merchant categories
    More than 30% of transactions in gambling/crypto/cash = flag.
    """
    results = []

    for app_id in transactions["applicant_id"].unique().to_list():
        app_txns = transactions.filter(pl.col("applicant_id") == app_id)
        total_count = len(app_txns)
        high_risk_count = len(
            app_txns.filter(pl.col("category").is_in(HIGH_RISK_CATEGORIES))
        )

        ratio = high_risk_count / total_count if total_count > 0 else 0
        triggered = ratio > 0.3
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

    return pl.DataFrame(results)
