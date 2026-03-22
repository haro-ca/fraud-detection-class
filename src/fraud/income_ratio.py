import polars as pl

from .constants import INCOME_RATIO_THRESHOLD


def income_ratio(transactions: pl.DataFrame, applications: pl.DataFrame) -> list[dict]:
    results = []
    for app_id in applications["id"].to_list():
        income = applications.filter(pl.col("id") == app_id)["annual_income"][0]
        app_txns = transactions.filter(pl.col("applicant_id") == app_id)
        total_spent = app_txns["amount"].sum()

        if total_spent is None:
            total_spent = 0

        ratio = float(total_spent) / float(income) if income > 0 else 999
        triggered = ratio > INCOME_RATIO_THRESHOLD
        score = min(ratio * 100, 100)

        results.append(
            {
                "application_id": app_id,
                "rule_name": "income_ratio",
                "triggered": triggered,
                "score": round(score, 2),
                "details": f"Spent ${total_spent:,.2f} on income ${float(income):,.2f} (ratio: {ratio:.2%})",
            }
        )
    return results
