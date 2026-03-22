from datetime import timedelta

import polars as pl

from .constants import VELOCITY_THRESHOLD, VELOCITY_WINDOW_HOURS


def velocity_check(transactions: pl.DataFrame) -> list[dict]:
    results = []
    for app_id in transactions["applicant_id"].unique().to_list():
        app_txns = transactions.filter(pl.col("applicant_id") == app_id).sort(
            "transaction_time"
        )
        times = app_txns["transaction_time"].to_list()

        max_in_window = 0
        for i in range(len(times)):
            window_end = times[i] + timedelta(hours=VELOCITY_WINDOW_HOURS)
            count_in_window = sum(1 for t in times if times[i] <= t <= window_end)
            max_in_window = max(max_in_window, count_in_window)

        triggered = max_in_window > VELOCITY_THRESHOLD
        score = min(max_in_window / 3.0 * 50, 100)
        results.append(
            {
                "application_id": app_id,
                "rule_name": "velocity_check",
                "triggered": triggered,
                "score": round(score, 2),
                "details": f"Max {max_in_window} transactions in {VELOCITY_WINDOW_HOURS}h window",
            }
        )
    return results
