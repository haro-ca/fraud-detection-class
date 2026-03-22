from datetime import timedelta

import polars as pl

from .constants import GEO_ANOMALY_THRESHOLD, GEO_ANOMALY_WINDOW_HOURS


def geo_anomaly(transactions: pl.DataFrame) -> list[dict]:
    results = []
    for app_id in transactions["applicant_id"].unique().to_list():
        app_txns = transactions.filter(pl.col("applicant_id") == app_id).sort(
            "transaction_time"
        )
        times = app_txns["transaction_time"].to_list()
        countries = app_txns["location_country"].to_list()

        max_countries_in_window = 0
        for i in range(len(times)):
            window_end = times[i] + timedelta(hours=GEO_ANOMALY_WINDOW_HOURS)
            countries_in_window = set()
            for j in range(len(times)):
                if times[i] <= times[j] <= window_end:
                    countries_in_window.add(countries[j])
            max_countries_in_window = max(
                max_countries_in_window, len(countries_in_window)
            )

        triggered = max_countries_in_window >= GEO_ANOMALY_THRESHOLD
        score = min(max_countries_in_window / 3.0 * 60, 100)
        results.append(
            {
                "application_id": app_id,
                "rule_name": "geo_anomaly",
                "triggered": triggered,
                "score": round(score, 2),
                "details": f"{max_countries_in_window} countries in {GEO_ANOMALY_WINDOW_HOURS}h window",
            }
        )
    return results
