from datetime import timedelta

import polars as pl

from .constants import (
    VELOCITY_THRESHOLD,
    VELOCITY_WINDOW_HOURS,
    INCOME_RATIO_THRESHOLD,
    GEO_ANOMALY_THRESHOLD,
    GEO_ANOMALY_WINDOW_HOURS,
    HIGH_RISK_CATEGORIES,
    HIGH_RISK_THRESHOLD,
    UNUSUAL_HOURS_START,
    UNUSUAL_HOURS_END,
    UNUSUAL_HOURS_THRESHOLD,
)


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


def run_all(transactions: pl.DataFrame, applications: pl.DataFrame) -> list[dict]:
    results = []
    results.extend(velocity_check(transactions))
    results.extend(income_ratio(transactions, applications))
    results.extend(geo_anomaly(transactions))
    results.extend(high_risk_merchant(transactions))
    results.extend(unusual_hours(transactions))
    return results


def to_dataframe(results: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(results)
