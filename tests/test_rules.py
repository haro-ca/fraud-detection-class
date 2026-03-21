import pytest
import polars as pl
from datetime import datetime, timedelta

from src.fraud import (
    velocity_check,
    income_ratio,
    geo_anomaly,
    high_risk_merchant,
    unusual_hours,
)


def make_txn_time(s: str) -> datetime:
    return datetime.fromisoformat(s)


class TestVelocityCheck:
    def test_triggers_with_four_transactions_in_two_hours(self):
        base = make_txn_time("2024-01-01T10:00:00")
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1, 1, 1],
                "transaction_time": [
                    base,
                    base + timedelta(hours=0, minutes=30),
                    base + timedelta(hours=1, minutes=0),
                    base + timedelta(hours=1, minutes=30),
                ],
            }
        )
        results = velocity_check(transactions)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 1
        assert triggered[0]["application_id"] == 1
        assert "Max 4" in triggered[0]["details"]

    def test_no_trigger_with_spread_out_transactions(self):
        base = make_txn_time("2024-01-01T10:00:00")
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1],
                "transaction_time": [
                    base,
                    base + timedelta(days=1),
                ],
            }
        )
        results = velocity_check(transactions)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 0

    def test_edge_case_exactly_at_threshold(self):
        base = make_txn_time("2024-01-01T10:00:00")
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1, 1, 1],
                "transaction_time": [
                    base,
                    base,
                    base,
                    base,
                ],
            }
        )
        results = velocity_check(transactions)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 1
        assert results[0]["score"] == pytest.approx(66.67, rel=0.1)


class TestIncomeRatio:
    def test_triggers_when_spending_exceeds_half_income(self):
        transactions = pl.DataFrame(
            {
                "applicant_id": [1],
                "amount": [60000.0],
            }
        )
        applications = pl.DataFrame(
            {
                "id": [1],
                "annual_income": [100000.0],
            }
        )
        results = income_ratio(transactions, applications)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 1
        assert triggered[0]["application_id"] == 1
        assert "60.00%" in triggered[0]["details"]

    def test_no_trigger_when_spending_below_half_income(self):
        transactions = pl.DataFrame(
            {
                "applicant_id": [1],
                "amount": [30000.0],
            }
        )
        applications = pl.DataFrame(
            {
                "id": [1],
                "annual_income": [100000.0],
            }
        )
        results = income_ratio(transactions, applications)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 0

    def test_edge_case_no_transactions(self):
        transactions = pl.DataFrame(
            {
                "applicant_id": [1],
                "amount": [0.0],
            }
        )
        applications = pl.DataFrame(
            {
                "id": [1],
                "annual_income": [100000.0],
            }
        )
        results = income_ratio(transactions, applications)
        assert len(results) == 1
        assert results[0]["triggered"] is False
        assert results[0]["score"] == 0.0


class TestGeoAnomaly:
    def test_triggers_with_three_countries_in_48_hours(self):
        base = make_txn_time("2024-01-01T10:00:00")
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1, 1],
                "transaction_time": [
                    base,
                    base + timedelta(hours=24),
                    base + timedelta(hours=48),
                ],
                "location_country": ["US", "MX", "CA"],
            }
        )
        results = geo_anomaly(transactions)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 1
        assert triggered[0]["application_id"] == 1

    def test_no_trigger_with_two_countries_in_48_hours(self):
        base = make_txn_time("2024-01-01T10:00:00")
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1],
                "transaction_time": [
                    base,
                    base + timedelta(hours=24),
                ],
                "location_country": ["US", "MX"],
            }
        )
        results = geo_anomaly(transactions)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 0

    def test_edge_case_exactly_three_countries(self):
        base = make_txn_time("2024-01-01T10:00:00")
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1, 1],
                "transaction_time": [
                    base,
                    base,
                    base,
                ],
                "location_country": ["US", "MX", "CA"],
            }
        )
        results = geo_anomaly(transactions)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 1
        assert results[0]["score"] == pytest.approx(60.0, rel=0.1)


class TestHighRiskMerchant:
    def test_triggers_when_high_risk_exceeds_30_percent(self):
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1, 1, 1],
                "category": ["gambling", "grocery", "food", "cash"],
            }
        )
        results = high_risk_merchant(transactions)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 1
        assert "2/4" in triggered[0]["details"]

    def test_no_trigger_when_high_risk_below_30_percent(self):
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1, 1, 1],
                "category": ["gambling", "grocery", "food", "electronics"],
            }
        )
        results = high_risk_merchant(transactions)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 0

    def test_edge_case_exactly_30_percent(self):
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                "category": [
                    "gambling",
                    "grocery",
                    "grocery",
                    "grocery",
                    "grocery",
                    "grocery",
                    "grocery",
                    "grocery",
                    "grocery",
                    "grocery",
                ],
            }
        )
        results = high_risk_merchant(transactions)
        assert len(results) == 1
        assert results[0]["triggered"] is False
        assert results[0]["score"] == 10.0


class TestUnusualHours:
    def test_triggers_when_majority_between_midnight_and_5am(self):
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1, 1],
                "transaction_time": [
                    make_txn_time("2024-01-01T01:00:00"),
                    make_txn_time("2024-01-01T02:00:00"),
                    make_txn_time("2024-01-01T03:00:00"),
                ],
            }
        )
        results = unusual_hours(transactions)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 1
        assert "3/3" in triggered[0]["details"]

    def test_no_trigger_when_few_transactions_at_night(self):
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1, 1],
                "transaction_time": [
                    make_txn_time("2024-01-01T10:00:00"),
                    make_txn_time("2024-01-01T11:00:00"),
                    make_txn_time("2024-01-01T01:00:00"),
                ],
            }
        )
        results = unusual_hours(transactions)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 0

    def test_edge_case_exactly_50_percent_nighttime(self):
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1, 1, 1],
                "transaction_time": [
                    make_txn_time("2024-01-01T01:00:00"),
                    make_txn_time("2024-01-01T02:00:00"),
                    make_txn_time("2024-01-01T12:00:00"),
                    make_txn_time("2024-01-01T13:00:00"),
                ],
            }
        )
        results = unusual_hours(transactions)
        assert len(results) == 1
        assert results[0]["triggered"] is False
        assert results[0]["score"] == 50.0

    def test_edge_case_all_transactions_at_night(self):
        transactions = pl.DataFrame(
            {
                "applicant_id": [1, 1],
                "transaction_time": [
                    make_txn_time("2024-01-01T04:59:59"),
                    make_txn_time("2024-01-01T00:00:01"),
                ],
            }
        )
        results = unusual_hours(transactions)
        triggered = [r for r in results if r["triggered"]]
        assert len(triggered) == 1
        assert results[0]["score"] == 100.0
