import polars as pl
from datetime import datetime

from src.rules.unusual_hours import check


class TestUnusualHours:
    def test_returns_correct_schema(self, sample_transactions):
        result = check(sample_transactions)
        assert result.columns == [
            "application_id",
            "rule_name",
            "triggered",
            "score",
            "details",
        ]

    def test_no_nighttime_transactions(self, sample_transactions):
        result = check(sample_transactions)
        app1_result = result.filter(pl.col("application_id") == 1)
        assert app1_result["triggered"][0] is False

    def test_details_contains_count(self, sample_transactions):
        result = check(sample_transactions)
        for row in result.iter_rows(named=True):
            assert "midnight-5AM" in row["details"]

    def test_handles_empty_transactions(self):
        empty_df = pl.DataFrame(
            {
                "id": pl.Series([], dtype=pl.Int64),
                "applicant_id": pl.Series([], dtype=pl.Int64),
                "amount": pl.Series([], dtype=pl.Float64),
                "merchant": pl.Series([], dtype=pl.String),
                "category": pl.Series([], dtype=pl.String),
                "transaction_time": pl.Series([], dtype=pl.Datetime),
                "location_country": pl.Series([], dtype=pl.String),
                "is_online": pl.Series([], dtype=pl.Int64),
            }
        )
        result = check(empty_df)
        assert len(result) == 0

    def test_detects_nighttime_transactions(self):
        night_txns = pl.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "applicant_id": [1, 1, 1, 1],
                "amount": [100.0, 200.0, 150.0, 300.0],
                "merchant": ["Store A", "Store B", "Store C", "Store D"],
                "category": ["groceries", "electronics", "groceries", "clothing"],
                "transaction_time": [
                    datetime(2026, 3, 21, 1, 0, 0),
                    datetime(2026, 3, 21, 2, 0, 0),
                    datetime(2026, 3, 21, 3, 0, 0),
                    datetime(2026, 3, 21, 4, 0, 0),
                ],
                "location_country": ["US", "US", "US", "US"],
                "is_online": [0, 0, 1, 0],
            }
        )
        result = check(night_txns)
        app1_result = result.filter(pl.col("application_id") == 1)
        assert app1_result["triggered"][0] is True
        assert app1_result["score"][0] == 100.0
