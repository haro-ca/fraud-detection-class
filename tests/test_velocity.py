import polars as pl

from src.rules.velocity import check


class TestVelocityCheck:
    def test_returns_correct_schema(self, sample_transactions):
        result = check(sample_transactions)
        assert result.columns == [
            "application_id",
            "rule_name",
            "triggered",
            "score",
            "details",
        ]

    def test_detects_velocity_violation(self, sample_transactions):
        result = check(sample_transactions)
        app1_result = result.filter(pl.col("application_id") == 1)
        assert app1_result["triggered"][0] is True
        assert app1_result["rule_name"][0] == "velocity_check"

    def test_no_violation_few_transactions(self, sample_transactions):
        result = check(sample_transactions)
        app3_result = result.filter(pl.col("application_id") == 3)
        assert app3_result["triggered"][0] is False

    def test_score_reflects_velocity(self, sample_transactions):
        result = check(sample_transactions)
        app1_result = result.filter(pl.col("application_id") == 1)
        assert app1_result["score"][0] > 0

    def test_details_contains_count(self, sample_transactions):
        result = check(sample_transactions)
        app1_result = result.filter(pl.col("application_id") == 1)
        assert "4h window" in app1_result["details"][0]

    def test_empty_transactions_returns_empty(self):
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
