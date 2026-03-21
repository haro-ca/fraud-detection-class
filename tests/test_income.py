import polars as pl

from src.rules.income import check


class TestIncomeRatio:
    def test_returns_correct_schema(self, sample_transactions, sample_applications):
        result = check(sample_transactions, sample_applications)
        assert result.columns == [
            "application_id",
            "rule_name",
            "triggered",
            "score",
            "details",
        ]

    def test_detects_high_spending_ratio(
        self, sample_transactions, sample_applications
    ):
        result = check(sample_transactions, sample_applications)
        app2_result = result.filter(pl.col("application_id") == 2)
        assert app2_result["triggered"][0] is True
        assert app2_result["rule_name"][0] == "income_ratio"

    def test_low_spending_ratio_no_trigger(
        self, sample_transactions, sample_applications
    ):
        result = check(sample_transactions, sample_applications)
        app1_result = result.filter(pl.col("application_id") == 1)
        assert app1_result["triggered"][0] is False

    def test_score_reflects_ratio(self, sample_transactions, sample_applications):
        result = check(sample_transactions, sample_applications)
        app2_result = result.filter(pl.col("application_id") == 2)
        assert app2_result["score"][0] > 50

    def test_details_contains_spending_info(
        self, sample_transactions, sample_applications
    ):
        result = check(sample_transactions, sample_applications)
        app1_result = result.filter(pl.col("application_id") == 1)
        assert "Spent $" in app1_result["details"][0]
        assert "income" in app1_result["details"][0]

    def test_handles_missing_transactions(self, sample_applications):
        empty_txns = pl.DataFrame(
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
        result = check(empty_txns, sample_applications)
        for row in result.iter_rows(named=True):
            assert row["triggered"] is False
