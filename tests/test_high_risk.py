import polars as pl

from src.rules.high_risk import check, HIGH_RISK_CATEGORIES


class TestHighRiskMerchant:
    def test_returns_correct_schema(self, sample_transactions):
        result = check(sample_transactions)
        assert result.columns == [
            "application_id",
            "rule_name",
            "triggered",
            "score",
            "details",
        ]

    def test_detects_high_risk_transactions(self, sample_transactions):
        result = check(sample_transactions)
        app2_result = result.filter(pl.col("application_id") == 2)
        assert app2_result["triggered"][0] is True
        assert app2_result["rule_name"][0] == "high_risk_merchant"

    def test_no_high_risk_transactions(self, sample_transactions):
        result = check(sample_transactions)
        app1_result = result.filter(pl.col("application_id") == 1)
        assert app1_result["triggered"][0] is False

    def test_score_reflects_risk_ratio(self, sample_transactions):
        result = check(sample_transactions)
        app2_result = result.filter(pl.col("application_id") == 2)
        assert app2_result["score"][0] > 30

    def test_details_contains_ratio(self, sample_transactions):
        result = check(sample_transactions)
        app2_result = result.filter(pl.col("application_id") == 2)
        assert "high-risk" in app2_result["details"][0]

    def test_high_risk_categories_defined(self):
        assert "gambling" in HIGH_RISK_CATEGORIES
        assert "crypto" in HIGH_RISK_CATEGORIES
        assert "cash" in HIGH_RISK_CATEGORIES

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
