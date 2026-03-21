import polars as pl

from src.rules import run_all


class TestRunAll:
    def test_returns_combined_results(self, sample_transactions, sample_applications):
        result = run_all(sample_transactions, sample_applications)
        assert len(result) > 0

    def test_all_rules_present(self, sample_transactions, sample_applications):
        result = run_all(sample_transactions, sample_applications)
        rule_names = result["rule_name"].unique().to_list()
        assert "velocity_check" in rule_names
        assert "income_ratio" in rule_names
        assert "geo_anomaly" in rule_names
        assert "high_risk_merchant" in rule_names
        assert "unusual_hours" in rule_names

    def test_correct_schema(self, sample_transactions, sample_applications):
        result = run_all(sample_transactions, sample_applications)
        assert result.columns == [
            "application_id",
            "rule_name",
            "triggered",
            "score",
            "details",
        ]

    def test_triggered_flags_contain_scores(
        self, sample_transactions, sample_applications
    ):
        result = run_all(sample_transactions, sample_applications)
        triggered = result.filter(pl.col("triggered"))
        for row in triggered.iter_rows(named=True):
            assert row["score"] > 0
