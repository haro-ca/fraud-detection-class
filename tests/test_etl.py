import os
import shutil
import tempfile
from datetime import datetime

import polars as pl
import pytest

from src.etl.stage import DuckDBStore, DatabricksStore, create_store


def make_ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


def make_db_path() -> str:
    tmpdir = tempfile.mkdtemp()
    return os.path.join(tmpdir, "test.duckdb")


def fresh_store(db_path: str) -> DuckDBStore:
    if os.path.exists(db_path):
        os.unlink(db_path)
    return DuckDBStore(db_path)


def cleanup_db_path(db_path: str) -> None:
    parent = os.path.dirname(db_path)
    shutil.rmtree(parent)


class TestDuckDBStore:
    def test_upsert_and_query_applications(self):
        db_path = make_db_path()
        try:
            store = fresh_store(db_path)
            df = pl.DataFrame(
                {
                    "id": [1, 2],
                    "applicant_name": ["Alice", "Bob"],
                    "email": ["alice@x.com", "bob@x.com"],
                    "ssn_last4": ["1234", "5678"],
                    "annual_income": [50000.0, 80000.0],
                    "requested_amount": [10000.0, 15000.0],
                    "employment_status": ["employed", "employed"],
                    "status": ["pending", "pending"],
                    "created_at": [
                        make_ts("2024-01-01T10:00:00"),
                        make_ts("2024-01-01T11:00:00"),
                    ],
                }
            )
            store.upsert_applications(df)
            result = store.query_applications_since(None)
            assert len(result) == 2
            assert result["id"].to_list() == [1, 2]
        finally:
            cleanup_db_path(db_path)

    def test_upsert_and_query_transactions(self):
        db_path = make_db_path()
        try:
            store = fresh_store(db_path)
            df = pl.DataFrame(
                {
                    "id": [10, 20],
                    "applicant_id": [1, 1],
                    "amount": [100.0, 200.0],
                    "merchant": ["Amazon", "Walmart"],
                    "category": ["electronics", "groceries"],
                    "transaction_time": [
                        make_ts("2024-01-01T10:00:00"),
                        make_ts("2024-01-01T11:00:00"),
                    ],
                    "location_country": ["US", "US"],
                    "is_online": [True, False],
                }
            )
            store.upsert_transactions(df)
            result = store.query_transactions_since(None)
            assert len(result) == 2
            assert result["amount"].sum() == 300.0
        finally:
            cleanup_db_path(db_path)

    def test_upsert_is_idempotent(self):
        db_path = make_db_path()
        try:
            store = fresh_store(db_path)
            df1 = pl.DataFrame(
                {
                    "id": [1],
                    "applicant_name": ["Alice"],
                    "email": ["alice@x.com"],
                    "ssn_last4": ["1234"],
                    "annual_income": [50000.0],
                    "requested_amount": [10000.0],
                    "employment_status": ["employed"],
                    "status": ["pending"],
                    "created_at": [make_ts("2024-01-01T10:00:00")],
                }
            )
            df2 = pl.DataFrame(
                {
                    "id": [1],
                    "applicant_name": ["Alice Updated"],
                    "email": ["alice@x.com"],
                    "ssn_last4": ["1234"],
                    "annual_income": [60000.0],
                    "requested_amount": [12000.0],
                    "employment_status": ["employed"],
                    "status": ["approved"],
                    "created_at": [make_ts("2024-01-01T10:00:00")],
                }
            )
            store.upsert_applications(df1)
            store.upsert_applications(df2)
            result = store.query_applications_since(None)
            assert len(result) == 1
            assert result["annual_income"][0] == 60000.0
            assert result["status"][0] == "approved"
        finally:
            cleanup_db_path(db_path)

    def test_query_applications_since_watermark(self):
        db_path = make_db_path()
        try:
            store = fresh_store(db_path)
            df = pl.DataFrame(
                {
                    "id": [1, 2],
                    "applicant_name": ["Alice", "Bob"],
                    "email": ["alice@x.com", "bob@x.com"],
                    "ssn_last4": ["1234", "5678"],
                    "annual_income": [50000.0, 80000.0],
                    "requested_amount": [10000.0, 15000.0],
                    "employment_status": ["employed", "employed"],
                    "status": ["pending", "pending"],
                    "created_at": [
                        make_ts("2024-01-01T10:00:00"),
                        make_ts("2024-01-01T11:00:00"),
                    ],
                }
            )
            store.upsert_applications(df)
            result = store.query_applications_since(make_ts("2024-01-01T10:30:00"))
            assert len(result) == 1
            assert result["id"][0] == 2
        finally:
            cleanup_db_path(db_path)

    def test_watermark_round_trip(self):
        db_path = make_db_path()
        try:
            store = fresh_store(db_path)
            ts = make_ts("2024-01-01T12:00:00")
            store.set_last_processed_at(ts)
            retrieved = store.get_last_processed_at()
            assert retrieved == ts
        finally:
            cleanup_db_path(db_path)

    def test_watermark_none_when_not_set(self):
        db_path = make_db_path()
        try:
            store = fresh_store(db_path)
            assert store.get_last_processed_at() is None
        finally:
            cleanup_db_path(db_path)

    def test_store_fraud_results(self):
        db_path = make_db_path()
        try:
            store = fresh_store(db_path)
            df = pl.DataFrame(
                {
                    "application_id": [1, 1, 2],
                    "rule_name": ["velocity_check", "income_ratio", "geo_anomaly"],
                    "triggered": [True, False, True],
                    "score": [80.0, 20.0, 60.0],
                    "details": [
                        "Max 5 in 4h window",
                        "Spent $1 on income $100K",
                        "3 countries in 48h",
                    ],
                    "created_at": [make_ts("2024-01-01T10:00:00")] * 3,
                }
            )
            store.store_fraud_results(df)
            result = store.query_applications_since(None)
            assert len(result) == 0
        finally:
            cleanup_db_path(db_path)


class TestCreateStore:
    def test_create_duckdb_store(self):
        db_path = make_db_path()
        try:
            store = create_store("duckdb", db_path=db_path)
            assert isinstance(store, DuckDBStore)
        finally:
            cleanup_db_path(db_path)

    def test_create_databricks_store_returns_instance(self):
        store = create_store("databricks")
        assert isinstance(store, DatabricksStore)

    def test_unknown_store_type_raises(self):
        with pytest.raises(ValueError, match="Unknown store type"):
            create_store("snowflake")


