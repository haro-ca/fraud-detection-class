import polars as pl
import pytest
from datetime import datetime, timedelta


@pytest.fixture
def sample_transactions():
    now = datetime(2026, 3, 21, 12, 0, 0)
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "applicant_id": [1, 1, 1, 1, 1, 2, 2, 2, 3, 3],
            "amount": [
                100.0,
                200.0,
                150.0,
                300.0,
                250.0,
                50.0,
                75.0,
                100.0,
                500.0,
                600.0,
            ],
            "merchant": [
                "Store A",
                "Store B",
                "Store C",
                "Store D",
                "Store E",
                "Store F",
                "Store G",
                "Store H",
                "Store I",
                "Store J",
            ],
            "category": [
                "groceries",
                "electronics",
                "groceries",
                "clothing",
                "food",
                "gambling",
                "crypto",
                "cash",
                "groceries",
                "clothing",
            ],
            "transaction_time": [
                now,
                now + timedelta(hours=1),
                now + timedelta(hours=2),
                now + timedelta(hours=3),
                now + timedelta(hours=5),
                now,
                now + timedelta(hours=2),
                now + timedelta(hours=4),
                now,
                now + timedelta(hours=1),
            ],
            "location_country": [
                "US",
                "US",
                "US",
                "US",
                "US",
                "US",
                "GB",
                "DE",
                "FR",
                "RU",
            ],
            "is_online": [0, 0, 1, 0, 1, 0, 0, 0, 0, 0],
        }
    )


@pytest.fixture
def sample_applications():
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "applicant_name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@email.com", "bob@email.com", "charlie@email.com"],
            "ssn_last4": ["1234", "5678", "9012"],
            "annual_income": [100000.0, 100.0, 200000.0],
            "requested_amount": [10000.0, 5000.0, 20000.0],
            "employment_status": ["employed", "employed", "self-employed"],
            "created_at": [
                datetime(2026, 3, 1),
                datetime(2026, 3, 15),
                datetime(2026, 3, 20),
            ],
            "status": ["pending", "pending", "pending"],
        }
    )


@pytest.fixture
def all_rules_result_schema():
    return ["application_id", "rule_name", "triggered", "score", "details"]
