import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import django
import duckdb
import polars as pl
from dotenv import load_dotenv

load_dotenv()

django.setup()

logger = logging.getLogger("etl")

DUCKDB_PATH = Path("data/fraud_analytics.duckdb")


def get_last_sync_timestamp() -> Optional[datetime]:
    try:
        conn = duckdb.connect(str(DUCKDB_PATH), read_only=False)
        try:
            result = conn.execute("SELECT MAX(last_sync) FROM sync_metadata").fetchone()
            if result and result[0]:
                return datetime.fromisoformat(result[0])
        except duckdb.CatalogException:
            return None
        finally:
            conn.close()
    except Exception:
        return None
    return None


def update_sync_timestamp() -> None:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=False)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_metadata (
                id INTEGER PRIMARY KEY,
                last_sync TEXT
            )
        """)
        conn.execute(
            "INSERT INTO sync_metadata VALUES (1, ?)", (datetime.now().isoformat(),)
        )
    finally:
        conn.close()


def load_applications_from_neon(days: int = 7) -> pl.DataFrame:
    from creditapp.models import CreditApplication

    cutoff = datetime.now() - timedelta(days=days)
    apps = CreditApplication.objects.filter(created_at__gte=cutoff).values(
        "id",
        "applicant_name",
        "email",
        "ssn_last4",
        "annual_income",
        "requested_amount",
        "employment_status",
        "created_at",
        "status",
    )
    return pl.DataFrame(
        {
            "id": [a["id"] for a in apps],
            "applicant_name": [a["applicant_name"] for a in apps],
            "email": [a["email"] for a in apps],
            "ssn_last4": [a["ssn_last4"] for a in apps],
            "annual_income": [float(a["annual_income"]) for a in apps],
            "requested_amount": [float(a["requested_amount"]) for a in apps],
            "employment_status": [a["employment_status"] for a in apps],
            "created_at": [a["created_at"] for a in apps],
            "status": [a["status"] for a in apps],
        }
    )


def load_transactions_from_neon(days: int = 7) -> pl.DataFrame:
    from creditapp.models import Transaction

    cutoff = datetime.now() - timedelta(days=days)
    txns = Transaction.objects.filter(transaction_time__gte=cutoff).values(
        "id",
        "applicant_id",
        "amount",
        "merchant",
        "category",
        "transaction_time",
        "location_country",
        "is_online",
    )
    return pl.DataFrame(
        {
            "id": [t["id"] for t in txns],
            "applicant_id": [t["applicant_id"] for t in txns],
            "amount": [float(t["amount"]) for t in txns],
            "merchant": [t["merchant"] for t in txns],
            "category": [t["category"] for t in txns],
            "transaction_time": [t["transaction_time"] for t in txns],
            "location_country": [t["location_country"] for t in txns],
            "is_online": [t["is_online"] for t in txns],
        }
    )


def load_fraud_results_from_neon(days: int = 7) -> pl.DataFrame:
    from creditapp.models import FraudResult

    cutoff = datetime.now() - timedelta(days=days)
    results = FraudResult.objects.filter(created_at__gte=cutoff).values(
        "id",
        "application_id",
        "rule_name",
        "triggered",
        "score",
        "details",
        "created_at",
    )
    return pl.DataFrame(
        {
            "id": [r["id"] for r in results],
            "application_id": [r["application_id"] for r in results],
            "rule_name": [r["rule_name"] for r in results],
            "triggered": [r["triggered"] for r in results],
            "score": [float(r["score"]) if r["score"] else None for r in results],
            "details": [r["details"] for r in results],
            "created_at": [r["created_at"] for r in results],
        }
    )


def write_to_duckdb(
    applications: pl.DataFrame, transactions: pl.DataFrame, fraud_results: pl.DataFrame
) -> None:
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DUCKDB_PATH), read_only=False)

    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS analytics")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS analytics.applications AS
            SELECT * FROM applications LIMIT 0
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS analytics.transactions AS
            SELECT * FROM transactions LIMIT 0
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS analytics.fraud_results AS
            SELECT * FROM fraud_results LIMIT 0
        """)

        if len(applications) > 0:
            conn.execute(
                "DELETE FROM analytics.applications WHERE id IN (SELECT id FROM applications)"
            )
            conn.execute(
                "INSERT INTO analytics.applications SELECT * FROM applications"
            )

        if len(transactions) > 0:
            conn.execute(
                "DELETE FROM analytics.transactions WHERE id IN (SELECT id FROM transactions)"
            )
            conn.execute(
                "INSERT INTO analytics.transactions SELECT * FROM transactions"
            )

        if len(fraud_results) > 0:
            conn.execute(
                "DELETE FROM analytics.fraud_results WHERE id IN (SELECT id FROM fraud_results)"
            )
            conn.execute(
                "INSERT INTO analytics.fraud_results SELECT * FROM fraud_results"
            )

    finally:
        conn.close()


def run_etl(days: int = 7) -> None:
    logger.info("Starting ETL pipeline")
    logger.info("Loading data from Neon (last %d days)", days)

    applications = load_applications_from_neon(days)
    transactions = load_transactions_from_neon(days)
    fraud_results = load_fraud_results_from_neon(days)

    logger.info(
        "Loaded %d applications, %d transactions, %d fraud results",
        len(applications),
        len(transactions),
        len(fraud_results),
    )

    logger.info("Writing to DuckDB at %s", DUCKDB_PATH)
    write_to_duckdb(applications, transactions, fraud_results)

    update_sync_timestamp()
    logger.info("ETL pipeline complete")


if __name__ == "__main__":
    import sys

    days = 7
    if len(sys.argv) > 1:
        days = int(sys.argv[1])

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    run_etl(days)
