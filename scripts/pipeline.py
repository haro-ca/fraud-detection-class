import logging

import django
import polars as pl
from dotenv import load_dotenv

from src.rules import run_all

load_dotenv()

django.setup()

logger = logging.getLogger("fraud_pipeline")


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def load_transactions() -> pl.DataFrame:
    from creditapp.models import Transaction

    txns = Transaction.objects.all().values(
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


def load_applications() -> pl.DataFrame:
    from creditapp.models import CreditApplication

    apps = CreditApplication.objects.all().values(
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


def write_fraud_results(results: pl.DataFrame) -> None:
    from creditapp.models import FraudResult

    FraudResult.objects.all().delete()
    for row in results.iter_rows(named=True):
        FraudResult.objects.create(
            application_id=row["application_id"],
            rule_name=row["rule_name"],
            triggered=row["triggered"],
            score=row["score"],
            details=row["details"],
        )


def update_application_statuses(results: pl.DataFrame) -> None:
    from creditapp.models import CreditApplication

    summary = results.group_by("application_id").agg(
        pl.col("triggered").sum().alias("rules_triggered")
    )
    for row in summary.iter_rows(named=True):
        app_id = row["application_id"]
        rules_triggered = row["rules_triggered"]
        new_status = "rejected" if rules_triggered >= 2 else "approved"
        CreditApplication.objects.filter(id=app_id).update(status=new_status)


def run_pipeline() -> None:
    logger.info("Starting fraud pipeline")
    logger.info("Loading data from database")
    transactions = load_transactions()
    applications = load_applications()
    logger.info(
        "Loaded %d transactions and %d applications",
        len(transactions),
        len(applications),
    )

    logger.info("Running fraud detection rules")
    results = run_all(transactions, applications)
    triggered_count = results.filter(pl.col("triggered")).height
    logger.info(
        "Fraud rules complete: %d flags total, %d triggered",
        len(results),
        triggered_count,
    )

    logger.info("Writing results to database")
    write_fraud_results(results)
    update_application_statuses(results)
    logger.info("Pipeline complete")


if __name__ == "__main__":
    import sys
    import logging as stdlib_logging

    level = (
        stdlib_logging.DEBUG
        if "-v" in sys.argv or "--verbose" in sys.argv
        else stdlib_logging.INFO
    )
    configure_logging(level)
    run_pipeline()
