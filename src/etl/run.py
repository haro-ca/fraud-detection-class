import logging
from datetime import datetime

import polars as pl

from src.fraud import run_all, score_applications

from .ingest import extract_applications, extract_transactions
from .publish import update_application_status, write_fraud_results

logger = logging.getLogger(__name__)


def run_pipeline(
    olap_store,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int | None = None,
) -> dict:
    if start is None:
        start = olap_store.get_last_processed_at()
        if start is not None:
            logger.info("Resuming from watermark: %s", start)

    logger.info("Pipeline started (start=%s, end=%s, limit=%s)", start, end, limit)

    applications = extract_applications(start=start, end=end, limit=limit)
    if applications.is_empty():
        logger.info("No new applications found — nothing to do")
        return {"processed": 0, "start": start, "end": end}

    app_ids = applications["id"].to_list()
    transactions = extract_transactions(start=start, end=end)
    transactions = transactions.filter(pl.col("applicant_id").is_in(app_ids))
    logger.info(
        "Ingested %d applications, %d transactions",
        len(applications),
        len(transactions),
    )

    olap_store.upsert_applications(applications)
    olap_store.upsert_transactions(transactions)
    logger.info("Staged data to OLAP")

    max_tx_time = None
    if not transactions.is_empty():
        max_tx_time = transactions["transaction_time"].max()
    else:
        max_tx_time = applications["created_at"].max()

    # Run rules only against newly ingested applications
    # but use all their transactions from OLAP for full history
    new_apps = olap_store.query_applications_since(None).filter(
        pl.col("id").is_in(app_ids)
    )
    all_transactions = olap_store.query_transactions_since(None).filter(
        pl.col("applicant_id").is_in(app_ids)
    )

    results = run_all(all_transactions, new_apps)
    results_df = pl.DataFrame(results)

    if not results_df.is_empty():
        olap_store.store_fraud_results(results_df)

    write_fraud_results(results)

    if results:
        decisions = score_applications(results)
        approved = decisions.filter(pl.col("decision") == "approved")
        rejected = decisions.filter(pl.col("decision") == "rejected")
        logger.info(
            "Decisions: %d approved, %d rejected",
            len(approved),
            len(rejected),
        )
        for row in decisions.iter_rows(named=True):
            app_id = row["application_id"]
            decision = row["decision"]
            update_application_status(app_id, decision)

    if max_tx_time is not None:
        olap_store.set_last_processed_at(max_tx_time)

    logger.info(
        "Pipeline complete — processed %d applications, watermark=%s",
        len(applications),
        max_tx_time,
    )

    return {
        "processed": len(applications),
        "watermark": max_tx_time,
    }
