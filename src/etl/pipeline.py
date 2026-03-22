from datetime import datetime

import polars as pl

from src.fraud import run_all, score_applications

from .extract import (
    extract_applications_since,
    extract_transactions_since,
    update_application_status,
    write_fraud_results_to_neon,
)


def run_pipeline(olap_store, watermark: datetime | None = None) -> dict:
    applications = extract_applications_since(watermark)
    if applications.is_empty():
        return {"processed": 0, "watermark": watermark}

    app_ids = applications["id"].to_list()
    transactions = extract_transactions_since(watermark)
    transactions = transactions.filter(pl.col("applicant_id").is_in(app_ids))

    olap_store.upsert_applications(applications)
    olap_store.upsert_transactions(transactions)

    max_tx_time = None
    if not transactions.is_empty():
        max_tx_time = transactions["transaction_time"].max()
    else:
        max_tx_time = applications["created_at"].max()

    all_applications = olap_store.query_applications_since(None)
    all_transactions = olap_store.query_transactions_since(None)

    results = run_all(all_transactions, all_applications)
    results_df = pl.DataFrame(results)

    if not results_df.is_empty():
        olap_store.store_fraud_results(results_df)

    write_fraud_results_to_neon(results)

    if results:
        decisions = score_applications(results)
        for row in decisions.iter_rows(named=True):
            app_id = row["application_id"]
            decision = row["decision"]
            update_application_status(app_id, decision)

    if max_tx_time is not None:
        olap_store.set_last_processed_at(max_tx_time)

    return {
        "processed": len(applications),
        "watermark": max_tx_time,
    }
