from datetime import datetime

import polars as pl

from src.fraud import run_all, score_applications

from .ingest import extract_applications, extract_transactions
from .publish import update_application_status, write_fraud_results


def run_pipeline(
    olap_store,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int | None = None,
) -> dict:
    if start is None:
        start = olap_store.get_last_processed_at()

    applications = extract_applications(start=start, end=end, limit=limit)
    if applications.is_empty():
        return {"processed": 0, "start": start, "end": end}

    app_ids = applications["id"].to_list()
    transactions = extract_transactions(start=start, end=end)
    transactions = transactions.filter(pl.col("applicant_id").is_in(app_ids))

    olap_store.upsert_applications(applications)
    olap_store.upsert_transactions(transactions)

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
