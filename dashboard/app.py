import streamlit as st
from pathlib import Path

import duckdb
import polars as pl

st.set_page_config(page_title="Fraud Analytics Dashboard", layout="wide")

DUCKDB_PATH = Path("data/fraud_analytics.duckdb")


def get_connection():
    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


def load_applications():
    conn = get_connection()
    try:
        result = conn.execute("SELECT * FROM analytics.applications").fetchdf()
        return pl.DataFrame(result)
    finally:
        conn.close()


def load_transactions():
    conn = get_connection()
    try:
        result = conn.execute("SELECT * FROM analytics.transactions").fetchdf()
        return pl.DataFrame(result)
    finally:
        conn.close()


def load_fraud_results():
    conn = get_connection()
    try:
        result = conn.execute("SELECT * FROM analytics.fraud_results").fetchdf()
        return pl.DataFrame(result)
    finally:
        conn.close()


st.title("Fraud Analytics Dashboard")

if not DUCKDB_PATH.exists():
    st.warning(
        "No analytics data found. Run the ETL pipeline first: `uv run python scripts/etl.py`"
    )
    st.stop()

applications = load_applications()
transactions = load_transactions()
fraud_results = load_fraud_results()

col1, col2, col3, col4 = st.columns(4)

total_apps = len(applications)
approved = len(applications.filter(pl.col("status") == "approved"))
rejected = len(applications.filter(pl.col("status") == "rejected"))
flagged = len(fraud_results.filter(pl.col("triggered")))

col1.metric("Total Applications", total_apps)
col2.metric("Approved", approved)
col3.metric("Rejected", rejected)
col4.metric("Fraud Flags", flagged, delta_color="inverse")

st.divider()

tab1, tab2, tab3 = st.tabs(["Overview", "Fraud Rules", "Transactions"])

with tab1:
    st.subheader("Application Status Distribution")
    status_counts = applications.group_by("status").len()
    st.bar_chart(status_counts.to_pandas().set_index("status"))

    st.subheader("Rules Triggered by Application")
    rules_per_app = (
        fraud_results.filter(pl.col("triggered"))
        .group_by("application_id")
        .len()
        .rename({"len": "rules_triggered"})
    )
    if len(rules_per_app) > 0:
        st.dataframe(
            rules_per_app.sort("rules_triggered", descending=True).to_pandas(),
            use_container_width=True,
        )
    else:
        st.info("No fraud flags found")

with tab2:
    st.subheader("Fraud Rules Summary")

    col_a, col_b = st.columns(2)

    rule_counts = fraud_results.group_by("rule_name").agg(
        pl.col("triggered").sum().alias("triggered"), pl.len().alias("total")
    )

    col_a.dataframe(rule_counts.to_pandas(), use_container_width=True)

    rule_avg_score = (
        fraud_results.group_by("rule_name")
        .agg(pl.col("score").mean().round(2).alias("avg_score"))
        .sort("avg_score", descending=True)
    )

    col_b.dataframe(rule_avg_score.to_pandas(), use_container_width=True)

    st.subheader("Rule Performance")
    triggered_by_rule = (
        fraud_results.filter(pl.col("triggered")).group_by("rule_name").len()
    )
    if len(triggered_by_rule) > 0:
        st.bar_chart(triggered_by_rule.to_pandas().set_index("rule_name"))

with tab3:
    st.subheader("Recent Transactions")
    if len(transactions) > 0:
        recent_txns = transactions.sort("transaction_time", descending=True).head(50)
        st.dataframe(
            recent_txns.select(
                [
                    "id",
                    "applicant_id",
                    "amount",
                    "merchant",
                    "category",
                    "location_country",
                    "transaction_time",
                ]
            ).to_pandas(),
            use_container_width=True,
        )

        st.subheader("Transaction Categories")
        cat_counts = (
            transactions.group_by("category").len().sort("len", descending=True)
        )
        st.bar_chart(cat_counts.to_pandas().set_index("category"))
    else:
        st.info("No transaction data found")


st.divider()
st.caption("Data source: DuckDB OLAP layer | Last updated via ETL pipeline")
