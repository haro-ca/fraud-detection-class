import os

import duckdb
import plotly.express as px
import polars as pl
import streamlit as st

from src.fraud.constants import HIGH_RISK_CATEGORIES

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

st.set_page_config(page_title="HOUND Dashboard", layout="wide")

DB_PATH = os.environ.get("HOUND_DB_PATH", "hound.db")


# ---------------------------------------------------------------------------
# Data loading (cached, polars only)
# ---------------------------------------------------------------------------


def _query(db_path: str, sql: str) -> pl.DataFrame:
    try:
        conn = duckdb.connect(db_path, read_only=True)
        rows = conn.execute(sql).fetchall()
        cols = [d[0] for d in conn.description]
        conn.close()
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows, schema=cols, orient="row")
    except duckdb.CatalogException:
        return pl.DataFrame()
    except duckdb.IOException:
        return pl.DataFrame()


@st.cache_data(ttl=60)
def load_applications(db_path: str) -> pl.DataFrame:
    return _query(db_path, "SELECT * FROM applications")


@st.cache_data(ttl=60)
def load_transactions(db_path: str) -> pl.DataFrame:
    return _query(db_path, "SELECT * FROM transactions")


@st.cache_data(ttl=60)
def load_fraud_results(db_path: str) -> pl.DataFrame:
    return _query(db_path, "SELECT * FROM fraud_results")


@st.cache_data(ttl=60)
def load_watermark(db_path: str) -> str | None:
    df = _query(db_path, "SELECT value FROM pipeline_watermark WHERE key = 'last_processed_at'")
    if df.is_empty():
        return None
    return df["value"][0]


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("HOUND")
st.sidebar.caption("fraud detection pipeline")

db_path = st.sidebar.text_input("DuckDB path", value=DB_PATH)

if st.sidebar.button("Refresh"):
    st.cache_data.clear()
    st.rerun()

watermark = load_watermark(db_path)
if watermark:
    st.sidebar.info(f"Last pipeline run:\n{watermark}")
else:
    st.sidebar.warning("No pipeline runs yet")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

apps = load_applications(db_path)
txns = load_transactions(db_path)
fraud = load_fraud_results(db_path)

if apps.is_empty():
    st.warning("No data found. Run the pipeline first:\n\n`uv run python -m src.cli run`")
    st.stop()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------

tab_overview, tab_rules, tab_txns, tab_detail = st.tabs(
    ["Overview", "Fraud Rules", "Transactions", "Application Detail"]
)

# ---------------------------------------------------------------------------
# Tab 1: Overview
# ---------------------------------------------------------------------------

with tab_overview:
    st.header("Overview")

    status_counts = apps.group_by("status").len().sort("status")
    total = len(apps)

    cols = st.columns(3)
    for i, status in enumerate(["approved", "pending", "rejected"]):
        row = status_counts.filter(pl.col("status") == status)
        count = row["len"][0] if not row.is_empty() else 0
        pct = f"{count / total:.0%}" if total > 0 else "0%"
        cols[i].metric(status.capitalize(), count, pct)

    st.subheader("Approval rate over time")

    apps_with_date = apps.with_columns(
        pl.col("created_at").cast(pl.Date).alias("date")
    )
    daily = apps_with_date.group_by("date").agg(
        pl.col("status").len().alias("total"),
        (pl.col("status") == "approved").sum().alias("approved"),
    ).sort("date")
    daily = daily.with_columns(
        (pl.col("approved") / pl.col("total") * 100).alias("approval_rate")
    )

    if not daily.is_empty():
        fig = px.line(
            daily.to_pandas(),
            x="date",
            y="approval_rate",
            labels={"approval_rate": "Approval Rate (%)", "date": "Date"},
        )
        fig.update_layout(yaxis_range=[0, 100])
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Tab 2: Fraud Rules
# ---------------------------------------------------------------------------

with tab_rules:
    st.header("Fraud Rules")

    if fraud.is_empty():
        st.warning("No fraud results yet.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Rules triggered most")
            triggered = fraud.filter(pl.col("triggered")).group_by("rule_name").len().sort("len", descending=True)
            if not triggered.is_empty():
                fig = px.bar(
                    triggered.to_pandas(),
                    x="rule_name",
                    y="len",
                    labels={"len": "Times Triggered", "rule_name": "Rule"},
                    color="rule_name",
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No rules have triggered.")

        with col2:
            st.subheader("Average score by rule")
            avg_scores = fraud.group_by("rule_name").agg(
                pl.col("score").mean().alias("avg_score")
            ).sort("avg_score", descending=True)
            fig = px.bar(
                avg_scores.to_pandas(),
                x="rule_name",
                y="avg_score",
                labels={"avg_score": "Avg Score", "rule_name": "Rule"},
                color="rule_name",
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        st.subheader("Score heatmap (recent applications)")
        n_apps = st.slider("Number of applications", 10, 100, 50)

        recent_ids = (
            apps.sort("created_at", descending=True)
            .head(n_apps)["id"]
            .to_list()
        )
        heatmap_data = fraud.filter(pl.col("application_id").is_in(recent_ids))

        if not heatmap_data.is_empty():
            pivot = heatmap_data.pivot(
                on="rule_name", index="application_id", values="score"
            ).sort("application_id")

            rule_cols = [c for c in pivot.columns if c != "application_id"]
            labels = pivot["application_id"].to_list()
            matrix = pivot.select(rule_cols).to_pandas()

            fig = px.imshow(
                matrix,
                x=rule_cols,
                y=[str(x) for x in labels],
                color_continuous_scale="RdYlGn_r",
                labels={"color": "Score"},
                aspect="auto",
            )
            fig.update_layout(yaxis_title="Application ID", xaxis_title="Rule")
            st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Tab 3: Transactions
# ---------------------------------------------------------------------------

with tab_txns:
    st.header("Transactions")

    if txns.is_empty():
        st.warning("No transactions yet.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Volume over time")
            txns_with_date = txns.with_columns(
                pl.col("transaction_time").cast(pl.Date).alias("date")
            )
            vol = txns_with_date.group_by("date").len().sort("date")
            fig = px.line(
                vol.to_pandas(),
                x="date",
                y="len",
                labels={"len": "Transactions", "date": "Date"},
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Spending by category")
            by_cat = txns.group_by("category").agg(
                pl.col("amount").sum().alias("total_spent")
            ).sort("total_spent", descending=True)
            fig = px.bar(
                by_cat.to_pandas(),
                x="category",
                y="total_spent",
                labels={"total_spent": "Total Spent ($)", "category": "Category"},
                color="category",
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        col3, col4 = st.columns(2)

        with col3:
            st.subheader("High-risk category trend")
            txns_dated = txns.with_columns(
                pl.col("transaction_time").cast(pl.Date).alias("date"),
                pl.col("category").is_in(HIGH_RISK_CATEGORIES).alias("is_high_risk"),
            )
            risk_trend = txns_dated.group_by("date").agg(
                pl.col("is_high_risk").sum().alias("high_risk"),
                pl.col("is_high_risk").len().alias("total"),
            ).sort("date")
            risk_trend = risk_trend.with_columns(
                (pl.col("high_risk") / pl.col("total") * 100).alias("high_risk_pct")
            )
            fig = px.line(
                risk_trend.to_pandas(),
                x="date",
                y="high_risk_pct",
                labels={"high_risk_pct": "High-Risk %", "date": "Date"},
            )
            fig.update_layout(yaxis_range=[0, 100])
            st.plotly_chart(fig, use_container_width=True)

        with col4:
            st.subheader("Geographic distribution")
            by_country = txns.group_by("location_country").len().sort("len", descending=True)
            fig = px.bar(
                by_country.to_pandas(),
                x="len",
                y="location_country",
                orientation="h",
                labels={"len": "Transactions", "location_country": "Country"},
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Tab 4: Application Detail
# ---------------------------------------------------------------------------

with tab_detail:
    st.header("Application Detail")

    options = apps.sort("created_at", descending=True).select(
        pl.format("#{} — {}", "id", "applicant_name").alias("label"),
        "id",
    )
    selected_label = st.selectbox("Select application", options["label"].to_list())

    if selected_label:
        selected_id = options.filter(pl.col("label") == selected_label)["id"][0]
        app_row = apps.filter(pl.col("id") == selected_id)

        name = app_row["applicant_name"][0]
        email = app_row["email"][0]
        income = app_row["annual_income"][0]
        requested = app_row["requested_amount"][0]
        status = app_row["status"][0]
        employment = app_row["employment_status"][0]

        status_color = {"approved": "green", "rejected": "red", "pending": "orange"}.get(status, "gray")
        st.markdown(f"### {name}")
        st.markdown(f"**{email}** · :{status_color}[{status}]")

        m1, m2, m3 = st.columns(3)
        m1.metric("Annual Income", f"${income:,.2f}")
        m2.metric("Requested Amount", f"${requested:,.2f}")
        m3.metric("Employment", employment)

        st.subheader("Fraud Rule Results")
        app_fraud = fraud.filter(pl.col("application_id") == selected_id).select(
            "rule_name", "triggered", "score", "details"
        )
        if not app_fraud.is_empty():
            triggered_count = app_fraud.filter(pl.col("triggered")).height
            st.markdown(f"**{triggered_count}/5 rules triggered** (threshold: 2+)")
            st.dataframe(app_fraud, use_container_width=True, hide_index=True)
        else:
            st.info("No fraud results for this application.")

        st.subheader("Transaction History")
        app_txns = txns.filter(pl.col("applicant_id") == selected_id).sort(
            "transaction_time", descending=True
        ).select(
            "transaction_time", "amount", "merchant", "category", "location_country", "is_online"
        )
        if not app_txns.is_empty():
            st.dataframe(app_txns, use_container_width=True, hide_index=True)
        else:
            st.info("No transactions for this application.")
