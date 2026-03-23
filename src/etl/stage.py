import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone

import duckdb
import polars as pl


class OLAPStore(ABC):
    @abstractmethod
    def upsert_applications(self, df: pl.DataFrame) -> None: ...

    @abstractmethod
    def upsert_transactions(self, df: pl.DataFrame) -> None: ...

    @abstractmethod
    def query_applications_since(self, watermark: datetime | None) -> pl.DataFrame: ...

    @abstractmethod
    def query_transactions_since(self, watermark: datetime | None) -> pl.DataFrame: ...

    @abstractmethod
    def store_fraud_results(self, df: pl.DataFrame) -> None: ...

    @abstractmethod
    def get_last_processed_at(self) -> datetime | None: ...

    @abstractmethod
    def set_last_processed_at(self, timestamp: datetime) -> None: ...


class DuckDBStore(OLAPStore):
    def __init__(self, db_path: str = "duckdb.db"):
        self.db_path = db_path
        self._init_schema()

    def _conn(self):
        return duckdb.connect(self.db_path, read_only=False)

    def _init_schema(self):
        with self._conn() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS applications (
                    id INTEGER PRIMARY KEY,
                    applicant_name VARCHAR,
                    email VARCHAR,
                    ssn_last4 VARCHAR,
                    annual_income DOUBLE,
                    requested_amount DOUBLE,
                    employment_status VARCHAR,
                    status VARCHAR,
                    created_at TIMESTAMP,
                    processed_at TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY,
                    applicant_id INTEGER,
                    amount DOUBLE,
                    merchant VARCHAR,
                    category VARCHAR,
                    transaction_time TIMESTAMP,
                    location_country VARCHAR,
                    is_online BOOLEAN
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fraud_results (
                    id INTEGER PRIMARY KEY,
                    application_id INTEGER,
                    rule_name VARCHAR,
                    triggered BOOLEAN,
                    score DOUBLE,
                    details VARCHAR,
                    created_at TIMESTAMP,
                    UNIQUE (application_id, rule_name)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_watermark (
                    key VARCHAR PRIMARY KEY,
                    value VARCHAR
                )
            """)
            conn.execute("CREATE SEQUENCE IF NOT EXISTS fraud_results_id_seq")

    def upsert_applications(self, df: pl.DataFrame) -> None:
        rows = df.to_dicts()
        if not rows:
            return
        cols = list(rows[0].keys())
        placeholders = ", ".join(f"${i + 1}" for i in range(len(cols)))
        with self._conn() as conn:
            for row in rows:
                conn.execute(
                    f"""
                    INSERT INTO applications ({", ".join(cols)})
                    VALUES ({placeholders})
                    ON CONFLICT (id) DO UPDATE SET
                    {", ".join(f"{c} = EXCLUDED.{c}" for c in cols)}
                    """,
                    [row[c] for c in cols],
                )

    def upsert_transactions(self, df: pl.DataFrame) -> None:
        rows = df.to_dicts()
        if not rows:
            return
        cols = list(rows[0].keys())
        placeholders = ", ".join(f"${i + 1}" for i in range(len(cols)))
        with self._conn() as conn:
            for row in rows:
                conn.execute(
                    f"""
                    INSERT INTO transactions ({", ".join(cols)})
                    VALUES ({placeholders})
                    ON CONFLICT (id) DO UPDATE SET
                    {", ".join(f"{c} = EXCLUDED.{c}" for c in cols)}
                    """,
                    [row[c] for c in cols],
                )

    def query_applications_since(self, watermark: datetime | None) -> pl.DataFrame:
        with self._conn() as conn:
            if watermark is None:
                rows = conn.execute("SELECT * FROM applications").fetchall()
                cols = [d[0] for d in conn.description]
            else:
                rows = conn.execute(
                    "SELECT * FROM applications WHERE created_at > ?",
                    [watermark],
                ).fetchall()
                cols = [d[0] for d in conn.description]
        if not rows:
            return pl.DataFrame(
                {
                    "id": [],
                    "applicant_name": [],
                    "email": [],
                    "ssn_last4": [],
                    "annual_income": [],
                    "requested_amount": [],
                    "employment_status": [],
                    "status": [],
                    "created_at": [],
                    "processed_at": [],
                }
            )
        return pl.DataFrame(rows, schema=cols, orient="row")

    def query_transactions_since(self, watermark: datetime | None) -> pl.DataFrame:
        with self._conn() as conn:
            if watermark is None:
                rows = conn.execute("SELECT * FROM transactions").fetchall()
                cols = [d[0] for d in conn.description]
            else:
                rows = conn.execute(
                    "SELECT * FROM transactions WHERE transaction_time > ?",
                    [watermark],
                ).fetchall()
                cols = [d[0] for d in conn.description]
        if not rows:
            return pl.DataFrame(
                {
                    "id": [],
                    "applicant_id": [],
                    "amount": [],
                    "merchant": [],
                    "category": [],
                    "transaction_time": [],
                    "location_country": [],
                    "is_online": [],
                }
            )
        return pl.DataFrame(rows, schema=cols, orient="row")

    def store_fraud_results(self, df: pl.DataFrame) -> None:
        rows = df.to_dicts()
        if not rows:
            return
        cols = ["application_id", "rule_name", "triggered", "score", "details"]
        now = datetime.now(timezone.utc)
        with self._conn() as conn:
            for row in rows:
                conn.execute(
                    f"""
                    INSERT INTO fraud_results (id, {", ".join(cols)}, created_at)
                    VALUES (nextval('fraud_results_id_seq'), {", ".join(f"${i + 1}" for i in range(len(cols)))}, ?)
                    ON CONFLICT (application_id, rule_name) DO UPDATE SET
                        triggered = EXCLUDED.triggered,
                        score = EXCLUDED.score,
                        details = EXCLUDED.details,
                        created_at = EXCLUDED.created_at
                    """,
                    [row[c] for c in cols] + [now],
                )

    def get_last_processed_at(self) -> datetime | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT value FROM pipeline_watermark WHERE key = 'last_processed_at'"
            ).fetchone()
        if row is None:
            return None
        return datetime.fromisoformat(row[0])

    def set_last_processed_at(self, timestamp: datetime) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_watermark (key, value)
                VALUES ('last_processed_at', ?)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                [timestamp.isoformat()],
            )


class DatabricksStore(OLAPStore):
    def __init__(self, warehouse_id: str | None = None):
        self.warehouse_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID")
        self.catalog = os.environ.get("DATABRICKS_CATALOG", "hive_metastore")
        self.schema = os.environ.get("DATABRICKS_SCHEMA", "fraud_detection")
        self._conn = None

    def _ensure_connection(self):
        if self._conn is None:
            try:
                from databricks import sql

                self._conn = sql.connect(
                    server_hostname=os.environ["DATABRICKS_HOST"],
                    http_path=os.environ["DATABRICKS_HTTP_PATH"],
                    access_token=os.environ["DATABRICKS_TOKEN"],
                )
            except Exception as exc:
                raise RuntimeError(
                    "Databricks connection failed. Set DATABRICKS_HOST, "
                    "DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN environment variables."
                ) from exc

    def upsert_applications(self, df: pl.DataFrame) -> None:
        raise NotImplementedError("Databricks upsert not implemented in stub")

    def upsert_transactions(self, df: pl.DataFrame) -> None:
        raise NotImplementedError("Databricks upsert not implemented in stub")

    def query_applications_since(self, watermark: datetime | None) -> pl.DataFrame:
        raise NotImplementedError("Databricks query not implemented in stub")

    def query_transactions_since(self, watermark: datetime | None) -> pl.DataFrame:
        raise NotImplementedError("Databricks query not implemented in stub")

    def store_fraud_results(self, df: pl.DataFrame) -> None:
        raise NotImplementedError("Databricks store not implemented in stub")

    def get_last_processed_at(self) -> datetime | None:
        raise NotImplementedError("Databricks watermark not implemented in stub")

    def set_last_processed_at(self, timestamp: datetime) -> None:
        raise NotImplementedError("Databricks watermark not implemented in stub")


def create_store(store_type: str = "duckdb", **kwargs) -> OLAPStore:
    if store_type == "duckdb":
        return DuckDBStore(**kwargs)
    elif store_type == "databricks":
        return DatabricksStore(**kwargs)
    else:
        raise ValueError(f"Unknown store type: {store_type}")
