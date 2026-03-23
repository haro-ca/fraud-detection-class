import os
from datetime import datetime

import psycopg2
import polars as pl
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]


def _get_conn():
    return psycopg2.connect(DATABASE_URL)


def extract_applications(
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int | None = None,
) -> pl.DataFrame:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT id, applicant_name, email, ssn_last4,
                       annual_income, requested_amount, employment_status,
                       status, created_at
                FROM credit_applications
            """
            conditions: list[str] = []
            params: list = []
            if start is not None:
                conditions.append("created_at > %s")
                params.append(start)
            if end is not None:
                conditions.append("created_at <= %s")
                params.append(end)
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY created_at ASC"
            if limit is not None:
                query += " LIMIT %s"
                params.append(limit)
            cur.execute(query, params or None)
            rows = cur.fetchall()
            cols = [
                "id",
                "applicant_name",
                "email",
                "ssn_last4",
                "annual_income",
                "requested_amount",
                "employment_status",
                "status",
                "created_at",
            ]
            return pl.DataFrame(rows, schema=cols, orient="row")
    finally:
        conn.close()


def extract_transactions(
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int | None = None,
) -> pl.DataFrame:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT id, applicant_id, amount, merchant, category,
                       transaction_time, location_country, is_online
                FROM transactions
            """
            conditions: list[str] = []
            params: list = []
            if start is not None:
                conditions.append("transaction_time > %s")
                params.append(start)
            if end is not None:
                conditions.append("transaction_time <= %s")
                params.append(end)
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY transaction_time ASC"
            if limit is not None:
                query += " LIMIT %s"
                params.append(limit)
            cur.execute(query, params or None)
            rows = cur.fetchall()
            cols = [
                "id",
                "applicant_id",
                "amount",
                "merchant",
                "category",
                "transaction_time",
                "location_country",
                "is_online",
            ]
            return pl.DataFrame(rows, schema=cols, orient="row")
    finally:
        conn.close()
