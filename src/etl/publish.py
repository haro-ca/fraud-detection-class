import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]


def _get_conn():
    return psycopg2.connect(DATABASE_URL)


def write_fraud_results(results: list[dict]) -> None:
    if not results:
        return
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            for r in results:
                cur.execute(
                    """
                    INSERT INTO fraud_results (application_id, rule_name, triggered, score, details)
                    VALUES (%(application_id)s, %(rule_name)s, %(triggered)s, %(score)s, %(details)s)
                    """,
                    r,
                )
            conn.commit()
    finally:
        conn.close()


def update_application_status(app_id: int, status: str) -> None:
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE credit_applications SET status = %s WHERE id = %s
                """,
                [status, app_id],
            )
            conn.commit()
    finally:
        conn.close()
