import logging
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]

logger = logging.getLogger(__name__)


def _get_conn():
    return psycopg2.connect(DATABASE_URL)


def write_fraud_results(results: list[dict]) -> None:
    if not results:
        logger.info("No fraud results to publish")
        return
    logger.info("Publishing %d fraud results to Neon", len(results))
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            for r in results:
                cur.execute(
                    """
                    INSERT INTO fraud_results (application_id, rule_name, triggered, score, details)
                    VALUES (%(application_id)s, %(rule_name)s, %(triggered)s, %(score)s, %(details)s)
                    ON CONFLICT (application_id, rule_name) DO UPDATE SET
                        triggered = EXCLUDED.triggered,
                        score = EXCLUDED.score,
                        details = EXCLUDED.details
                    """,
                    r,
                )
            conn.commit()
        logger.info("Published %d fraud results", len(results))
    except psycopg2.Error:
        logger.exception("Failed to publish fraud results to Neon")
        raise
    finally:
        conn.close()


def update_application_status(app_id: int, status: str) -> None:
    logger.info("Updating application %d status to '%s'", app_id, status)
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
    except psycopg2.Error:
        logger.exception("Failed to update application %d status", app_id)
        raise
    finally:
        conn.close()
