"""
utils/db_helper.py

PostgreSQL connection helpers and low-level execute utilities.
Uses psycopg2 and reads credentials from environment variables.
"""

import logging
import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def get_connection() -> psycopg2.extensions.connection:
    """Open and return a psycopg2 connection using env vars."""
    conn = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
    conn.autocommit = False
    return conn


@contextmanager
def transaction() -> Generator[psycopg2.extensions.cursor, None, None]:
    """
    Context manager that yields a cursor inside an explicit transaction.
    Commits on success, rolls back on any exception.
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_values(sql: str, rows: list[tuple], page_size: int = 1000) -> int:
    """
    Bulk-insert rows using psycopg2.extras.execute_values.

    Parameters
    ----------
    sql      : SQL with %s placeholder for the VALUES block.
    rows     : List of tuples matching the VALUES columns.
    page_size: Number of rows per round-trip.

    Returns
    -------
    Total rows inserted/upserted.
    """
    if not rows:
        return 0
    with transaction() as cur:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=page_size)
        return cur.rowcount


def file_already_processed(object_path: str) -> bool:
    """
    Check the staging.processed_files table to see if a MinIO
    object has already been loaded. Implements idempotency gate.
    """
    with transaction() as cur:
        cur.execute(
            "SELECT 1 FROM staging.processed_files WHERE object_path = %s",
            (object_path,),
        )
        return cur.fetchone() is not None


def mark_file_processed(
    object_path: str,
    entity: str,
    row_count: int,
    quarantine_count: int = 0,
    status: str = "success",
) -> None:
    """Insert a record into staging.processed_files after successful load."""
    with transaction() as cur:
        cur.execute(
            """
            INSERT INTO staging.processed_files
                (object_path, entity, row_count, quarantine_count, status)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (object_path) DO UPDATE
                SET processed_at = NOW(),
                    row_count = EXCLUDED.row_count,
                    quarantine_count = EXCLUDED.quarantine_count,
                    status = EXCLUDED.status
            """,
            (object_path, entity, row_count, quarantine_count, status),
        )
    logger.info("Marked %s as processed (entity=%s, rows=%d)", object_path, entity, row_count)
