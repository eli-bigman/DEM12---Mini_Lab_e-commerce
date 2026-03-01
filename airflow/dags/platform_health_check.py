"""
dags/platform_health_check.py

Phase 1 placeholder DAG.

Validates that Airflow can reach MinIO and PostgreSQL.
Subsequent phases (2+) will add the full ETL DAGs.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="platform_health_check",
    description="Phase 1: Validates connectivity to MinIO and PostgreSQL.",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["platform", "health", "phase-1"],
)
def platform_health_check():
    """DAG that verifies core service connectivity."""

    @task(task_id="check_minio_connection")
    def check_minio() -> str:
        """Attempt to list buckets in MinIO to verify connectivity."""
        from minio import Minio

        endpoint = os.environ["MINIO_ENDPOINT"]
        access_key = os.environ["MINIO_ROOT_USER"]
        secret_key = os.environ["MINIO_ROOT_PASSWORD"]

        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
        buckets = [b.name for b in client.list_buckets()]
        print(f"MinIO connection OK. Buckets: {buckets}")
        return f"OK: {buckets}"

    @task(task_id="check_postgres_connection")
    def check_postgres() -> str:
        """Attempt to connect and query PostgreSQL to verify connectivity."""
        import psycopg2

        conn = psycopg2.connect(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ.get("POSTGRES_PORT", "5432")),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        )
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
        conn.close()
        print(f"PostgreSQL connection OK. Version: {version}")
        return f"OK: {version}"

    check_minio() >> check_postgres()


platform_health_check()
