"""
dags/ecommerce_pipeline.py

E-Commerce Data Pipeline DAG.

Runs hourly (or on-demand) to process any new CSV files that the
Collector service has uploaded to MinIO since the last run.

Flow per entity:
  1. Scan MinIO for files not yet recorded in staging.processed_files.
  2. Read each file into a pandas DataFrame.
  3. Validate with validators.validate() -> split into (valid, invalid).
  4. Route invalid rows to the MinIO quarantine bucket (DLQ).
  5. Clean valid rows with cleaners.clean().
  6. UPSERT into the staging schema via loaders.load_to_staging().
  7. Mark the file as processed in staging.processed_files.
  8. After all entities are staged, transform into the analytics star schema.

Idempotency:
  - staging.processed_files prevents re-processing the same files.
  - All staging inserts use ON CONFLICT DO UPDATE (UPSERT).
  - analytics inserts use SCD2 / UPSERT / DELETE-then-INSERT patterns.

CDC:
  - order_status changes (Pending -> Refunded/Cancelled) are captured on
    re-upload: the orders UPSERT updates order_status whenever a file with
    the same order_id is loaded again.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from utils.cleaners import clean
from utils.db_helper import file_already_processed, mark_file_processed
from utils.loaders import load_to_staging
from utils.minio_helper import get_client, list_objects, read_csv, send_to_quarantine
from utils.transformers import run_all_transforms
from utils.validators import validate

logger = logging.getLogger(__name__)

ENTITIES = ["customers", "products", "orders", "payments", "inventory", "revenue", "returns"]

DEFAULT_ARGS = {
    "owner": "platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


# ---------------------------------------------------------------------------
# Task implementations
# ---------------------------------------------------------------------------

def _process_entity(entity: str) -> dict:
    """
    Full ingest pipeline for a single entity:
    scan → validate → quarantine invalids → clean → load staging → mark processed.

    Returns a summary dict with counts for XCom.
    """
    bucket = os.environ.get("MINIO_RAW_BUCKET", "raw-data")
    client = get_client()

    all_paths = list_objects(client, bucket, prefix=f"{entity}/")
    new_paths = [p for p in all_paths if not file_already_processed(p)]

    logger.info("Entity=%s: %d new file(s) to process", entity, len(new_paths))

    total_valid = 0
    total_invalid = 0

    for obj_path in new_paths:
        try:
            df = read_csv(client, bucket, obj_path)

            # -- Validate --
            valid_df, invalid_df = validate(entity, df)

            # -- DLQ: quarantine invalid rows --
            quarantine_count = 0
            if not invalid_df.empty:
                send_to_quarantine(client, entity, obj_path, invalid_df)
                quarantine_count = len(invalid_df)

            # -- Skip entirely if nothing is valid --
            if valid_df.empty:
                logger.warning("No valid rows in %s, skipping load.", obj_path)
                mark_file_processed(
                    obj_path, entity,
                    row_count=0,
                    quarantine_count=quarantine_count,
                    status="partial" if quarantine_count > 0 else "success",
                )
                total_invalid += quarantine_count
                continue

            # -- Clean --
            clean_df = clean(entity, valid_df)

            # -- Load to staging --
            load_to_staging(entity, clean_df, source_file=obj_path)

            # -- Mark as processed --
            status = "partial" if quarantine_count > 0 else "success"
            mark_file_processed(
                obj_path, entity,
                row_count=len(clean_df),
                quarantine_count=quarantine_count,
                status=status,
            )

            total_valid += len(clean_df)
            total_invalid += quarantine_count

        except Exception as exc:
            logger.error("Failed processing %s: %s", obj_path, exc, exc_info=True)
            raise

    return {"entity": entity, "valid": total_valid, "invalid": total_invalid, "files": len(new_paths)}


# ---------------------------------------------------------------------------
# DAG definition (TaskFlow API)
# ---------------------------------------------------------------------------

@dag(
    dag_id="ecommerce_pipeline",
    description="Hourly pipeline: MinIO -> staging -> analytics star schema.",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["platform", "pipeline"],
)
def ecommerce_pipeline():
    """Main e-commerce data processing DAG."""

    @task(task_id="ingest_customers")
    def ingest_customers() -> dict:
        return _process_entity("customers")

    @task(task_id="ingest_products")
    def ingest_products() -> dict:
        return _process_entity("products")

    @task(task_id="ingest_orders")
    def ingest_orders() -> dict:
        return _process_entity("orders")

    @task(task_id="ingest_payments")
    def ingest_payments() -> dict:
        return _process_entity("payments")

    @task(task_id="ingest_inventory")
    def ingest_inventory() -> dict:
        return _process_entity("inventory")

    @task(task_id="ingest_revenue")
    def ingest_revenue() -> dict:
        return _process_entity("revenue")

    @task(task_id="ingest_returns")
    def ingest_returns() -> dict:
        return _process_entity("returns")

    @task(task_id="transform_to_analytics")
    def transform_to_analytics(**kwargs) -> dict:
        """
        Run all staging -> analytics transforms after all entities are staged.
        Runs SCD2 merges and fact table upserts.
        """
        results = run_all_transforms()
        logger.info("Analytics transform complete: %s", results)
        return results

    # -- Define execution order --
    # Dimensions must be loaded before facts that reference them.
    # All ingestion tasks run in parallel, then analytics transform runs last.
    customers_done  = ingest_customers()
    products_done   = ingest_products()
    orders_done     = ingest_orders()
    payments_done   = ingest_payments()
    inventory_done  = ingest_inventory()
    revenue_done    = ingest_revenue()
    returns_done    = ingest_returns()

    [
        customers_done,
        products_done,
        orders_done,
        payments_done,
        inventory_done,
        revenue_done,
        returns_done,
    ] >> transform_to_analytics()


ecommerce_pipeline()
