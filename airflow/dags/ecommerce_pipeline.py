"""
dags/ecommerce_pipeline.py

E-Commerce Data Pipeline DAG — Modularized.

Runs hourly (or on-demand) to process any new CSV files that the
Collector service has uploaded to MinIO since the last run.

Modular design:
  1. INGEST (per entity): discover → validate → quarantine → clean → load → mark processed
  2. TRANSFORM (separate task per domain): dimensions → facts → aggregates → quality checks

Idempotency:
  - staging.processed_files prevents re-processing the same files.
  - All staging inserts use ON CONFLICT DO UPDATE (UPSERT).
  - analytics transforms use SCD2 / UPSERT / DELETE-then-INSERT patterns.

CDC:
  - order_status changes are captured on re-upload via staging UPSERT behavior.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from utils.cleaners import clean
from utils.db_helper import file_already_processed, mark_file_processed
from utils.loaders import load_to_staging
from utils.minio_helper import get_client, list_objects, read_csv, send_to_quarantine
from utils.transformers import (
    ensure_analytics_schema,
    transform_dim_customers,
    transform_dim_products,
    transform_dim_inventory,
    transform_fact_orders,
    transform_fact_payments,
    transform_fact_returns,
    transform_agg_revenue,
    cleanup_orphaned_foreign_keys,
    validate_referential_constraints,
    check_default_partition_usage,
    refresh_dashboard_materialized_views,
)
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
# Granular ingest stage functions (separate discover, validate, clean, load)
# ---------------------------------------------------------------------------

def _discover_new_files(entity: str) -> dict:
    """Discover new files in MinIO for this entity that haven't been processed."""
    bucket = os.environ.get("MINIO_RAW_BUCKET", "raw-data")
    client = get_client()

    all_paths = list_objects(client, bucket, prefix=f"{entity}/")
    new_paths = [p for p in all_paths if not file_already_processed(p)]

    logger.info("Entity=%s: discovered %d new file(s) to process", entity, len(new_paths))
    return {
        "entity": entity,
        "file_paths": new_paths,
        "file_count": len(new_paths),
    }


def _validate_and_quarantine(entity: str, file_paths: list) -> dict:
    """
    Validate all files for the entity.
    Separate schema + business rules validation.
    Quarantine invalid rows to DLQ.
    """
    if not file_paths:
        logger.info("Entity=%s: no files to validate", entity)
        return {
            "entity": entity,
            "valid_rows": 0,
            "invalid_rows": 0,
            "quarantined_files": 0,
        }

    bucket = os.environ.get("MINIO_RAW_BUCKET", "raw-data")
    client = get_client()

    total_valid = 0
    total_invalid = 0
    quarantined_file_count = 0

    for obj_path in file_paths:
        file_invalid = 0
        
        for chunk_idx, df in enumerate(read_csv(client, bucket, obj_path)):
            # VALIDATE: schema + business rules
            valid_df, invalid_df = validate(entity, df)

            # QUARANTINE: invalid rows to DLQ
            if not invalid_df.empty:
                send_to_quarantine(client, entity, obj_path, invalid_df, chunk_idx)
                file_invalid += len(invalid_df)
                quarantined_file_count += 1

            total_valid += len(valid_df)
            total_invalid += file_invalid

    logger.info(
        "Entity=%s validation complete: %d valid, %d invalid rows",
        entity, total_valid, total_invalid
    )
    return {
        "entity": entity,
        "valid_rows": total_valid,
        "invalid_rows": total_invalid,
        "quarantined_files": quarantined_file_count,
    }


def _clean_data(entity: str, file_paths: list) -> dict:
    """
    Clean valid rows from files (type coercion, normalization).
    Returns cleaned DataFrames in cache for next stage (load_to_staging).
    """
    if not file_paths:
        logger.info("Entity=%s: no files to clean", entity)
        return {
            "entity": entity,
            "cleaned_rows": 0,
            "failed_files": 0,
        }

    bucket = os.environ.get("MINIO_RAW_BUCKET", "raw-data")
    client = get_client()

    total_cleaned = 0
    failed_file_count = 0

    for obj_path in file_paths:
        try:
            file_cleaned = 0
            
            for chunk_idx, df in enumerate(read_csv(client, bucket, obj_path)):
                # VALIDATE: only process valid rows
                valid_df, invalid_df = validate(entity, df)

                # SKIP if nothing is valid
                if valid_df.empty:
                    continue

                # CLEAN: type coercion, normalization (no DB writes yet)
                clean_df = clean(entity, valid_df)
                file_cleaned += len(clean_df)

            if file_cleaned == 0:
                logger.warning("File %s had no valid rows to clean", obj_path)
            
            total_cleaned += file_cleaned

        except Exception as exc:
            logger.error("Failed cleaning %s: %s", obj_path, exc, exc_info=True)
            failed_file_count += 1

    logger.info(
        "Entity=%s cleaning complete: %d rows cleaned, %d files failed",
        entity, total_cleaned, failed_file_count
    )
    return {
        "entity": entity,
        "cleaned_rows": total_cleaned,
        "failed_files": failed_file_count,
    }


def _load_to_staging_db(entity: str, file_paths: list) -> dict:
    """
    Load cleaned rows from files into staging schema (UPSERT).
    Marks files as processed upon success.
    """
    if not file_paths:
        logger.info("Entity=%s: no files to load", entity)
        return {
            "entity": entity,
            "loaded_rows": 0,
            "failed_files": 0,
        }

    bucket = os.environ.get("MINIO_RAW_BUCKET", "raw-data")
    client = get_client()

    total_loaded = 0
    failed_file_count = 0

    for obj_path in file_paths:
        try:
            file_loaded = 0
            
            for chunk_idx, df in enumerate(read_csv(client, bucket, obj_path)):
                # VALIDATE: only process valid rows
                valid_df, invalid_df = validate(entity, df)

                # SKIP if nothing is valid
                if valid_df.empty:
                    continue

                # CLEAN: type coercion, normalization
                clean_df = clean(entity, valid_df)

                # LOAD: stage UPSERT
                load_to_staging(entity, clean_df, source_file=obj_path)
                file_loaded += len(clean_df)

            if file_loaded == 0:
                logger.warning("File %s had no valid rows to load", obj_path)
            
            total_loaded += file_loaded

            # MARK: file as processed
            mark_file_processed(
                obj_path, entity,
                row_count=file_loaded,
                quarantine_count=0,
                status="success" if file_loaded > 0 else "empty",
            )

        except Exception as exc:
            logger.error("Failed loading %s: %s", obj_path, exc, exc_info=True)
            failed_file_count += 1

    logger.info(
        "Entity=%s loading complete: %d rows loaded, %d files failed",
        entity, total_loaded, failed_file_count
    )
    return {
        "entity": entity,
        "loaded_rows": total_loaded,
        "failed_files": failed_file_count,
    }


# ---------------------------------------------------------------------------
# DAG definition (TaskFlow API with TaskGroups for modularity)
# ---------------------------------------------------------------------------

@dag(
    dag_id="ecommerce_pipeline",
    description="Modularized pipeline: MinIO → staging (validate/clean/load) → analytics star schema.",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["platform", "pipeline"],
)
def ecommerce_pipeline():
    """Main e-commerce data processing DAG — modularized by stage."""

    # =========================================================================
    # INGEST STAGE: separate TaskGroup per entity with granular stages
    #   discover → validate → quarantine → clean → load → mark processed
    # =========================================================================
    
    ingest_tasks = {}
    
    for entity in ENTITIES:
        with TaskGroup(group_id=f"ingest_{entity}") as tg_ingest:
            
            @task(task_id=f"discover_files_{entity}")
            def discover_files() -> dict:
                """Discover new files in MinIO for this entity."""
                return _discover_new_files(entity)

            @task(task_id=f"validate_quarantine_{entity}")
            def validate_quarantine(discover_result: dict) -> dict:
                """Validate all discovered files and quarantine invalid rows."""
                return _validate_and_quarantine(entity, discover_result["file_paths"])

            @task(task_id=f"clean_{entity}")
            def clean_files(discover_result: dict) -> dict:
                """Clean valid rows (type coercion, normalization)."""
                return _clean_data(entity, discover_result["file_paths"])

            @task(task_id=f"load_{entity}")
            def load_files(discover_result: dict) -> dict:
                """Load cleaned rows into staging schema (UPSERT)."""
                return _load_to_staging_db(entity, discover_result["file_paths"])

            # Within-entity dependencies: discover → validate → clean → load
            discover = discover_files()
            validate = validate_quarantine(discover)
            clean = clean_files(discover)
            load = load_files(discover)
            discover >> validate >> clean >> load
            
            ingest_tasks[entity] = load

    # =========================================================================
    # TRANSFORM STAGE: separate modules for dimensions, facts, aggregates
    # =========================================================================

    with TaskGroup(group_id="transform") as tg_transform:
        
        @task(task_id="bootstrap_analytics_schema")
        def bootstrap_schema() -> str:
            """Ensure all analytics tables and partitions exist."""
            ensure_analytics_schema()
            return "schema_ready"

        @task(task_id="transform_dim_customers")
        def xform_customers() -> dict:
            """SCD2 merge: staging customers → analytics dim_customers."""
            n = transform_dim_customers()
            return {"table": "dim_customers", "rows": n}

        @task(task_id="transform_dim_products")
        def xform_products() -> dict:
            """SCD2 merge: staging products → analytics dim_products."""
            n = transform_dim_products()
            return {"table": "dim_products", "rows": n}

        @task(task_id="transform_dim_inventory")
        def xform_inventory() -> dict:
            """Snapshot upsert: staging inventory → analytics dim_inventory."""
            n = transform_dim_inventory()
            return {"table": "dim_inventory", "rows": n}

        @task(task_id="transform_fact_orders")
        def xform_orders() -> dict:
            """CDC-aware upsert: staging orders → analytics fact_orders (partitioned)."""
            n = transform_fact_orders()
            return {"table": "fact_orders", "rows": n}

        @task(task_id="transform_fact_payments")
        def xform_payments() -> dict:
            """Upsert: staging payments → analytics fact_payments."""
            n = transform_fact_payments()
            return {"table": "fact_payments", "rows": n}

        @task(task_id="transform_fact_returns")
        def xform_returns() -> dict:
            """Upsert: staging returns → analytics fact_returns."""
            n = transform_fact_returns()
            return {"table": "fact_returns", "rows": n}

        @task(task_id="transform_agg_revenue")
        def xform_revenue() -> dict:
            """Delete-then-insert: staging revenue → analytics agg_revenue."""
            n = transform_agg_revenue()
            return {"table": "agg_revenue", "rows": n}

        # Dependencies within transform stage
        schema = bootstrap_schema()
        
        cust = xform_customers()
        prod = xform_products()
        inv = xform_inventory()
        
        schema >> [cust, prod, inv]
        
        orders = xform_orders()
        [cust, prod, inv] >> orders
        
        payments = xform_payments()
        revenue = xform_revenue()
        orders >> [payments, revenue]
        
        returns = xform_returns()
        orders >> returns

    # =========================================================================
    # POST-TRANSFORM QUALITY CHECKS
    # =========================================================================

    with TaskGroup(group_id="post_transform_quality") as tg_quality:

        @task(task_id="cleanup_orphaned_fks")
        def cleanup_fks() -> int:
            """Null out orphaned FK references before constraint validation."""
            return cleanup_orphaned_foreign_keys()

        @task(task_id="validate_referential_integrity")
        def validate_fks() -> int:
            """Validate FK constraints (non-partitioned tables only due to Postgres limitation)."""
            return validate_referential_constraints()

        @task(task_id="check_default_partition")
        def check_partition() -> int:
            """Alert if fact_orders default partition has rows (out-of-range data)."""
            return check_default_partition_usage()

        @task(task_id="refresh_dashboard_views")
        def refresh_views() -> int:
            """Refresh materialized views for dashboard."""
            return refresh_dashboard_materialized_views()

        # Post-transform dependencies
        cleanup = cleanup_fks()
        validate = validate_fks()
        partition = check_partition()
        views = refresh_views()
        
        cleanup >> validate >> [partition, views]

    # =========================================================================
    # DAG Execution order
    # =========================================================================
    
    # All ingest entities run in parallel, then transform tier runs, then quality checks.
    all_ingest_list = list(ingest_tasks.values())
    all_ingest_list >> tg_transform >> tg_quality


ecommerce_pipeline()
