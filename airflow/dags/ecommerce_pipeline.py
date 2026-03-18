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
# Ingest stage functions (compose scanning, validation, cleaning, loading)
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
            file_valid = 0
            file_invalid = 0
            
            for chunk_idx, df in enumerate(read_csv(client, bucket, obj_path)):
                # VALIDATE: schema + business rules
                valid_df, invalid_df = validate(entity, df)

                # QUARANTINE: invalid rows to DLQ
                quarantine_count = 0
                if not invalid_df.empty:
                    send_to_quarantine(client, entity, obj_path, invalid_df, chunk_idx)
                    quarantine_count = len(invalid_df)

                # SKIP if nothing is valid in chunk
                if valid_df.empty:
                    file_invalid += quarantine_count
                    continue

                # CLEAN: type coercion, normalization
                clean_df = clean(entity, valid_df)

                # LOAD: stage UPSERT
                load_to_staging(entity, clean_df, source_file=obj_path)

                file_valid += len(clean_df)
                file_invalid += quarantine_count

            # MARK: file as processed
            if file_valid == 0 and file_invalid == 0:
                logger.warning("File %s was completely empty.", obj_path)
                
            status = "partial" if file_invalid > 0 else "success"
            mark_file_processed(
                obj_path, entity,
                row_count=file_valid,
                quarantine_count=file_invalid,
                status=status,
            )

            total_valid += file_valid
            total_invalid += file_invalid

        except Exception as exc:
            logger.error("Failed processing %s: %s", obj_path, exc, exc_info=True)
            raise

    return {"entity": entity, "valid": total_valid, "invalid": total_invalid, "files": len(new_paths)}


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
    # INGEST STAGE: scan + validate + clean + load (one TaskGroup per entity)
    # =========================================================================
    
    ingest_tasks = {}
    
    for entity in ENTITIES:
        with TaskGroup(group_id=f"ingest_{entity}") as tg_ingest:
            @task(task_id=f"ingest_{entity}")
            def ingest_entity() -> dict:
                return _process_entity(entity)
            
            ingest_tasks[entity] = ingest_entity()

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
