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
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from tasks.ingest import (
    clean_data,
    discover_new_files,
    load_to_staging_db,
    validate_and_quarantine,
)
from tasks.transform import (
    check_default_partition_usage,
    cleanup_orphaned_foreign_keys,
    ensure_analytics_schema,
    refresh_dashboard_materialized_views,
    transform_agg_revenue,
    transform_dim_customers,
    transform_dim_inventory,
    transform_dim_products,
    transform_fact_orders,
    transform_fact_payments,
    transform_fact_returns,
    validate_referential_constraints,
)

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
            def discover_files(_entity: str = entity) -> dict:
                """Discover new files in MinIO for this entity."""
                return discover_new_files(_entity)

            @task(task_id=f"validate_quarantine_{entity}")
            def validate_quarantine(discover_result: dict, _entity: str = entity) -> dict:
                """Validate all discovered files and quarantine invalid rows."""
                return validate_and_quarantine(_entity, discover_result["file_paths"])

            @task(task_id=f"clean_{entity}")
            def clean_files(discover_result: dict, _entity: str = entity) -> dict:
                """Clean valid rows (type coercion, normalization)."""
                return clean_data(_entity, discover_result["file_paths"])

            @task(task_id=f"load_{entity}")
            def load_files(discover_result: dict, _entity: str = entity) -> dict:
                """Load cleaned rows into staging schema (UPSERT)."""
                return load_to_staging_db(_entity, discover_result["file_paths"])

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
