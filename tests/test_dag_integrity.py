"""
tests/test_dag_integrity.py

Verifies that all Airflow DAG files can be imported without errors
and that the DAG structure meets basic requirements.

These tests run without a live Airflow environment.
"""

import sys
import os
import importlib
import pytest

# Point Python at the dags directory so the DAG files are importable
DAGS_PATH = os.path.join(os.path.dirname(__file__), "..", "airflow", "dags")
sys.path.insert(0, DAGS_PATH)


class TestDagImports:
    def test_platform_health_check_imports(self):
        """platform_health_check.py must import without raising."""
        import platform_health_check
        assert platform_health_check is not None

    def test_ecommerce_pipeline_imports(self):
        """ecommerce_pipeline.py must import without raising."""
        import ecommerce_pipeline
        assert ecommerce_pipeline is not None


class TestUtilsImports:
    def test_validators_imports(self):
        from utils import validators
        assert hasattr(validators, "validate")

    def test_cleaners_imports(self):
        from utils import cleaners
        assert hasattr(cleaners, "clean")

    def test_loaders_imports(self):
        from utils import loaders
        assert hasattr(loaders, "load_to_staging")

    def test_transformers_imports(self):
        from utils import transformers
        assert hasattr(transformers, "run_all_transforms")

    def test_minio_helper_imports(self):
        from utils import minio_helper
        assert hasattr(minio_helper, "get_client")

    def test_db_helper_imports(self):
        from utils import db_helper
        assert hasattr(db_helper, "get_connection")


class TestDagStructure:
    def test_ecommerce_pipeline_has_modular_structure(self):
        """The pipeline DAG must have modular task groups for ingest (discover/validate/clean/load), transform, and quality checks."""
        import ecommerce_pipeline
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=DAGS_PATH, include_examples=False)
        dag = dag_bag.get_dag("ecommerce_pipeline")

        assert dag is not None, "ecommerce_pipeline DAG not found in DagBag"

        task_ids = {t.task_id for t in dag.tasks}
        task_ids_str = " | ".join(sorted(task_ids))
        
        # Check for granular ingest stages (discover, validate, clean, load per entity)
        # Total: 7 entities × 4 stages = 28 ingest tasks
        ingest_entities = [
            "customers", "products", "orders",
            "payments", "inventory", "revenue", "returns"
        ]
        for entity in ingest_entities:
            discover = f"discover_files_{entity}"
            validate = f"validate_quarantine_{entity}"
            clean = f"clean_{entity}"
            load = f"load_{entity}"
            
            # Tasks may be in TaskGroups, check for exact or prefixed names
            has_discover = discover in task_ids or any(t.endswith(f".{discover}") for t in task_ids)
            has_validate = validate in task_ids or any(t.endswith(f".{validate}") for t in task_ids)
            has_clean = clean in task_ids or any(t.endswith(f".{clean}") for t in task_ids)
            has_load = load in task_ids or any(t.endswith(f".{load}") for t in task_ids)
            
            assert has_discover, f"Missing {discover} in DAG tasks: {task_ids_str}"
            assert has_validate, f"Missing {validate} in DAG tasks: {task_ids_str}"
            assert has_clean, f"Missing {clean} in DAG tasks: {task_ids_str}"
            assert has_load, f"Missing {load} in DAG tasks: {task_ids_str}"
        
        # Check for transform tasks
        transform_tasks = {
            "bootstrap_analytics_schema",
            "transform_dim_customers",
            "transform_dim_products",
            "transform_dim_inventory",
            "transform_fact_orders",
            "transform_fact_payments",
            "transform_fact_returns",
            "transform_agg_revenue",
        }
        for task in transform_tasks:
            assert task in task_ids or any(t.endswith(f".{task}") for t in task_ids), (
                f"Missing transform task {task} in: {task_ids_str}"
            )
        
        # Check for quality tasks
        quality_tasks = {
            "cleanup_orphaned_fks",
            "validate_referential_integrity",
            "check_default_partition",
            "refresh_dashboard_views",
        }
        for task in quality_tasks:
            assert task in task_ids or any(t.endswith(f".{task}") for t in task_ids), (
                f"Missing quality task {task} in: {task_ids_str}"
            )

    def test_ecommerce_pipeline_schedule(self):
        """Pipeline must be scheduled hourly."""
        from airflow.models import DagBag
        dag_bag = DagBag(dag_folder=DAGS_PATH, include_examples=False)
        dag = dag_bag.get_dag("ecommerce_pipeline")
        assert dag is not None
        assert dag.schedule_interval in ("@hourly", "0 * * * *")

    def test_no_dag_import_errors(self):
        """DagBag must report zero import errors."""
        from airflow.models import DagBag
        dag_bag = DagBag(dag_folder=DAGS_PATH, include_examples=False)
        assert dag_bag.import_errors == {}, (
            f"DAG import errors: {dag_bag.import_errors}"
        )
