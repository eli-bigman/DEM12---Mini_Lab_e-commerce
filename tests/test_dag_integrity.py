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
    def test_ecommerce_pipeline_has_seven_entity_tasks(self):
        """The pipeline DAG must declare one ingest task per entity."""
        import ecommerce_pipeline
        from airflow.models import DagBag

        dag_bag = DagBag(dag_folder=DAGS_PATH, include_examples=False)
        dag = dag_bag.get_dag("ecommerce_pipeline")

        assert dag is not None, "ecommerce_pipeline DAG not found in DagBag"

        task_ids = {t.task_id for t in dag.tasks}
        expected_tasks = {
            "ingest_customers", "ingest_products", "ingest_orders",
            "ingest_payments", "ingest_inventory", "ingest_revenue",
            "ingest_returns", "transform_to_analytics",
        }
        assert expected_tasks.issubset(task_ids), (
            f"Missing tasks: {expected_tasks - task_ids}"
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
