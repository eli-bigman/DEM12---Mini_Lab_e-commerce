"""
tests/test_transformers.py

Focused unit tests for SQL behavior in airflow/dags/utils/transformers.py.
"""

import os
import sys
from contextlib import contextmanager

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))

from utils import transformers


class _FakeCursor:
    def __init__(self):
        self.calls = []
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchone(self):
        return {"cnt": 0}


@contextmanager
def _fake_transaction(cursor):
    yield cursor


class TestEnsureAnalyticsSchema:
    def test_bootstrap_creates_partial_unique_indexes_for_scd2(self, monkeypatch):
        cur = _FakeCursor()
        monkeypatch.setattr(transformers, "transaction", lambda: _fake_transaction(cur))

        transformers.ensure_analytics_schema()

        ddl_sql = cur.calls[0][0]
        assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_cust_current_unique" in ddl_sql
        assert "ON analytics.dim_customers(customer_id)" in ddl_sql
        assert "WHERE is_current = TRUE" in ddl_sql
        assert "CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_prod_current_unique" in ddl_sql
        assert "ON analytics.dim_products(product_id)" in ddl_sql


class TestDimInventoryTransform:
    def test_no_global_zeroing_statement(self, monkeypatch):
        cur = _FakeCursor()
        monkeypatch.setattr(transformers, "transaction", lambda: _fake_transaction(cur))

        transformers.transform_dim_inventory()

        assert len(cur.calls) == 1
        executed_sql = cur.calls[0][0]
        assert "UPDATE analytics.dim_inventory" not in executed_sql
        assert "INSERT INTO analytics.dim_inventory" in executed_sql
