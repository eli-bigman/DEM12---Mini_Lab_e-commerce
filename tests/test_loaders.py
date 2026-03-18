"""
tests/test_loaders.py

Unit tests for airflow/dags/utils/loaders.py

These tests assert the SQL UPSERT clauses include full mutable-field updates
for orders, payments, and returns.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))

from utils import loaders


def _capture_execute_values(monkeypatch):
    captured = {}

    def fake_execute_values(sql, rows):
        captured["sql"] = sql
        captured["rows"] = rows
        return len(rows)

    monkeypatch.setattr(loaders, "execute_values", fake_execute_values)
    return captured


class TestOrderLoaderUpsert:
    def test_upsert_updates_all_mutable_fields(self, monkeypatch, valid_orders_df):
        captured = _capture_execute_values(monkeypatch)
        loaded = loaders.load_orders(valid_orders_df, "orders.csv")

        assert loaded == len(valid_orders_df)
        sql = captured["sql"]
        assert "customer_id     = EXCLUDED.customer_id" in sql
        assert "product_id      = EXCLUDED.product_id" in sql
        assert "quantity        = EXCLUDED.quantity" in sql
        assert "unit_price      = EXCLUDED.unit_price" in sql
        assert "total_amount    = EXCLUDED.total_amount" in sql
        assert "order_timestamp = EXCLUDED.order_timestamp" in sql
        assert "payment_method  = EXCLUDED.payment_method" in sql
        assert "order_status    = EXCLUDED.order_status" in sql


class TestPaymentLoaderUpsert:
    def test_upsert_updates_all_mutable_fields(self, monkeypatch, valid_payments_df):
        captured = _capture_execute_values(monkeypatch)
        loaded = loaders.load_payments(valid_payments_df, "payments.csv")

        assert loaded == len(valid_payments_df)
        sql = captured["sql"]
        assert "order_id        = EXCLUDED.order_id" in sql
        assert "payment_date    = EXCLUDED.payment_date" in sql
        assert "payment_method  = EXCLUDED.payment_method" in sql
        assert "amount          = EXCLUDED.amount" in sql
        assert "payment_status  = EXCLUDED.payment_status" in sql


class TestReturnLoaderUpsert:
    def test_upsert_updates_all_mutable_fields(self, monkeypatch, valid_returns_df):
        captured = _capture_execute_values(monkeypatch)
        loaded = loaders.load_returns(valid_returns_df, "returns.csv")

        assert loaded == len(valid_returns_df)
        sql = captured["sql"]
        assert "order_id        = EXCLUDED.order_id" in sql
        assert "return_date     = EXCLUDED.return_date" in sql
        assert "return_reason   = EXCLUDED.return_reason" in sql
        assert "refund_amount   = EXCLUDED.refund_amount" in sql
        assert "return_status   = EXCLUDED.return_status" in sql
