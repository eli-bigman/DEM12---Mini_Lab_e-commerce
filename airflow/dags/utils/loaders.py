"""
utils/loaders.py

Staging layer UPSERT functions.
Each function accepts a cleaned pandas DataFrame and bulk-upserts it
into the corresponding staging.* table using INSERT ... ON CONFLICT.

Idempotency guarantee: re-running with the same data has no effect
because each entity's natural key is the PRIMARY KEY with ON CONFLICT DO UPDATE.
"""

from __future__ import annotations

import logging

import pandas as pd

from .db_helper import execute_values

logger = logging.getLogger(__name__)


def _safe(value):
    """Convert pandas NA/NaT to Python None; return value otherwise."""
    if pd.isna(value):
        return None
    return value


# ---------------------------------------------------------------------------
# Per-entity loaders
# ---------------------------------------------------------------------------

def load_customers(df: pd.DataFrame, source_file: str) -> int:
    sql = """
        INSERT INTO staging.customers
            (customer_id, signup_date, country, acquisition_channel,
             customer_segment, _source_file)
        VALUES %s
        ON CONFLICT (customer_id) DO UPDATE SET
            signup_date         = EXCLUDED.signup_date,
            country             = EXCLUDED.country,
            acquisition_channel = EXCLUDED.acquisition_channel,
            customer_segment    = EXCLUDED.customer_segment,
            _loaded_at          = NOW(),
            _source_file        = EXCLUDED._source_file
    """
    rows = [
        (
            row.customer_id, _safe(row.signup_date),
            _safe(row.country), _safe(row.acquisition_channel),
            _safe(row.customer_segment), source_file,
        )
        for row in df.itertuples(index=False)
    ]
    n = execute_values(sql, rows)
    logger.info("Loaded %d customer rows into staging", len(rows))
    return n


def load_products(df: pd.DataFrame, source_file: str) -> int:
    sql = """
        INSERT INTO staging.products
            (product_id, product_name, category, cost_price,
             selling_price, inventory_quantity, _source_file)
        VALUES %s
        ON CONFLICT (product_id) DO UPDATE SET
            product_name        = EXCLUDED.product_name,
            category            = EXCLUDED.category,
            cost_price          = EXCLUDED.cost_price,
            selling_price       = EXCLUDED.selling_price,
            inventory_quantity  = EXCLUDED.inventory_quantity,
            _loaded_at          = NOW(),
            _source_file        = EXCLUDED._source_file
    """
    rows = [
        (
            row.product_id, _safe(row.product_name), _safe(row.category),
            _safe(row.cost_price), _safe(row.selling_price),
            _safe(row.inventory_quantity), source_file,
        )
        for row in df.itertuples(index=False)
    ]
    n = execute_values(sql, rows)
    logger.info("Loaded %d product rows into staging", len(rows))
    return n


def load_orders(df: pd.DataFrame, source_file: str) -> int:
    sql = """
        INSERT INTO staging.orders
            (order_id, customer_id, product_id, quantity, unit_price,
             total_amount, order_timestamp, payment_method, order_status, _source_file)
        VALUES %s
        ON CONFLICT (order_id) DO UPDATE SET
            order_status    = EXCLUDED.order_status,
            _loaded_at      = NOW(),
            _source_file    = EXCLUDED._source_file
    """
    rows = [
        (
            row.order_id, _safe(row.customer_id), _safe(row.product_id),
            _safe(row.quantity), _safe(row.unit_price), _safe(row.total_amount),
            _safe(row.order_timestamp), _safe(row.payment_method),
            _safe(row.order_status), source_file,
        )
        for row in df.itertuples(index=False)
    ]
    n = execute_values(sql, rows)
    logger.info("Loaded %d order rows into staging", len(rows))
    return n


def load_payments(df: pd.DataFrame, source_file: str) -> int:
    sql = """
        INSERT INTO staging.payments
            (payment_id, order_id, payment_date, payment_method,
             amount, payment_status, _source_file)
        VALUES %s
        ON CONFLICT (payment_id) DO UPDATE SET
            payment_status  = EXCLUDED.payment_status,
            _loaded_at      = NOW(),
            _source_file    = EXCLUDED._source_file
    """
    rows = [
        (
            row.payment_id, _safe(row.order_id), _safe(row.payment_date),
            _safe(row.payment_method), _safe(row.amount),
            _safe(row.payment_status), source_file,
        )
        for row in df.itertuples(index=False)
    ]
    n = execute_values(sql, rows)
    logger.info("Loaded %d payment rows into staging", len(rows))
    return n


def load_inventory(df: pd.DataFrame, source_file: str) -> int:
    sql = """
        INSERT INTO staging.inventory
            (inventory_id, product_id, warehouse_location,
             quantity_on_hand, last_restock_date, _source_file)
        VALUES %s
        ON CONFLICT (inventory_id) DO UPDATE SET
            quantity_on_hand    = EXCLUDED.quantity_on_hand,
            last_restock_date   = EXCLUDED.last_restock_date,
            _loaded_at          = NOW(),
            _source_file        = EXCLUDED._source_file
    """
    rows = [
        (
            row.inventory_id, _safe(row.product_id), _safe(row.warehouse_location),
            _safe(row.quantity_on_hand), _safe(row.last_restock_date), source_file,
        )
        for row in df.itertuples(index=False)
    ]
    n = execute_values(sql, rows)
    logger.info("Loaded %d inventory rows into staging", len(rows))
    return n


def load_revenue(df: pd.DataFrame, source_file: str) -> int:
    sql = """
        INSERT INTO staging.revenue
            (revenue_id, record_date, total_revenue, total_profit, _source_file)
        VALUES %s
        ON CONFLICT (revenue_id) DO UPDATE SET
            total_revenue   = EXCLUDED.total_revenue,
            total_profit    = EXCLUDED.total_profit,
            _loaded_at      = NOW(),
            _source_file    = EXCLUDED._source_file
    """
    rows = [
        (
            row.revenue_id, _safe(row.record_date),
            _safe(row.total_revenue), _safe(row.total_profit), source_file,
        )
        for row in df.itertuples(index=False)
    ]
    n = execute_values(sql, rows)
    logger.info("Loaded %d revenue rows into staging", len(rows))
    return n


def load_returns(df: pd.DataFrame, source_file: str) -> int:
    sql = """
        INSERT INTO staging.returns
            (return_id, order_id, return_date, return_reason,
             refund_amount, return_status, _source_file)
        VALUES %s
        ON CONFLICT (return_id) DO UPDATE SET
            return_status   = EXCLUDED.return_status,
            _loaded_at      = NOW(),
            _source_file    = EXCLUDED._source_file
    """
    rows = [
        (
            row.return_id, _safe(row.order_id), _safe(row.return_date),
            _safe(row.return_reason), _safe(row.refund_amount),
            _safe(row.return_status), source_file,
        )
        for row in df.itertuples(index=False)
    ]
    n = execute_values(sql, rows)
    logger.info("Loaded %d return rows into staging", len(rows))
    return n


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

LOADERS = {
    "customers": load_customers,
    "products":  load_products,
    "orders":    load_orders,
    "payments":  load_payments,
    "inventory": load_inventory,
    "revenue":   load_revenue,
    "returns":   load_returns,
}


def load_to_staging(entity: str, df: pd.DataFrame, source_file: str) -> int:
    """Route a cleaned DataFrame to the correct staging UPSERT loader."""
    fn = LOADERS.get(entity)
    if fn is None:
        raise ValueError(f"No loader defined for entity: {entity!r}")
    return fn(df, source_file)
