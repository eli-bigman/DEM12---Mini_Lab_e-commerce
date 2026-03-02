"""
utils/transformers.py

Transforms data from the staging schema into the analytics star schema.

Design principles:
  - Idempotent: all writes use INSERT ... ON CONFLICT or DELETE-then-INSERT.
  - SCD Type 2: dim_customers and dim_products expire the current row and
    insert a new row whenever a tracked attribute changes.
  - CDC-aware: fact_orders handles late-arriving order_status updates by
    upserting the current state.
  - dim_inventory uses a simple UPSERT (snapshot, no history).
"""

from __future__ import annotations

import logging

from .db_helper import transaction

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# dim_customers  (SCD Type 2)
# ---------------------------------------------------------------------------

def transform_dim_customers() -> int:
    """
    Merge staging.customers into analytics.dim_customers.

    SCD Type 2 logic:
      1. For any customer whose tracked attributes (segment, acquisition_channel,
         country) have changed, expire the current row (set valid_to, is_current=false).
      2. Insert a new current row for changed or new customers.
    """
    sql = """
        WITH staged AS (
            SELECT DISTINCT ON (customer_id)
                customer_id, signup_date, country, acquisition_channel, customer_segment
            FROM staging.customers
            ORDER BY customer_id, _loaded_at DESC
        ),
        expired AS (
            UPDATE analytics.dim_customers dc
            SET
                valid_to   = NOW(),
                is_current = FALSE
            FROM staged s
            WHERE dc.customer_id = s.customer_id
              AND dc.is_current = TRUE
              AND (
                  dc.country             IS DISTINCT FROM s.country OR
                  dc.acquisition_channel IS DISTINCT FROM s.acquisition_channel OR
                  dc.customer_segment    IS DISTINCT FROM s.customer_segment
              )
            RETURNING dc.customer_id
        )
        INSERT INTO analytics.dim_customers
            (customer_id, signup_date, country, acquisition_channel, customer_segment,
             valid_from, valid_to, is_current)
        SELECT
            s.customer_id, s.signup_date::DATE, s.country,
            s.acquisition_channel, s.customer_segment,
            NOW(), NULL, TRUE
        FROM staged s
        WHERE
            -- New customers
            NOT EXISTS (
                SELECT 1 FROM analytics.dim_customers dc
                WHERE dc.customer_id = s.customer_id
            )
            OR
            -- Customers that were just expired
            s.customer_id IN (SELECT customer_id FROM expired)
        ON CONFLICT DO NOTHING
    """
    with transaction() as cur:
        cur.execute(sql)
        n = cur.rowcount
    logger.info("dim_customers upserted %d rows", n)
    return n


# ---------------------------------------------------------------------------
# dim_products  (SCD Type 2)
# ---------------------------------------------------------------------------

def transform_dim_products() -> int:
    """
    Merge staging.products into analytics.dim_products using SCD Type 2.
    Tracked attributes: cost_price, selling_price, category.
    """
    sql = """
        WITH staged AS (
            SELECT DISTINCT ON (product_id)
                product_id, product_name, category, cost_price, selling_price
            FROM staging.products
            ORDER BY product_id, _loaded_at DESC
        ),
        expired AS (
            UPDATE analytics.dim_products dp
            SET
                valid_to   = NOW(),
                is_current = FALSE
            FROM staged s
            WHERE dp.product_id = s.product_id
              AND dp.is_current = TRUE
              AND (
                  dp.cost_price    IS DISTINCT FROM s.cost_price OR
                  dp.selling_price IS DISTINCT FROM s.selling_price OR
                  dp.category      IS DISTINCT FROM s.category
              )
            RETURNING dp.product_id
        )
        INSERT INTO analytics.dim_products
            (product_id, product_name, category, cost_price, selling_price,
             valid_from, valid_to, is_current)
        SELECT
            s.product_id, s.product_name, s.category,
            s.cost_price::NUMERIC, s.selling_price::NUMERIC,
            NOW(), NULL, TRUE
        FROM staged s
        WHERE
            NOT EXISTS (
                SELECT 1 FROM analytics.dim_products dp
                WHERE dp.product_id = s.product_id
            )
            OR
            s.product_id IN (SELECT product_id FROM expired)
        ON CONFLICT DO NOTHING
    """
    with transaction() as cur:
        cur.execute(sql)
        n = cur.rowcount
    logger.info("dim_products upserted %d rows", n)
    return n


# ---------------------------------------------------------------------------
# dim_inventory  (snapshot UPSERT)
# ---------------------------------------------------------------------------

def transform_dim_inventory() -> int:
    """Upsert staging.inventory into analytics.dim_inventory (current snapshot)."""
    sql = """
        INSERT INTO analytics.dim_inventory
            (inventory_id, product_id, warehouse_location,
             quantity_on_hand, last_restock_date, updated_at)
        SELECT
            i.inventory_id::UUID,
            i.product_id::UUID,
            i.warehouse_location,
            i.quantity_on_hand::INTEGER,
            i.last_restock_date::DATE,
            NOW()
        FROM staging.inventory i
        ON CONFLICT (inventory_id) DO UPDATE SET
            quantity_on_hand  = EXCLUDED.quantity_on_hand,
            last_restock_date = EXCLUDED.last_restock_date,
            updated_at        = NOW()
    """
    with transaction() as cur:
        cur.execute(sql)
        n = cur.rowcount
    logger.info("dim_inventory upserted %d rows", n)
    return n


# ---------------------------------------------------------------------------
# fact_orders  (CDC-aware UPSERT)
# ---------------------------------------------------------------------------

def transform_fact_orders() -> int:
    """
    Upsert staging.orders into analytics.fact_orders.

    Resolves surrogate keys from the current SCD2 dimension rows.
    Handles CDC by upserting order_status so late-arriving status
    changes (e.g. Pending → Refunded) overwrite the previous value.
    """
    sql = """
        INSERT INTO analytics.fact_orders
            (order_id, customer_sk, product_sk, date_key,
             order_timestamp, quantity, revenue, profit,
             order_status, payment_method)
        SELECT
            o.order_id::UUID,
            dc.customer_sk,
            dp.product_sk,
            TO_CHAR(o.order_timestamp::TIMESTAMPTZ, 'YYYYMMDD')::INTEGER AS date_key,
            o.order_timestamp::TIMESTAMPTZ,
            o.quantity::INTEGER,
            o.total_amount::NUMERIC                 AS revenue,
            (o.total_amount::NUMERIC
              - COALESCE(dp.cost_price, 0) * o.quantity::INTEGER) AS profit,
            o.order_status,
            o.payment_method
        FROM staging.orders o
        LEFT JOIN analytics.dim_customers dc
            ON dc.customer_id = o.customer_id::UUID AND dc.is_current = TRUE
        LEFT JOIN analytics.dim_products dp
            ON dp.product_id = o.product_id::UUID AND dp.is_current = TRUE
        ON CONFLICT (order_id, order_timestamp) DO UPDATE SET
            order_status = EXCLUDED.order_status,
            revenue      = EXCLUDED.revenue,
            profit       = EXCLUDED.profit
    """
    with transaction() as cur:
        cur.execute(sql)
        n = cur.rowcount
    logger.info("fact_orders upserted %d rows", n)
    return n


# ---------------------------------------------------------------------------
# fact_payments  (UPSERT)
# ---------------------------------------------------------------------------

def transform_fact_payments() -> int:
    sql = """
        INSERT INTO analytics.fact_payments
            (payment_id, order_id, date_key, payment_date,
             payment_method, amount, payment_status)
        SELECT
            p.payment_id::UUID,
            p.order_id::UUID,
            TO_CHAR(p.payment_date::TIMESTAMPTZ, 'YYYYMMDD')::INTEGER,
            p.payment_date::TIMESTAMPTZ,
            p.payment_method,
            p.amount::NUMERIC,
            p.payment_status
        FROM staging.payments p
        ON CONFLICT (payment_id) DO UPDATE SET
            payment_status = EXCLUDED.payment_status,
            amount         = EXCLUDED.amount
    """
    with transaction() as cur:
        cur.execute(sql)
        n = cur.rowcount
    logger.info("fact_payments upserted %d rows", n)
    return n


# ---------------------------------------------------------------------------
# fact_returns  (UPSERT)
# ---------------------------------------------------------------------------

def transform_fact_returns() -> int:
    sql = """
        INSERT INTO analytics.fact_returns
            (return_id, order_id, date_key, return_date,
             refund_amount, return_reason, return_status)
        SELECT
            r.return_id::UUID,
            r.order_id::UUID,
            TO_CHAR(r.return_date::TIMESTAMPTZ, 'YYYYMMDD')::INTEGER,
            r.return_date::TIMESTAMPTZ,
            r.refund_amount::NUMERIC,
            r.return_reason,
            r.return_status
        FROM staging.returns r
        ON CONFLICT (return_id) DO UPDATE SET
            return_status = EXCLUDED.return_status,
            refund_amount = EXCLUDED.refund_amount
    """
    with transaction() as cur:
        cur.execute(sql)
        n = cur.rowcount
    logger.info("fact_returns upserted %d rows", n)
    return n


# ---------------------------------------------------------------------------
# agg_revenue  (DELETE-then-INSERT for idempotent daily aggregation)
# ---------------------------------------------------------------------------

def transform_agg_revenue() -> int:
    """
    Rebuild agg_revenue from staging.revenue.
    Uses DELETE-then-INSERT for idempotency on the same record_date.
    """
    sql_delete = """
        DELETE FROM analytics.agg_revenue
        WHERE record_date IN (SELECT DISTINCT record_date::DATE FROM staging.revenue)
    """
    sql_insert = """
        INSERT INTO analytics.agg_revenue
            (record_date, total_revenue, total_profit, total_orders, updated_at)
        SELECT
            s.record_date::DATE,
            SUM(s.total_revenue::NUMERIC),
            SUM(s.total_profit::NUMERIC),
            COUNT(DISTINCT o.order_id)          AS total_orders,
            NOW()
        FROM staging.revenue s
        LEFT JOIN staging.orders o
            ON o.order_timestamp::DATE = s.record_date::DATE
               AND o.order_status = 'Completed'
        GROUP BY s.record_date::DATE
    """
    with transaction() as cur:
        cur.execute(sql_delete)
        cur.execute(sql_insert)
        n = cur.rowcount
    logger.info("agg_revenue rebuilt %d rows", n)
    return n


# ---------------------------------------------------------------------------
# Full transform orchestrator
# ---------------------------------------------------------------------------

def run_all_transforms() -> dict[str, int]:
    """
    Execute all staging → analytics transforms in dependency order.
    Returns a map of {table_name: rows_affected}.
    """
    results = {}
    results["dim_customers"]  = transform_dim_customers()
    results["dim_products"]   = transform_dim_products()
    results["dim_inventory"]  = transform_dim_inventory()
    results["fact_orders"]    = transform_fact_orders()
    results["fact_payments"]  = transform_fact_payments()
    results["fact_returns"]   = transform_fact_returns()
    results["agg_revenue"]    = transform_agg_revenue()
    return results
