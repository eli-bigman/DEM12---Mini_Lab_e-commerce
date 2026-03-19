"""Fact and aggregate transform task helpers."""

from __future__ import annotations

import logging

from utils.db_helper import transaction

logger = logging.getLogger(__name__)


def transform_fact_orders() -> int:
    """Merge staging.orders into analytics.fact_orders with delete-then-insert semantics."""
    sql_delete = """
        DELETE FROM analytics.fact_orders
        WHERE order_id IN (SELECT DISTINCT order_id::UUID FROM staging.orders)
    """
    sql_insert = """
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
    """
    with transaction() as cur:
        cur.execute(sql_delete)
        cur.execute(sql_insert)
        n = cur.rowcount
    logger.info("fact_orders upserted %d rows", n)
    return n


def transform_fact_payments() -> int:
    """Upsert staging.payments into analytics.fact_payments."""
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


def transform_fact_returns() -> int:
    """Upsert staging.returns into analytics.fact_returns."""
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


def transform_agg_revenue() -> int:
    """Rebuild agg_revenue from recent staging.revenue changes."""
    sql_delete = """
        DELETE FROM analytics.agg_revenue
        WHERE record_date IN (
            SELECT DISTINCT record_date::DATE FROM staging.revenue
            WHERE _loaded_at >= NOW() - INTERVAL '2 hours'
        )
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
        WHERE s.record_date::DATE IN (
            SELECT DISTINCT record_date::DATE FROM staging.revenue
            WHERE _loaded_at >= NOW() - INTERVAL '2 hours'
        )
        GROUP BY s.record_date::DATE
    """
    with transaction() as cur:
        cur.execute(sql_delete)
        cur.execute(sql_insert)
        n = cur.rowcount
    logger.info("agg_revenue rebuilt %d rows", n)
    return n

__all__ = [
    "transform_fact_orders",
    "transform_fact_payments",
    "transform_fact_returns",
    "transform_agg_revenue",
]
