"""Dimension transform task helpers."""

from __future__ import annotations

import logging

from utils.db_helper import transaction

logger = logging.getLogger(__name__)


def transform_dim_customers() -> int:
    """Merge staging.customers into analytics.dim_customers using SCD Type 2."""
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
            NOT EXISTS (
                SELECT 1 FROM analytics.dim_customers dc
                WHERE dc.customer_id = s.customer_id
            )
            OR
            s.customer_id IN (SELECT customer_id FROM expired)
        ON CONFLICT DO NOTHING
    """
    with transaction() as cur:
        cur.execute(sql)
        n = cur.rowcount
    logger.info("dim_customers upserted %d rows", n)
    return n


def transform_dim_products() -> int:
    """Merge staging.products into analytics.dim_products using SCD Type 2."""
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

__all__ = [
    "transform_dim_customers",
    "transform_dim_products",
    "transform_dim_inventory",
]
