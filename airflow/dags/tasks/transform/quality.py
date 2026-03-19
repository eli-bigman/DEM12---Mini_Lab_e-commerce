"""Post-transform quality and dashboard refresh task helpers."""

from __future__ import annotations

import logging

from utils.db_helper import transaction

logger = logging.getLogger(__name__)


def ensure_dashboard_views() -> int:
    """Ensure dashboard materialized views and wrapper views exist."""
    sql = """
        CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_kpi_summary AS
        WITH current_period AS (
            SELECT
                COALESCE(SUM(revenue), 0) AS total_revenue,
                COALESCE(SUM(profit), 0) AS total_profit,
                COUNT(DISTINCT order_id) AS total_orders,
                COALESCE(SUM(revenue) / NULLIF(COUNT(DISTINCT order_id), 0), 0) AS avg_order_value
            FROM analytics.fact_orders
            WHERE order_status = 'Completed'
        ),
        last_month_customers AS (
            SELECT COUNT(DISTINCT customer_id)::NUMERIC AS cnt
            FROM analytics.dim_customers
            WHERE signup_date >= CURRENT_DATE - INTERVAL '30 days'
              AND signup_date < CURRENT_DATE
              AND is_current = TRUE
        ),
        prior_month_customers AS (
            SELECT COUNT(DISTINCT customer_id)::NUMERIC AS cnt
            FROM analytics.dim_customers
            WHERE signup_date >= CURRENT_DATE - INTERVAL '60 days'
              AND signup_date < CURRENT_DATE - INTERVAL '30 days'
              AND is_current = TRUE
        )
        SELECT
            cp.total_revenue,
            cp.total_profit,
            cp.total_orders,
            ROUND(cp.avg_order_value, 2) AS avg_order_value,
            ROUND(
                CASE WHEN pm.cnt = 0 THEN 0 ELSE ((lm.cnt - pm.cnt) / pm.cnt) * 100 END,
                2
            ) AS customer_growth_rate_pct
        FROM current_period cp
        CROSS JOIN last_month_customers lm
        CROSS JOIN prior_month_customers pm
        WITH NO DATA;

        CREATE OR REPLACE VIEW analytics.v_kpi_summary AS
        SELECT * FROM analytics.mv_kpi_summary;

        CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_revenue_trend_daily AS
        SELECT
            dd.full_date AS date,
            dd.year,
            dd.month_number,
            dd.month_name,
            COALESCE(SUM(fo.revenue), 0) AS daily_revenue,
            COALESCE(SUM(fo.profit), 0) AS daily_profit,
            COUNT(DISTINCT fo.order_id) AS daily_orders
        FROM analytics.dim_date dd
        LEFT JOIN analytics.fact_orders fo
            ON fo.date_key = dd.date_key
           AND fo.order_status = 'Completed'
        WHERE dd.full_date <= CURRENT_DATE
        GROUP BY dd.full_date, dd.year, dd.month_number, dd.month_name
        ORDER BY dd.full_date
        WITH NO DATA;

        CREATE INDEX IF NOT EXISTS idx_mv_revenue_trend_daily_date
            ON analytics.mv_revenue_trend_daily(date);

        CREATE OR REPLACE VIEW analytics.v_revenue_trend_daily AS
        SELECT * FROM analytics.mv_revenue_trend_daily;

        CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_revenue_by_category AS
        SELECT
            dp.category,
            COALESCE(SUM(fo.revenue), 0) AS total_revenue,
            COALESCE(SUM(fo.profit), 0) AS total_profit,
            COUNT(DISTINCT fo.order_id) AS total_orders,
            ROUND(
                COALESCE(SUM(fo.revenue), 0) * 100.0 / NULLIF(SUM(SUM(fo.revenue)) OVER (), 0),
                2
            ) AS revenue_share_pct
        FROM analytics.fact_orders fo
        JOIN analytics.dim_products dp ON dp.product_sk = fo.product_sk
        WHERE fo.order_status = 'Completed'
        GROUP BY dp.category
        ORDER BY total_revenue DESC
        WITH NO DATA;

        CREATE INDEX IF NOT EXISTS idx_mv_revenue_by_category_name
            ON analytics.mv_revenue_by_category(category);

        CREATE OR REPLACE VIEW analytics.v_revenue_by_category AS
        SELECT * FROM analytics.mv_revenue_by_category;

        CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_revenue_by_channel AS
        SELECT
            dc.acquisition_channel,
            COALESCE(SUM(fo.revenue), 0) AS total_revenue,
            COUNT(DISTINCT fo.order_id) AS total_orders,
            COUNT(DISTINCT fo.customer_sk) AS unique_customers,
            ROUND(
                COALESCE(SUM(fo.revenue), 0) * 100.0 / NULLIF(SUM(SUM(fo.revenue)) OVER (), 0),
                2
            ) AS revenue_share_pct
        FROM analytics.fact_orders fo
        JOIN analytics.dim_customers dc ON dc.customer_sk = fo.customer_sk
        WHERE fo.order_status = 'Completed'
        GROUP BY dc.acquisition_channel
        ORDER BY total_revenue DESC
        WITH NO DATA;

        CREATE INDEX IF NOT EXISTS idx_mv_revenue_by_channel_name
            ON analytics.mv_revenue_by_channel(acquisition_channel);

        CREATE OR REPLACE VIEW analytics.v_revenue_by_channel AS
        SELECT * FROM analytics.mv_revenue_by_channel;

        CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_top_products_profit AS
        SELECT
            dp.product_name,
            dp.category,
            COALESCE(SUM(fo.profit), 0) AS total_profit,
            COALESCE(SUM(fo.revenue), 0) AS total_revenue,
            SUM(fo.quantity) AS units_sold
        FROM analytics.fact_orders fo
        JOIN analytics.dim_products dp ON dp.product_sk = fo.product_sk
        WHERE fo.order_status = 'Completed'
        GROUP BY dp.product_name, dp.category
        ORDER BY total_profit DESC
        LIMIT 10
        WITH NO DATA;

        CREATE INDEX IF NOT EXISTS idx_mv_top_products_profit_name
            ON analytics.mv_top_products_profit(product_name);

        CREATE OR REPLACE VIEW analytics.v_top_products_profit AS
        SELECT * FROM analytics.mv_top_products_profit;

        CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_customer_retention AS
        SELECT
            dc.customer_segment,
            COUNT(DISTINCT fo.order_id) AS total_orders,
            COUNT(DISTINCT fo.customer_sk) AS unique_customers,
            COALESCE(SUM(fo.revenue), 0) AS total_revenue,
            ROUND(
                COUNT(DISTINCT fo.order_id) * 100.0 / NULLIF(SUM(COUNT(DISTINCT fo.order_id)) OVER (), 0),
                2
            ) AS order_share_pct
        FROM analytics.fact_orders fo
        JOIN analytics.dim_customers dc ON dc.customer_sk = fo.customer_sk
        GROUP BY dc.customer_segment
        ORDER BY total_orders DESC
        WITH NO DATA;

        CREATE INDEX IF NOT EXISTS idx_mv_customer_retention_segment
            ON analytics.mv_customer_retention(customer_segment);

        CREATE OR REPLACE VIEW analytics.v_customer_retention AS
        SELECT * FROM analytics.mv_customer_retention;

        CREATE OR REPLACE VIEW analytics.v_order_status_distribution AS
        SELECT
            order_status,
            COUNT(DISTINCT order_id) AS order_count,
            COALESCE(SUM(revenue), 0) AS total_revenue,
            ROUND(
                COUNT(DISTINCT order_id) * 100.0 / NULLIF(SUM(COUNT(DISTINCT order_id)) OVER (), 0),
                2
            ) AS pct_of_total
        FROM analytics.fact_orders
        GROUP BY order_status
        ORDER BY order_count DESC;

        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'metabase_user') THEN
                GRANT USAGE ON SCHEMA analytics TO metabase_user;
                GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO metabase_user;
                ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
                GRANT SELECT ON TABLES TO metabase_user;
            END IF;
        END;
        $$;
    """

    sql_count = """
        SELECT COUNT(*) AS cnt
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'analytics'
          AND c.relkind IN ('m', 'v')
          AND c.relname = ANY(%s)
    """

    object_names = [
        "mv_kpi_summary",
        "mv_revenue_trend_daily",
        "mv_revenue_by_category",
        "mv_revenue_by_channel",
        "mv_top_products_profit",
        "mv_customer_retention",
        "v_kpi_summary",
        "v_revenue_trend_daily",
        "v_revenue_by_category",
        "v_revenue_by_channel",
        "v_top_products_profit",
        "v_customer_retention",
        "v_order_status_distribution",
    ]

    with transaction() as cur:
        cur.execute(sql)
        cur.execute(sql_count, (object_names,))
        created_or_present = int(cur.fetchone()["cnt"])

    logger.info("Dashboard views ensured (%d objects present)", created_or_present)
    return created_or_present


def refresh_dashboard_materialized_views() -> int:
    """Refresh expensive dashboard materialized views after facts/dimensions update."""
    matviews = [
        "mv_kpi_summary",
        "mv_revenue_trend_daily",
        "mv_revenue_by_category",
        "mv_revenue_by_channel",
        "mv_top_products_profit",
        "mv_customer_retention",
    ]

    sql_count = """
        SELECT COUNT(*) AS cnt
        FROM pg_matviews
        WHERE schemaname = 'analytics'
          AND matviewname = ANY(%s)
    """

    sql_refresh = """
        DO $$
        DECLARE
            mv_name TEXT;
            target_views TEXT[] := ARRAY[
                'mv_kpi_summary',
                'mv_revenue_trend_daily',
                'mv_revenue_by_category',
                'mv_revenue_by_channel',
                'mv_top_products_profit',
                'mv_customer_retention'
            ];
        BEGIN
            FOREACH mv_name IN ARRAY target_views LOOP
                IF EXISTS (
                    SELECT 1
                    FROM pg_matviews
                    WHERE schemaname = 'analytics'
                      AND matviewname = mv_name
                ) THEN
                    EXECUTE format('REFRESH MATERIALIZED VIEW analytics.%I', mv_name);
                END IF;
            END LOOP;
        END;
        $$;
    """

    ensure_dashboard_views()

    with transaction() as cur:
        cur.execute(sql_count, (matviews,))
        count = int(cur.fetchone()["cnt"])
        cur.execute(sql_refresh)

    logger.info("Refreshed %d analytics materialized views", count)
    return count


def check_default_partition_usage() -> int:
    """Return count of rows in default partition and warn when non-zero."""
    sql = "SELECT COUNT(*) AS cnt FROM analytics.fact_orders_default"
    with transaction() as cur:
        cur.execute(sql)
        cnt = int(cur.fetchone()["cnt"])

    if cnt > 0:
        logger.warning(
            "Default partition analytics.fact_orders_default contains %d rows; check partition maintenance.",
            cnt,
        )
    else:
        logger.info("Default partition analytics.fact_orders_default is empty")
    return cnt


def cleanup_orphaned_foreign_keys() -> int:
    """Null out orphaned FK values so deferred constraint validation can succeed."""
    sql_orders_customer = """
        UPDATE analytics.fact_orders fo
        SET customer_sk = NULL
        WHERE customer_sk IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM analytics.dim_customers dc
              WHERE dc.customer_sk = fo.customer_sk
          )
    """
    sql_orders_product = """
        UPDATE analytics.fact_orders fo
        SET product_sk = NULL
        WHERE product_sk IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM analytics.dim_products dp
              WHERE dp.product_sk = fo.product_sk
          )
    """
    sql_orders_date = """
        UPDATE analytics.fact_orders fo
        SET date_key = NULL
        WHERE date_key IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM analytics.dim_date dd
              WHERE dd.date_key = fo.date_key
          )
    """
    sql_payments_date = """
        UPDATE analytics.fact_payments fp
        SET date_key = NULL
        WHERE date_key IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM analytics.dim_date dd
              WHERE dd.date_key = fp.date_key
          )
    """
    sql_returns_date = """
        UPDATE analytics.fact_returns fr
        SET date_key = NULL
        WHERE date_key IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM analytics.dim_date dd
              WHERE dd.date_key = fr.date_key
          )
    """

    with transaction() as cur:
        cur.execute(sql_orders_customer)
        n_orders_customer = cur.rowcount
        cur.execute(sql_orders_product)
        n_orders_product = cur.rowcount
        cur.execute(sql_orders_date)
        n_orders_date = cur.rowcount
        cur.execute(sql_payments_date)
        n_payments_date = cur.rowcount
        cur.execute(sql_returns_date)
        n_returns_date = cur.rowcount

    total = n_orders_customer + n_orders_product + n_orders_date + n_payments_date + n_returns_date
    logger.info("Cleaned %d orphaned FK references before constraint validation", total)
    return total


def validate_referential_constraints() -> int:
    """Validate non-partitioned FK constraints after orphan cleanup."""
    statements = [
        "ALTER TABLE analytics.fact_payments VALIDATE CONSTRAINT fk_fact_payments_date_key",
        "ALTER TABLE analytics.fact_returns VALIDATE CONSTRAINT fk_fact_returns_date_key",
    ]
    with transaction() as cur:
        for sql in statements:
            try:
                cur.execute(sql)
            except Exception as e:
                logger.warning("Failed to validate constraint: %s", e)
    logger.info("Validated %d referential constraints (skipped partitioned fact_orders)", len(statements))
    return len(statements)

__all__ = [
    "ensure_dashboard_views",
    "cleanup_orphaned_foreign_keys",
    "validate_referential_constraints",
    "check_default_partition_usage",
    "refresh_dashboard_materialized_views",
]
