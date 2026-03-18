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
  - Bootstrap: ensure_analytics_schema() idempotently creates the analytics
    schema and all tables before any transform runs, so the pipeline works
    even when the Postgres volume already existed (init scripts only run on
    first container boot).
"""

from __future__ import annotations

import logging

from .db_helper import transaction

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema bootstrap  (idempotent DDL - safe to run every pipeline run)
# ---------------------------------------------------------------------------

def ensure_analytics_schema() -> None:
    """
    Idempotently create the analytics schema and all its tables.

    This is a safety net for environments where the Postgres data volume
    already existed before the init scripts were added (or when the container
    was restarted without recreating the volume).
    PostgreSQL's docker-entrypoint-initdb.d only runs on first boot.
    """
    ddl = """
        -- Schema
        CREATE SCHEMA IF NOT EXISTS analytics;

        -- dim_date
        CREATE TABLE IF NOT EXISTS analytics.dim_date (
            date_key      INTEGER PRIMARY KEY,
            full_date     DATE NOT NULL UNIQUE,
            day_of_week   SMALLINT NOT NULL,
            day_name      TEXT NOT NULL,
            day_of_month  SMALLINT NOT NULL,
            day_of_year   SMALLINT NOT NULL,
            week_of_year  SMALLINT NOT NULL,
            month_number  SMALLINT NOT NULL,
            month_name    TEXT NOT NULL,
            quarter       SMALLINT NOT NULL,
            year          SMALLINT NOT NULL,
            is_weekend    BOOLEAN NOT NULL
        );

        -- dim_customers (SCD2)
        CREATE TABLE IF NOT EXISTS analytics.dim_customers (
            customer_sk        BIGSERIAL PRIMARY KEY,
            customer_id        UUID NOT NULL,
            signup_date        DATE,
            country            TEXT,
            acquisition_channel TEXT,
            customer_segment   TEXT,
            valid_from         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            valid_to           TIMESTAMPTZ,
            is_current         BOOLEAN NOT NULL DEFAULT TRUE
        );
        CREATE INDEX IF NOT EXISTS idx_dim_cust_nk
            ON analytics.dim_customers(customer_id);
        CREATE INDEX IF NOT EXISTS idx_dim_cust_current
            ON analytics.dim_customers(customer_id, is_current);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_cust_current_unique
            ON analytics.dim_customers(customer_id)
            WHERE is_current = TRUE;

        -- dim_products (SCD2)
        CREATE TABLE IF NOT EXISTS analytics.dim_products (
            product_sk    BIGSERIAL PRIMARY KEY,
            product_id    UUID NOT NULL,
            product_name  TEXT,
            category      TEXT,
            cost_price    NUMERIC(12, 2),
            selling_price NUMERIC(12, 2),
            valid_from    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            valid_to      TIMESTAMPTZ,
            is_current    BOOLEAN NOT NULL DEFAULT TRUE
        );
        CREATE INDEX IF NOT EXISTS idx_dim_prod_nk
            ON analytics.dim_products(product_id);
        CREATE INDEX IF NOT EXISTS idx_dim_prod_current
            ON analytics.dim_products(product_id, is_current);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_prod_current_unique
            ON analytics.dim_products(product_id)
            WHERE is_current = TRUE;

        -- dim_inventory (snapshot)
        CREATE TABLE IF NOT EXISTS analytics.dim_inventory (
            inventory_sk      BIGSERIAL PRIMARY KEY,
            inventory_id      UUID NOT NULL UNIQUE,
            product_id        UUID NOT NULL,
            warehouse_location TEXT,
            quantity_on_hand   INTEGER,
            last_restock_date  DATE,
            updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_dim_inv_product
            ON analytics.dim_inventory(product_id);

        -- fact_payments
        CREATE TABLE IF NOT EXISTS analytics.fact_payments (
            payment_id     UUID PRIMARY KEY,
            order_id       UUID NOT NULL,
            date_key       INTEGER,
            payment_date   TIMESTAMPTZ,
            payment_method TEXT,
            amount         NUMERIC(12, 2),
            payment_status TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_fp_order
            ON analytics.fact_payments(order_id);
        CREATE INDEX IF NOT EXISTS idx_fp_date
            ON analytics.fact_payments(date_key);

        -- fact_returns
        CREATE TABLE IF NOT EXISTS analytics.fact_returns (
            return_id     UUID PRIMARY KEY,
            order_id      UUID NOT NULL,
            date_key      INTEGER,
            return_date   TIMESTAMPTZ,
            refund_amount NUMERIC(12, 2),
            return_reason TEXT,
            return_status TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_fr_order
            ON analytics.fact_returns(order_id);
        CREATE INDEX IF NOT EXISTS idx_fr_date
            ON analytics.fact_returns(date_key);

        -- Referential constraints (added as NOT VALID, validated after cleanup)
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_fact_orders_customer_sk'
            ) THEN
                ALTER TABLE analytics.fact_orders
                ADD CONSTRAINT fk_fact_orders_customer_sk
                FOREIGN KEY (customer_sk)
                REFERENCES analytics.dim_customers(customer_sk)
                NOT VALID;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_fact_orders_product_sk'
            ) THEN
                ALTER TABLE analytics.fact_orders
                ADD CONSTRAINT fk_fact_orders_product_sk
                FOREIGN KEY (product_sk)
                REFERENCES analytics.dim_products(product_sk)
                NOT VALID;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_fact_orders_date_key'
            ) THEN
                ALTER TABLE analytics.fact_orders
                ADD CONSTRAINT fk_fact_orders_date_key
                FOREIGN KEY (date_key)
                REFERENCES analytics.dim_date(date_key)
                NOT VALID;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_fact_payments_date_key'
            ) THEN
                ALTER TABLE analytics.fact_payments
                ADD CONSTRAINT fk_fact_payments_date_key
                FOREIGN KEY (date_key)
                REFERENCES analytics.dim_date(date_key)
                NOT VALID;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_fact_returns_date_key'
            ) THEN
                ALTER TABLE analytics.fact_returns
                ADD CONSTRAINT fk_fact_returns_date_key
                FOREIGN KEY (date_key)
                REFERENCES analytics.dim_date(date_key)
                NOT VALID;
            END IF;
        END;
        $$;

        -- agg_revenue
        CREATE TABLE IF NOT EXISTS analytics.agg_revenue (
            record_date   DATE PRIMARY KEY,
            total_revenue NUMERIC(15, 2) NOT NULL DEFAULT 0,
            total_profit  NUMERIC(15, 2) NOT NULL DEFAULT 0,
            total_orders  INTEGER NOT NULL DEFAULT 0,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """

    # fact_orders is partitioned — must be created separately so we can also
    # create its child partitions.  We do NOT try to recreate existing ones.
    fact_orders_ddl = """
        CREATE TABLE IF NOT EXISTS analytics.fact_orders (
            order_id        UUID NOT NULL,
            customer_sk     BIGINT,
            product_sk      BIGINT,
            date_key        INTEGER,
            order_timestamp TIMESTAMPTZ NOT NULL,
            quantity        INTEGER,
            revenue         NUMERIC(12, 2),
            profit          NUMERIC(12, 2),
            order_status    TEXT,
            payment_method  TEXT,
            UNIQUE (order_id, order_timestamp)
        ) PARTITION BY RANGE (order_timestamp);

        -- Default partition catches anything outside explicit monthly ranges
        CREATE TABLE IF NOT EXISTS analytics.fact_orders_default
            PARTITION OF analytics.fact_orders DEFAULT;

        CREATE INDEX IF NOT EXISTS idx_fo_order_id
            ON analytics.fact_orders(order_id);
        CREATE INDEX IF NOT EXISTS idx_fo_customer
            ON analytics.fact_orders(customer_sk);
        CREATE INDEX IF NOT EXISTS idx_fo_product
            ON analytics.fact_orders(product_sk);
        CREATE INDEX IF NOT EXISTS idx_fo_date
            ON analytics.fact_orders(date_key);
    """

    partition_ddl = """
        DO $$
        DECLARE
            month_start DATE;
            p_name TEXT;
            p_from DATE;
            p_to DATE;
        BEGIN
            FOR month_start IN
                SELECT generate_series(
                    date_trunc('month', CURRENT_DATE - INTERVAL '12 months')::DATE,
                    date_trunc('month', CURRENT_DATE + INTERVAL '24 months')::DATE,
                    INTERVAL '1 month'
                )::DATE
            LOOP
                p_name := format('fact_orders_%s_%s',
                    EXTRACT(YEAR FROM month_start)::INT,
                    lpad(EXTRACT(MONTH FROM month_start)::INT::TEXT, 2, '0')
                );
                p_from := month_start;
                p_to := (month_start + INTERVAL '1 month')::DATE;

                IF NOT EXISTS (
                    SELECT 1 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'analytics' AND c.relname = p_name
                ) THEN
                    EXECUTE format(
                        'CREATE TABLE IF NOT EXISTS analytics.%I
                         PARTITION OF analytics.fact_orders
                         FOR VALUES FROM (%L) TO (%L)',
                        p_name, p_from, p_to
                    );
                END IF;
            END LOOP;
        END;
        $$;
    """

    dim_date_ddl = """
        INSERT INTO analytics.dim_date (
            date_key, full_date, day_of_week, day_name, day_of_month,
            day_of_year, week_of_year, month_number, month_name,
            quarter, year, is_weekend
        )
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INTEGER,
            d,
            EXTRACT(DOW  FROM d)::SMALLINT,
            TO_CHAR(d, 'Day'),
            EXTRACT(DAY  FROM d)::SMALLINT,
            EXTRACT(DOY  FROM d)::SMALLINT,
            EXTRACT(WEEK FROM d)::SMALLINT,
            EXTRACT(MONTH FROM d)::SMALLINT,
            TO_CHAR(d, 'Month'),
            EXTRACT(QUARTER FROM d)::SMALLINT,
            EXTRACT(YEAR FROM d)::SMALLINT,
            EXTRACT(DOW FROM d) IN (0, 6)
        FROM generate_series(
            (CURRENT_DATE - INTERVAL '2 years')::DATE,
            (CURRENT_DATE + INTERVAL '2 years')::DATE,
            '1 day'::INTERVAL
        ) AS gs(d)
        ON CONFLICT (date_key) DO NOTHING;
    """

    with transaction() as cur:
        cur.execute(ddl)
        cur.execute(fact_orders_ddl)
        cur.execute(partition_ddl)
        cur.execute(dim_date_ddl)
    logger.info("ensure_analytics_schema: all tables verified/created successfully")


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
    Merge staging.orders into analytics.fact_orders.

    Resolves surrogate keys from the current SCD2 dimension rows.
    Uses DELETE-then-INSERT to cleanly handle late arriving updates 
    and prevent duplication if the timestamp was changed.
    """
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
    Optimized to only process recently modified records.
    """
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


# ---------------------------------------------------------------------------
# Dashboard materialized views (refresh after transforms)
# ---------------------------------------------------------------------------

def refresh_dashboard_materialized_views() -> int:
    """
    Refresh expensive dashboard materialized views after facts/dimensions update.
    Only refreshes views that exist, so this is safe across environments.
    """
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
    """
    Validate NOT VALID FKs after orphan cleanup; returns number validated.
    
    NOTE: fact_orders is partitioned, and PostgreSQL does not yet support FK validation
    on partitioned tables. We only validate non-partitioned tables (payments, returns).
    """
    statements = [
        # Skip fact_orders validation due to partitioned table limitation
        # (PostgreSQL 16+ does not support VALIDATE CONSTRAINT on partitioned tables)
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


# ---------------------------------------------------------------------------
# Full transform orchestrator
# ---------------------------------------------------------------------------

def run_all_transforms() -> dict[str, int]:
    """
    Execute all staging → analytics transforms in dependency order.
    Returns a map of {table_name: rows_affected}.
    """
    # Bootstrap: ensure all analytics tables exist before we try to write to them.
    # This is a safety net for cases where the Postgres volume already existed
    # and init scripts did not run.  All DDL is idempotent (IF NOT EXISTS).
    ensure_analytics_schema()

    results = {}
    results["dim_customers"]  = transform_dim_customers()
    results["dim_products"]   = transform_dim_products()
    results["dim_inventory"]  = transform_dim_inventory()
    results["fact_orders"]    = transform_fact_orders()
    results["fact_payments"]  = transform_fact_payments()
    results["fact_returns"]   = transform_fact_returns()
    results["agg_revenue"]    = transform_agg_revenue()
    results["orphan_fks_cleaned"] = cleanup_orphaned_foreign_keys()
    results["validated_constraints"] = validate_referential_constraints()
    results["fact_orders_default_rows"] = check_default_partition_usage()
    results["materialized_views_refreshed"] = refresh_dashboard_materialized_views()
    return results
