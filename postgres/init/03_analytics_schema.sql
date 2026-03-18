-- ============================================================
-- 03_analytics_schema.sql
-- Star schema for the e-commerce analytics platform.
--
-- Design principles applied:
--   - Fact tables are range-partitioned monthly by their primary timestamp.
--   - Dimension tables implement SCD Type 2 (valid_from, valid_to, is_current)
--     so that historical fact rows remain linked to the correct dimension state.
--   - All upserts use INSERT ... ON CONFLICT for idempotency.
--   - Indexes placed on all foreign keys and common filter columns.
-- ============================================================
SET search_path TO analytics;
-- ============================================================
-- DIMENSION TABLES
-- ============================================================
-- ------------------------------------------------------------
-- dim_date: Pre-populated time dimension.
-- Populated once via a stored procedure (see bottom of file).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key INTEGER PRIMARY KEY,
    -- YYYYMMDD integer surrogate key
    full_date DATE NOT NULL UNIQUE,
    day_of_week SMALLINT NOT NULL,
    -- 0 = Sunday
    day_name TEXT NOT NULL,
    day_of_month SMALLINT NOT NULL,
    day_of_year SMALLINT NOT NULL,
    week_of_year SMALLINT NOT NULL,
    month_number SMALLINT NOT NULL,
    month_name TEXT NOT NULL,
    quarter SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);
-- ------------------------------------------------------------
-- dim_customers: SCD Type 2
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_customers (
    customer_sk BIGSERIAL PRIMARY KEY,
    -- surrogate key
    customer_id UUID NOT NULL,
    -- natural key
    signup_date DATE,
    country TEXT,
    acquisition_channel TEXT,
    customer_segment TEXT,
    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to TIMESTAMPTZ,
    -- NULL means current record
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_dim_cust_nk ON analytics.dim_customers(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_cust_current ON analytics.dim_customers(customer_id, is_current);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_cust_current_unique ON analytics.dim_customers(customer_id)
WHERE is_current = TRUE;
-- ------------------------------------------------------------
-- dim_products: SCD Type 2
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_products (
    product_sk BIGSERIAL PRIMARY KEY,
    product_id UUID NOT NULL,
    product_name TEXT,
    category TEXT,
    cost_price NUMERIC(12, 2),
    selling_price NUMERIC(12, 2),
    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS idx_dim_prod_nk ON analytics.dim_products(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_prod_current ON analytics.dim_products(product_id, is_current);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dim_prod_current_unique ON analytics.dim_products(product_id)
WHERE is_current = TRUE;
-- ------------------------------------------------------------
-- dim_inventory: Non-SCD (current snapshot only)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_inventory (
    inventory_sk BIGSERIAL PRIMARY KEY,
    inventory_id UUID NOT NULL UNIQUE,
    product_id UUID NOT NULL,
    warehouse_location TEXT,
    quantity_on_hand INTEGER,
    last_restock_date DATE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dim_inv_product ON analytics.dim_inventory(product_id);
-- ============================================================
-- FACT TABLES (partitioned monthly)
-- ============================================================
-- ------------------------------------------------------------
-- fact_orders: partitioned by order_timestamp (monthly)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_orders (
    order_id UUID NOT NULL,
    customer_sk BIGINT,
    -- FK → dim_customers.customer_sk
    product_sk BIGINT,
    -- FK → dim_products.product_sk
    date_key INTEGER,
    -- FK → dim_date.date_key
    order_timestamp TIMESTAMPTZ NOT NULL,
    quantity INTEGER,
    revenue NUMERIC(12, 2),
    profit NUMERIC(12, 2),
    order_status TEXT,
    payment_method TEXT,
    UNIQUE (order_id, order_timestamp)
) PARTITION BY RANGE (order_timestamp);
-- Create monthly partitions for a rolling window
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
        p_name := format(
            'fact_orders_%s_%s',
            EXTRACT(YEAR FROM month_start)::INT,
            lpad(EXTRACT(MONTH FROM month_start)::INT::TEXT, 2, '0')
        );
        p_from := month_start;
        p_to := (month_start + INTERVAL '1 month')::DATE;

        IF NOT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'analytics'
              AND c.relname = p_name
        ) THEN
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS analytics.%I
                 PARTITION OF analytics.fact_orders
                 FOR VALUES FROM (%L) TO (%L)',
                p_name,
                p_from,
                p_to
            );
        END IF;
    END LOOP;
END;
$$;
-- Default partition for out-of-range data
CREATE TABLE IF NOT EXISTS analytics.fact_orders_default PARTITION OF analytics.fact_orders DEFAULT;
-- Indexes on the parent table propagate to partitions
CREATE INDEX IF NOT EXISTS idx_fo_order_id ON analytics.fact_orders(order_id);
CREATE INDEX IF NOT EXISTS idx_fo_customer ON analytics.fact_orders(customer_sk);
CREATE INDEX IF NOT EXISTS idx_fo_product ON analytics.fact_orders(product_sk);
CREATE INDEX IF NOT EXISTS idx_fo_date ON analytics.fact_orders(date_key);
-- ------------------------------------------------------------
-- fact_payments
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_payments (
    payment_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    date_key INTEGER,
    payment_date TIMESTAMPTZ,
    payment_method TEXT,
    amount NUMERIC(12, 2),
    payment_status TEXT
);
CREATE INDEX IF NOT EXISTS idx_fp_order ON analytics.fact_payments(order_id);
CREATE INDEX IF NOT EXISTS idx_fp_date ON analytics.fact_payments(date_key);
-- ------------------------------------------------------------
-- fact_returns
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_returns (
    return_id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    date_key INTEGER,
    return_date TIMESTAMPTZ,
    refund_amount NUMERIC(12, 2),
    return_reason TEXT,
    return_status TEXT
);
CREATE INDEX IF NOT EXISTS idx_fr_order ON analytics.fact_returns(order_id);
CREATE INDEX IF NOT EXISTS idx_fr_date ON analytics.fact_returns(date_key);
-- ------------------------------------------------------------
-- agg_revenue: daily aggregated revenue summary
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.agg_revenue (
    record_date DATE PRIMARY KEY,
    total_revenue NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_profit NUMERIC(15, 2) NOT NULL DEFAULT 0,
    total_orders INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- ============================================================
-- POPULATE dim_date for a rolling 4-year window
-- ============================================================
INSERT INTO analytics.dim_date (
    date_key,
    full_date,
    day_of_week,
    day_name,
    day_of_month,
    day_of_year,
    week_of_year,
    month_number,
    month_name,
    quarter,
    year,
    is_weekend
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER,
    d,
    EXTRACT(DOW FROM d)::SMALLINT,
    TO_CHAR(d, 'Day'),
    EXTRACT(DAY FROM d)::SMALLINT,
    EXTRACT(DOY FROM d)::SMALLINT,
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
-- Grant to Metabase
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO metabase_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
GRANT SELECT ON TABLES TO metabase_user;