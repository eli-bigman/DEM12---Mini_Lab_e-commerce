-- ============================================================
-- 02_staging_schema.sql
-- Staging tables for raw CSV data loaded from MinIO.
-- These act as a landing zone before transformation into the
-- analytics star schema. All tables support idempotent UPSERT.
-- ============================================================

\connect ecommerce

SET search_path TO staging;

-- ------------------------------------------------------------
-- File tracking: prevents reprocessing already-loaded files
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.processed_files (
    id              SERIAL PRIMARY KEY,
    object_path     TEXT        NOT NULL UNIQUE,   -- MinIO path: entity/YYYY/MM/DD/file.csv
    entity          TEXT        NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    row_count       INTEGER,
    quarantine_count INTEGER     DEFAULT 0,
    status          TEXT        NOT NULL DEFAULT 'success'  -- success | partial
);

-- ------------------------------------------------------------
-- Customers staging
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id         UUID        PRIMARY KEY,
    signup_date         DATE,
    country             TEXT,
    acquisition_channel TEXT,
    customer_segment    TEXT,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_file        TEXT
);

-- ------------------------------------------------------------
-- Products staging
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.products (
    product_id          UUID        PRIMARY KEY,
    product_name        TEXT,
    category            TEXT,
    cost_price          NUMERIC(12, 2),
    selling_price       NUMERIC(12, 2),
    inventory_quantity  INTEGER,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_file        TEXT
);

-- ------------------------------------------------------------
-- Orders staging
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.orders (
    order_id            UUID        PRIMARY KEY,
    customer_id         UUID,
    product_id          UUID,
    quantity            INTEGER,
    unit_price          NUMERIC(12, 2),
    total_amount        NUMERIC(12, 2),
    order_timestamp     TIMESTAMPTZ,
    payment_method      TEXT,
    order_status        TEXT,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_file        TEXT
);

-- ------------------------------------------------------------
-- Payments staging
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.payments (
    payment_id          UUID        PRIMARY KEY,
    order_id            UUID,
    payment_date        TIMESTAMPTZ,
    payment_method      TEXT,
    amount              NUMERIC(12, 2),
    payment_status      TEXT,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_file        TEXT
);

-- ------------------------------------------------------------
-- Inventory staging
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.inventory (
    inventory_id        UUID        PRIMARY KEY,
    product_id          UUID,
    warehouse_location  TEXT,
    quantity_on_hand    INTEGER,
    last_restock_date   DATE,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_file        TEXT
);

-- ------------------------------------------------------------
-- Revenue staging
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.revenue (
    revenue_id          UUID        PRIMARY KEY,
    record_date         DATE,
    total_revenue       NUMERIC(15, 2),
    total_profit        NUMERIC(15, 2),
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_file        TEXT
);

-- ------------------------------------------------------------
-- Returns staging
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.returns (
    return_id           UUID        PRIMARY KEY,
    order_id            UUID,
    return_date         TIMESTAMPTZ,
    return_reason       TEXT,
    refund_amount       NUMERIC(12, 2),
    return_status       TEXT,
    _loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    _source_file        TEXT
);

-- ------------------------------------------------------------
-- Indexes to speed up transformation queries
-- ------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_stg_orders_customer  ON staging.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_stg_orders_product   ON staging.orders(product_id);
CREATE INDEX IF NOT EXISTS idx_stg_orders_ts        ON staging.orders(order_timestamp);
CREATE INDEX IF NOT EXISTS idx_stg_payments_order   ON staging.payments(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_returns_order    ON staging.returns(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_inventory_product ON staging.inventory(product_id);

-- Grant to Metabase (read-only)
GRANT SELECT ON ALL TABLES IN SCHEMA staging TO metabase_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT SELECT ON TABLES TO metabase_user;
