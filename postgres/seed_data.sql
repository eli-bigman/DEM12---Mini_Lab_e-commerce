-- ============================================================
-- seed_data.sql
-- Bootstrap 3 months of demo analytics data (optional).
-- 
-- This is NOT auto-executed on container startup.
-- Run manually via: make seed
--
-- Safe to run alongside the pipeline:
--   - Uses ON CONFLICT DO NOTHING (idempotent)
--   - Inserts only into analytics schema (pipeline uses staging -> transform)
--   - Different UUIDs than pipeline-generated data (no collision)
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
SET search_path TO analytics, public;

-- 1) Seed customer dimension (SCD2 current rows only)
INSERT INTO analytics.dim_customers (
    customer_id,
    signup_date,
    country,
    acquisition_channel,
    customer_segment,
    valid_from,
    is_current
)
SELECT
    uuid_generate_v5('6ba7b811-9dad-11d1-80b4-00c04fd430c8'::uuid, 'customer-' || i::text),
    (CURRENT_DATE - ((abs(hashtext('signup-' || i::text)) % 365)::text || ' days')::interval)::date,
    (ARRAY['USA', 'UK', 'Germany', 'Canada', 'Netherlands', 'Ghana', 'Kenya'])[1 + (abs(hashtext('country-' || i::text)) % 7)],
    (ARRAY['Organic', 'Paid Search', 'Social', 'Referral', 'Email'])[1 + (abs(hashtext('channel-' || i::text)) % 5)],
    (ARRAY['New', 'Returning', 'VIP'])[1 + (abs(hashtext('segment-' || i::text)) % 3)],
    NOW() - ((abs(hashtext('cust-valid-from-' || i::text)) % 120)::text || ' days')::interval,
    TRUE
FROM generate_series(1, 300) AS g(i)
ON CONFLICT DO NOTHING;

-- 2) Seed product dimension (SCD2 current rows only)
INSERT INTO analytics.dim_products (
    product_id,
    product_name,
    category,
    cost_price,
    selling_price,
    valid_from,
    is_current
)
SELECT
    uuid_generate_v5('6ba7b811-9dad-11d1-80b4-00c04fd430c8'::uuid, 'product-' || i::text),
    'Product ' || LPAD(i::text, 2, '0'),
    (ARRAY['Electronics', 'Home', 'Fashion', 'Beauty', 'Sports'])[1 + (abs(hashtext('cat-' || i::text)) % 5)],
    ROUND((8 + (abs(hashtext('cost-' || i::text)) % 900) / 10.0)::numeric, 2),
    ROUND((20 + (abs(hashtext('sell-' || i::text)) % 1400) / 10.0)::numeric, 2),
    NOW() - ((abs(hashtext('prod-valid-from-' || i::text)) % 120)::text || ' days')::interval,
    TRUE
FROM generate_series(1, 40) AS g(i)
ON CONFLICT DO NOTHING;

-- 3) Generate ~3 months of orders into fact_orders
WITH params AS (
    SELECT
        (CURRENT_DATE - INTERVAL '89 days')::date AS start_date,
        CURRENT_DATE::date AS end_date,
        24::int AS orders_per_day
),
customer_pool AS (
    SELECT
        customer_sk,
        ROW_NUMBER() OVER (ORDER BY customer_sk) AS rn,
        COUNT(*) OVER () AS total
    FROM analytics.dim_customers
    WHERE is_current = TRUE
),
product_pool AS (
    SELECT
        product_sk,
        cost_price,
        selling_price,
        ROW_NUMBER() OVER (ORDER BY product_sk) AS rn,
        COUNT(*) OVER () AS total
    FROM analytics.dim_products
    WHERE is_current = TRUE
),
order_blueprint AS (
    SELECT
        d::date AS order_date,
        n AS order_seq,
        TO_CHAR(d::date, 'YYYYMMDD') || '-' || LPAD(n::text, 3, '0') AS order_seed
    FROM params p
    CROSS JOIN generate_series(p.start_date, p.end_date, '1 day'::interval) AS d
    CROSS JOIN generate_series(1, p.orders_per_day) AS n
),
order_rows AS (
    SELECT
        uuid_generate_v5('6ba7b811-9dad-11d1-80b4-00c04fd430c8'::uuid, 'order-' || ob.order_seed) AS order_id,
        cp.customer_sk,
        pp.product_sk,
        ob.order_date,
        (
            ob.order_date::timestamp
            + ((abs(hashtext(ob.order_seed || '-sec')) % 86400)::text || ' seconds')::interval
        ) AS order_timestamp,
        (1 + (abs(hashtext(ob.order_seed || '-qty')) % 4))::int AS quantity,
        pp.cost_price,
        pp.selling_price,
        (ARRAY['Card', 'Bank Transfer', 'Mobile Money', 'PayPal'])[1 + (abs(hashtext(ob.order_seed || '-pay-method')) % 4)] AS payment_method,
        CASE
            WHEN (abs(hashtext(ob.order_seed || '-status')) % 100) < 84 THEN 'Completed'
            WHEN (abs(hashtext(ob.order_seed || '-status')) % 100) < 93 THEN 'Pending'
            ELSE 'Cancelled'
        END AS order_status
    FROM order_blueprint ob
    JOIN customer_pool cp
      ON cp.rn = 1 + (abs(hashtext(ob.order_seed || '-cust')) % cp.total)
    JOIN product_pool pp
      ON pp.rn = 1 + (abs(hashtext(ob.order_seed || '-prod')) % pp.total)
)
INSERT INTO analytics.fact_orders (
    order_id,
    customer_sk,
    product_sk,
    date_key,
    order_timestamp,
    quantity,
    revenue,
    profit,
    order_status,
    payment_method
)
SELECT
    o.order_id,
    o.customer_sk,
    o.product_sk,
    TO_CHAR(o.order_date, 'YYYYMMDD')::int AS date_key,
    o.order_timestamp,
    o.quantity,
    CASE
        WHEN o.order_status = 'Completed' THEN ROUND((o.quantity * o.selling_price)::numeric, 2)
        ELSE 0::numeric
    END AS revenue,
    CASE
        WHEN o.order_status = 'Completed' THEN ROUND((o.quantity * (o.selling_price - o.cost_price))::numeric, 2)
        ELSE 0::numeric
    END AS profit,
    o.order_status,
    o.payment_method
FROM order_rows o
ON CONFLICT (order_id, order_timestamp) DO NOTHING;

-- 4) Seed payments for completed orders
INSERT INTO analytics.fact_payments (
    payment_id,
    order_id,
    date_key,
    payment_date,
    payment_method,
    amount,
    payment_status
)
SELECT
    uuid_generate_v5('6ba7b811-9dad-11d1-80b4-00c04fd430c8'::uuid, 'payment-' || fo.order_id::text),
    fo.order_id,
    TO_CHAR((fo.order_timestamp + INTERVAL '2 hours')::date, 'YYYYMMDD')::int,
    fo.order_timestamp + INTERVAL '2 hours',
    fo.payment_method,
    fo.revenue,
    CASE
        WHEN (abs(hashtext(fo.order_id::text || '-payment-status')) % 100) < 97 THEN 'Paid'
        ELSE 'Failed'
    END AS payment_status
FROM analytics.fact_orders fo
WHERE fo.order_status = 'Completed'
ON CONFLICT (payment_id) DO NOTHING;

-- 5) Seed returns for a small slice of completed orders
INSERT INTO analytics.fact_returns (
    return_id,
    order_id,
    date_key,
    return_date,
    refund_amount,
    return_reason,
    return_status
)
SELECT
    uuid_generate_v5('6ba7b811-9dad-11d1-80b4-00c04fd430c8'::uuid, 'return-' || fo.order_id::text),
    fo.order_id,
    TO_CHAR((fo.order_timestamp + INTERVAL '7 days')::date, 'YYYYMMDD')::int,
    fo.order_timestamp + INTERVAL '7 days',
    ROUND((fo.revenue * (0.50 + (abs(hashtext(fo.order_id::text || '-refund-scale')) % 50) / 100.0))::numeric, 2),
    (ARRAY['Damaged', 'Wrong Item', 'No Longer Needed', 'Late Delivery'])[1 + (abs(hashtext(fo.order_id::text || '-reason')) % 4)],
    'Approved'
FROM analytics.fact_orders fo
WHERE fo.order_status = 'Completed'
  AND (abs(hashtext(fo.order_id::text || '-is-return')) % 100) < 8
ON CONFLICT (return_id) DO NOTHING;

-- 6) Refresh daily aggregate table from seeded orders
INSERT INTO analytics.agg_revenue (
    record_date,
    total_revenue,
    total_profit,
    total_orders,
    updated_at
)
SELECT
    fo.order_timestamp::date AS record_date,
    ROUND(SUM(fo.revenue), 2) AS total_revenue,
    ROUND(SUM(fo.profit), 2) AS total_profit,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    NOW()
FROM analytics.fact_orders fo
WHERE fo.order_status = 'Completed'
GROUP BY fo.order_timestamp::date
ON CONFLICT (record_date) DO UPDATE SET
    total_revenue = EXCLUDED.total_revenue,
    total_profit = EXCLUDED.total_profit,
    total_orders = EXCLUDED.total_orders,
    updated_at = NOW();

-- 7) Keep dashboard materialized views in sync with freshly seeded analytics data
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
