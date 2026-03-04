-- ============================================================
-- 04_analytics_views.sql
-- Pre-built SQL views to power Metabase dashboard cards.
-- Each view maps directly to one KPI or chart on the dashboard.
-- ============================================================
SET search_path TO analytics;
-- ------------------------------------------------------------
-- v_kpi_summary:  Single-row executive KPI snapshot
-- Used for: Total Revenue, Total Orders, AOV, Gross Profit,
--           Customer Growth Rate cards
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_kpi_summary AS WITH current_period AS (
        SELECT COALESCE(SUM(revenue), 0) AS total_revenue,
            COALESCE(SUM(profit), 0) AS total_profit,
            COUNT(DISTINCT order_id) AS total_orders,
            COALESCE(
                SUM(revenue) / NULLIF(COUNT(DISTINCT order_id), 0),
                0
            ) AS avg_order_value
        FROM analytics.fact_orders
        WHERE order_status = 'Completed'
    ),
    last_month_customers AS (
        SELECT COUNT(DISTINCT customer_id)::NUMERIC AS cnt
        FROM analytics.dim_customers
        WHERE valid_from >= NOW() - INTERVAL '30 days'
            AND is_current = TRUE
    ),
    prior_month_customers AS (
        SELECT COUNT(DISTINCT customer_id)::NUMERIC AS cnt
        FROM analytics.dim_customers
        WHERE valid_from >= NOW() - INTERVAL '60 days'
            AND valid_from < NOW() - INTERVAL '30 days'
    )
SELECT cp.total_revenue,
    cp.total_profit,
    cp.total_orders,
    ROUND(cp.avg_order_value, 2) AS avg_order_value,
    ROUND(
        CASE
            WHEN pm.cnt = 0 THEN 0
            ELSE ((lm.cnt - pm.cnt) / pm.cnt) * 100
        END,
        2
    ) AS customer_growth_rate_pct
FROM current_period cp
    CROSS JOIN last_month_customers lm
    CROSS JOIN prior_month_customers pm;
-- ------------------------------------------------------------
-- v_revenue_trend_daily: Revenue and profit by calendar day
-- Used for: Revenue Trend Over Time (line chart)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_revenue_trend_daily AS
SELECT dd.full_date AS date,
    dd.year,
    dd.month_number,
    dd.month_name,
    COALESCE(SUM(fo.revenue), 0) AS daily_revenue,
    COALESCE(SUM(fo.profit), 0) AS daily_profit,
    COUNT(DISTINCT fo.order_id) AS daily_orders
FROM analytics.dim_date dd
    LEFT JOIN analytics.fact_orders fo ON fo.date_key = dd.date_key
    AND fo.order_status = 'Completed'
WHERE dd.full_date <= CURRENT_DATE
GROUP BY dd.full_date,
    dd.year,
    dd.month_number,
    dd.month_name
ORDER BY dd.full_date;
-- ------------------------------------------------------------
-- v_revenue_by_category: Revenue aggregated by product category
-- Used for: Revenue by Category (bar / pie chart)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_revenue_by_category AS
SELECT dp.category,
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
ORDER BY total_revenue DESC;
-- ------------------------------------------------------------
-- v_revenue_by_channel: Revenue by customer acquisition channel
-- Used for: Revenue by Acquisition Channel (bar chart)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_revenue_by_channel AS
SELECT dc.acquisition_channel,
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
ORDER BY total_revenue DESC;
-- ------------------------------------------------------------
-- v_top_products_profit: Top 10 products by total profit
-- Used for: Top 10 Products by Profit (table / bar chart)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_top_products_profit AS
SELECT dp.product_name,
    dp.category,
    COALESCE(SUM(fo.profit), 0) AS total_profit,
    COALESCE(SUM(fo.revenue), 0) AS total_revenue,
    SUM(fo.quantity) AS units_sold
FROM analytics.fact_orders fo
    JOIN analytics.dim_products dp ON dp.product_sk = fo.product_sk
WHERE fo.order_status = 'Completed'
GROUP BY dp.product_name,
    dp.category
ORDER BY total_profit DESC
LIMIT 10;
-- ------------------------------------------------------------
-- v_customer_retention: Segment order volume breakdown
-- Used for: Customer Retention Indicators (pie / bar chart)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_customer_retention AS
SELECT dc.customer_segment,
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
ORDER BY total_orders DESC;
-- ------------------------------------------------------------
-- v_order_status_distribution: Order counts by status
-- Used for: Order Status Distribution (donut / bar chart)
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.v_order_status_distribution AS
SELECT order_status,
    COUNT(DISTINCT order_id) AS order_count,
    COALESCE(SUM(revenue), 0) AS total_revenue,
    ROUND(
        COUNT(DISTINCT order_id) * 100.0 / NULLIF(SUM(COUNT(DISTINCT order_id)) OVER (), 0),
        2
    ) AS pct_of_total
FROM analytics.fact_orders
GROUP BY order_status
ORDER BY order_count DESC;
-- Grant access to Metabase
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO metabase_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics
GRANT SELECT ON TABLES TO metabase_user;