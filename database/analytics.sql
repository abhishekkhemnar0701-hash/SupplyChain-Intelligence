-- 1. Overall business KPIs
CREATE OR REPLACE VIEW vw_supply_chain_kpis AS
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(*) AS total_order_events,
    COALESCE(SUM(total_amount), 0) AS total_revenue,
    COALESCE(SUM(quantity), 0) AS total_quantity_sold,
    COALESCE(AVG(total_amount), 0) AS average_event_value
FROM processed_orders;


-- 2. Daily sales performance
CREATE OR REPLACE VIEW vw_daily_sales AS
SELECT
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_revenue
FROM processed_orders
GROUP BY order_date;


-- 3. Product-wise performance
CREATE OR REPLACE VIEW vw_product_performance AS
SELECT
    product_id,
    product_name,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_quantity_sold,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS average_order_value
FROM processed_orders
GROUP BY product_id, product_name;


-- 4. Monthly sales trends
CREATE OR REPLACE VIEW vw_monthly_sales AS
SELECT
    order_year,
    order_month,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(quantity) AS total_quantity,
    SUM(total_amount) AS total_revenue
FROM processed_orders
GROUP BY order_year, order_month;


-- 5. Order status distribution
CREATE OR REPLACE VIEW vw_order_status AS
SELECT
    order_status,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(total_amount) AS total_revenue
FROM processed_orders
GROUP BY order_status;