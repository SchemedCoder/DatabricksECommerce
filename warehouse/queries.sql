-- ====================================================================
-- E-Commerce Databricks & Snowflake Analytical Queries
-- ====================================================================

-- ----------------------------------------------------
-- Databricks Delta Lake Optimization & Administration
-- ----------------------------------------------------
USE CATALOG ecommerce_catalog;
USE SCHEMA analytics;

-- 1. Delta Lake Time Travel Query (View historical snapshots)
-- Query the conformed Silver table as it was in version 1 of transactions
SELECT * FROM silver_enriched_orders VERSION AS OF 1 LIMIT 5;

-- 2. Audit Trail Inspection
-- View transaction history of operations (MERGE, WRITE, OPTIMIZE)
DESCRIBE HISTORY silver_enriched_orders;

-- 3. File Compaction
-- Merges small parquet files into optimized target file sizes (usually ~1GB in prod)
OPTIMIZE silver_enriched_orders;

-- 4. Multi-dimensional Clustering (Z-Ordering)
-- Co-locates data on disk by order_date. Spark will skip files that do not contain 
-- target query dates during execution, reducing physical I/O.
OPTIMIZE silver_enriched_orders ZORDER BY (order_date);

-- 5. Physical Storage Reclamation (Vacuum)
-- Deletes physical files removed from transaction log older than retention threshold.
-- WARNING: Time travel past 168 hours will be disabled.
VACUUM silver_enriched_orders RETAIN 168 HOURS;


-- ----------------------------------------------------
-- Snowflake Gold Layer Analytics
-- ----------------------------------------------------
USE DATABASE ECOMMERCE_DB;
USE SCHEMA PUBLIC;

-- 1. Analytical Report: Top Cities by Total Net Spend and Order Volume
SELECT 
    city,
    COUNT(order_id) as total_orders,
    SUM(net_amount) as total_revenue,
    ROUND(AVG(net_amount), 2) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM FACT_DELIVERED_ORDERS
GROUP BY city
ORDER BY total_revenue DESC;

-- 2. Window Function: Find the highest value order bucket for each category
WITH ranked_buckets AS (
    SELECT 
        category,
        order_bucket,
        SUM(net_amount) as bucket_revenue,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY SUM(net_amount) DESC) as rnk
    FROM FACT_DELIVERED_ORDERS
    GROUP BY category, order_bucket
)
SELECT 
    category,
    order_bucket,
    bucket_revenue
FROM ranked_buckets
WHERE rnk = 1;

