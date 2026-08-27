-- ====================================================================
-- Databricks Unity Catalog & Snowflake Schema Definitions
-- ====================================================================

-- ----------------------------------------------------
-- Databricks Unity Catalog Delta Tables
-- ----------------------------------------------------
CREATE CATALOG IF NOT EXISTS ecommerce_catalog;
USE CATALOG ecommerce_catalog;

CREATE SCHEMA IF NOT EXISTS analytics;
USE SCHEMA analytics;

-- 1. Bronze Orders Table (Raw JSON/CSV ingestion replica)
CREATE TABLE IF NOT EXISTS bronze_orders (
    order_id INT,
    customer_id INT,
    order_date STRING,
    category STRING,
    quantity INT,
    unit_price DOUBLE,
    discount DOUBLE,
    order_status STRING,
    ingested_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.appendOnly' = 'true'
);

-- 2. Silver Enriched Orders Table (Cleaned, joined conformed fact table)
CREATE TABLE IF NOT EXISTS silver_enriched_orders (
    order_id INT,
    customer_id INT,
    customer_name STRING,
    city STRING,
    order_date DATE,
    category STRING,
    quantity INT,
    unit_price DOUBLE,
    discount DOUBLE,
    gross_amount DOUBLE,
    discount_amount DOUBLE,
    net_amount DOUBLE,
    order_bucket STRING,
    payment_mode STRING,
    payment_status STRING,
    year INT,
    month INT
)
USING DELTA
PARTITIONED BY (year, month)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 3. Gold Category Summary Table (Aggregated revenue metrics)
CREATE TABLE IF NOT EXISTS gold_category_summary (
    category STRING,
    total_orders LONG,
    total_revenue DOUBLE,
    avg_order_value DOUBLE,
    unique_customers LONG,
    calculated_at TIMESTAMP
)
USING DELTA;


-- ----------------------------------------------------
-- Snowflake Gold Data Warehouse DDL
-- ----------------------------------------------------
CREATE DATABASE IF NOT EXISTS ECOMMERCE_DB;
CREATE SCHEMA IF NOT EXISTS ECOMMERCE_DB.PUBLIC;

-- 1. Fact Table: Delivered Orders
CREATE TABLE IF NOT EXISTS ECOMMERCE_DB.PUBLIC.FACT_DELIVERED_ORDERS (
    order_id INT,
    customer_id INT,
    customer_name VARCHAR(100),
    city VARCHAR(50),
    order_date DATE,
    category VARCHAR(50),
    quantity INT,
    unit_price NUMBER(10, 2),
    discount NUMBER(5, 2),
    gross_amount NUMBER(12, 2),
    discount_amount NUMBER(12, 2),
    net_amount NUMBER(12, 2),
    order_bucket VARCHAR(20),
    payment_mode VARCHAR(20),
    payment_status VARCHAR(20),
    year INT,
    month INT
);

-- 2. Aggregated Summary Table: Category Revenue
CREATE TABLE IF NOT EXISTS ECOMMERCE_DB.PUBLIC.AGG_CATEGORY_REVENUE (
    category VARCHAR(50),
    total_orders INT,
    total_revenue NUMBER(15, 2),
    avg_order_value NUMBER(12, 2),
    unique_customers INT,
    calculated_at TIMESTAMP
);
