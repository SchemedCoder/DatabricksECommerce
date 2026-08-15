from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, trim, lower, upper, to_date, broadcast, sum, avg, count, countDistinct
from src.logger import get_logger

logger = get_logger("Transformation")

def clean_orders(df: DataFrame) -> DataFrame:
    """
    Cleans raw orders data: casts types, handles nulls, and standardizes categories.
    """
    logger.info("Cleaning orders DataFrame...")
    return (
        df.fillna({"discount": 0.0})
        .withColumn("category", trim(lower(col("category"))))
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("discount", col("discount").cast("double"))
    )

def clean_customers(df: DataFrame) -> DataFrame:
    """
    Cleans raw customers data: casts types and standardizes string fields.
    """
    logger.info("Cleaning customers DataFrame...")
    return (
        df.withColumn("customer_name", trim(col("customer_name")))
        .withColumn("city", trim(col("city")))
        .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))
    )

def clean_payments(df: DataFrame) -> DataFrame:
    """
    Cleans raw payments data: casts types and standardizes payment status.
    """
    logger.info("Cleaning payments DataFrame...")
    return (
        df.withColumn("payment_mode", trim(upper(col("payment_mode"))))
        .withColumn("payment_status", trim(upper(col("payment_status"))))
        .withColumn("payment_date", to_date(col("payment_date"), "yyyy-MM-dd"))
    )

def enrich_and_join(orders_df: DataFrame, customers_df: DataFrame, payments_df: DataFrame) -> DataFrame:
    """
    Enriches orders with monetary calculations and joins customers and payments data.
    
    Optimizations Applied (3-4 Years Exp Justifications):
    1. Native Spark expressions are used for bucket categorizations instead of a PySpark Python UDF.
       - *Why*: Native Catalyst expressions execute inside the JVM/Tungsten engine, avoiding serialization 
         overhead between the JVM and Python interpreter (which occurs with standard Python UDFs).
    2. Broadcast Joins:
       - *Why*: The customers lookup table is small. Broadcasting it avoids expensive shuffle operations 
         across the Spark cluster.
    """
    logger.info("Enriching orders and performing joins (Silver Layer creation)...")
    
    # 1. Monetary and Bucket Calculations (Replacing slow Python UDF with native case-when)
    enriched_orders = (
        orders_df
        .withColumn("gross_amount", col("quantity") * col("unit_price"))
        .withColumn("discount_amount", (col("gross_amount") * col("discount")) / 100.0)
        .withColumn("net_amount", col("gross_amount") - col("discount_amount"))
        # Native optimization:
        .withColumn(
            "order_bucket",
            when(col("quantity") >= 4, "large")
            .when(col("quantity") >= 2, "medium")
            .when(col("quantity").isNotNull(), "small")
            .otherwise("unknown")
        )
    )

    # 2. Joins using broadcast for the customer dimension
    # Joining on 'customer_id' and 'order_id' as matching columns to prevent duplicate key references.
    final_df = (
        enriched_orders
        .join(broadcast(customers_df), "customer_id", "inner")
        .join(broadcast(payments_df), "order_id", "left")
    )
    return final_df

def generate_category_summary(enriched_df: DataFrame) -> DataFrame:
    """
    Generates high-level aggregated business summary metrics per product category.
    """
    logger.info("Generating category summary aggregations (Gold Layer creation)...")
    summary = (
        enriched_df
        .groupBy("category")
        .agg(
            count("*").alias("total_orders"),
            sum("net_amount").alias("total_revenue"),
            avg("net_amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers")
        )
        .orderBy(col("total_revenue").desc())
    )
    return summary
