from pyspark.sql.functions import col, lit, when, trim, lower, to_date, broadcast, udf, sum, avg, count, countDistinct
from pyspark.sql.types import StringType

# 1. UDF Definition
@udf(returnType=StringType())
def order_bucket_udf(quantity):
    if quantity is None: return "unknown"
    if quantity >= 4: return "large"
    elif quantity >= 2: return "medium"
    else: return "small"

# 2. Cleaning Function
def clean_orders(df):
    return (
        df.fillna({"discount": 0})
        .withColumn("category", trim(lower(col("category"))))
        .withColumn("order_date", to_date(col("order_date")))
        .withColumn("quantity", col("quantity").cast("int"))
        .withColumn("unit_price", col("unit_price").cast("double"))
    )

# 3. Optimized Join (Fixes the AMBIGUOUS_REFERENCE error)
def enrich_and_join(orders_df, customers_df, payments_df):
    enriched_orders = (
        orders_df
        .withColumn("gross_amount", col("quantity") * col("unit_price"))
        .withColumn("discount_amount", (col("gross_amount") * col("discount")) / 100)
        .withColumn("net_amount", col("gross_amount") - col("discount_amount"))
        .withColumn("order_bucket", order_bucket_udf(col("quantity")))
    )

    # Merges matching columns automatically to prevent duplicate 'customer_id' and 'order_id' fields
    final_df = (
        enriched_orders
        .join(broadcast(customers_df), "customer_id", "inner")
        .join(broadcast(payments_df), "order_id", "left")
    )
    return final_df

# 4. Aggregation Function (Fixes the Window Performance Warning)
def generate_category_summary(enriched_df):
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