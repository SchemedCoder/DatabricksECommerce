from config import APP_NAME, ORDERS_PATH, CUSTOMERS_PATH, PAYMENTS_PATH, OUTPUT_ENRICHED_PATH, OUTPUT_SUMMARY_PATH
from spark_session import get_spark
from ingestion import load_csv
from transformation import clean_orders, enrich_and_join, generate_category_summary
from partitioning import write_partitioned_output, write_summary_output
from pyspark.sql.functions import col, lit, when, trim, lower, to_date, broadcast, udf, sum, avg, count, countDistinct


# 2. Initialize Spark Session
spark = get_spark(APP_NAME)
print("Spark Session Initialized")

# 3. Ingestion
print("⏳ Loading raw data...")
orders_raw = load_csv(spark, ORDERS_PATH)
customers_raw = load_csv(spark, CUSTOMERS_PATH)
payments_raw = load_csv(spark, PAYMENTS_PATH)

# 4. Transformations
print("🧹 Cleaning and enriching data...")
orders_cleaned = clean_orders(orders_raw)
enriched_df = enrich_and_join(orders_cleaned, customers_raw, payments_raw)

# Filter for Delivered Orders only
delivered_orders = enriched_df.filter(col("order_status") == "DELIVERED")

# Generate Aggregated Summary
category_summary = generate_category_summary(delivered_orders)

# 5. Display Previews
print("\n--- Enriched Orders Schema ---")
delivered_orders.printSchema()

print("\n--- Top 5 Enriched Orders ---")
delivered_orders.show(5, truncate=False)

print("\n--- Category Summary ---")
category_summary.show()

# 6. Writing Data
print("💾 Writing final outputs...")
write_partitioned_output(delivered_orders, OUTPUT_ENRICHED_PATH)
write_summary_output(category_summary, OUTPUT_SUMMARY_PATH)

print("🎉 Pipeline Execution Completed Successfully!")