import time
from pyspark.sql.functions import year, month, col
from config.config import (
    APP_NAME,
    ORDERS_PATH,
    CUSTOMERS_PATH,
    PAYMENTS_PATH,
    OUTPUT_BRONZE_ORDERS,
    OUTPUT_BRONZE_CUSTOMERS,
    OUTPUT_BRONZE_PAYMENTS,
    OUTPUT_SILVER_ENRICHED,
    OUTPUT_SILVER_QUARANTINE,
    OUTPUT_GOLD_SUMMARY
)
from src.spark_session import get_spark
from src.logger import get_logger
from src.ingestion import load_raw_csv, write_to_bronze, CUSTOMERS_SCHEMA, ORDERS_SCHEMA, PAYMENTS_SCHEMA
from src.transformation import clean_orders, clean_customers, clean_payments, enrich_and_join, generate_category_summary
from src.validation import DataValidator
from src.warehouse import write_to_snowflake, write_partitioned_delta

logger = get_logger("Orchestrator")

def run_pipeline() -> None:
    """
    Main Orchestrator executing the Medallion ETL pipeline:
    Bronze (Ingestion) -> Silver (Cleaning, Validation, Quarantine, Enrichment) -> Gold (Snowflake Warehouse load)
    """
    start_time = time.time()
    logger.info("Starting E-Commerce ETL Pipeline Job...")

    # 1. Initialize Spark Session
    spark = get_spark(APP_NAME)
    logger.info("Spark Session Initialized.")

    try:
        # ----------------------------------------------------
        # STAGE 1: Bronze Layer (Raw CSV to Delta Lake Ingestion)
        # ----------------------------------------------------
        logger.info("--- STAGE 1: Bronze Ingestion ---")
        
        # Load raw inputs with schema enforcement
        raw_orders = load_raw_csv(spark, ORDERS_PATH, ORDERS_SCHEMA)
        raw_customers = load_raw_csv(spark, CUSTOMERS_PATH, CUSTOMERS_SCHEMA)
        raw_payments = load_raw_csv(spark, PAYMENTS_PATH, PAYMENTS_SCHEMA)
        
        # Persist directly to Delta Lake Bronze layer for ACID/Time-travel capabilities
        write_to_bronze(raw_orders, OUTPUT_BRONZE_ORDERS)
        write_to_bronze(raw_customers, OUTPUT_BRONZE_CUSTOMERS)
        write_to_bronze(raw_payments, OUTPUT_BRONZE_PAYMENTS)

        # ----------------------------------------------------
        # STAGE 2: Silver Layer (Validation, Cleaning, Enrichment, Quarantine)
        # ----------------------------------------------------
        logger.info("--- STAGE 2: Silver Cleaning & Enrichment ---")
        
        # Read from Bronze Delta tables (representing the source of truth for the rest of the pipeline)
        bronze_orders_df = spark.read.format("delta").load(OUTPUT_BRONZE_ORDERS)
        bronze_customers_df = spark.read.format("delta").load(OUTPUT_BRONZE_CUSTOMERS)
        bronze_payments_df = spark.read.format("delta").load(OUTPUT_BRONZE_PAYMENTS)

        # Critical integrity checks: abort if primary keys are null or tables are completely empty
        DataValidator.run_critical_checks(bronze_orders_df, "order_id", "bronze_orders")
        DataValidator.run_critical_checks(bronze_customers_df, "customer_id", "bronze_customers")
        DataValidator.run_critical_checks(bronze_payments_df, "payment_id", "bronze_payments")

        # Clean the bronze dataframes
        cleaned_orders = clean_orders(bronze_orders_df)
        cleaned_customers = clean_customers(bronze_customers_df)
        cleaned_payments = clean_payments(bronze_payments_df)

        # Perform optimized broadcast joins and calculations
        enriched_orders_df = enrich_and_join(cleaned_orders, cleaned_customers, cleaned_payments)

        # Implement Quarantine pattern on enriched dataset
        clean_silver_df, quarantined_df = DataValidator.split_quarantine_orders(enriched_orders_df)

        # Write Quarantine records to dedicated Delta directory for data stewards to review
        if quarantined_df.count() > 0:
            logger.warning(f"Saving quarantined records to: {OUTPUT_SILVER_QUARANTINE}")
            (
                quarantined_df.write
                .format("delta")
                .mode("append")
                .save(OUTPUT_SILVER_QUARANTINE)
            )

        # Add calendar features (Year/Month) for partition optimization
        partitioned_silver_df = (
            clean_silver_df
            .withColumn("year", year(col("order_date")))
            .withColumn("month", month(col("order_date")))
        )

        # Write clean enriched dataset partitioned by Year/Month to Delta Lake (Silver Storage)
        write_partitioned_delta(partitioned_silver_df, OUTPUT_SILVER_ENRICHED, ["year", "month"])

        # ----------------------------------------------------
        # STAGE 3: Gold Layer & Warehousing (Snowflake Loading)
        # ----------------------------------------------------
        logger.info("--- STAGE 3: Gold Summary & Snowflake Load ---")
        
        # Read from Silver Delta Table
        silver_orders = spark.read.format("delta").load(OUTPUT_SILVER_ENRICHED)
        
        # Filter for Delivered orders only for final revenue reports
        delivered_orders = silver_orders.filter(col("order_status") == "DELIVERED")
        
        # Generate Gold business aggregated metric table
        category_summary = generate_category_summary(delivered_orders)
        
        # Log previews of results for monitoring
        logger.info("Category Summary preview:")
        category_summary.show(truncate=False)

        # Load clean orders and aggregated summaries into Snowflake (using local Parquet fallback if necessary)
        write_to_snowflake(delivered_orders, "FACT_DELIVERED_ORDERS", mode="overwrite")
        write_to_snowflake(category_summary, "AGG_CATEGORY_REVENUE", mode="overwrite")

        elapsed_time = time.time() - start_time
        logger.info(f"[SUCCESS] Pipeline Execution Completed Successfully in {elapsed_time:.2f} seconds!")

    except Exception as e:
        logger.error(f"[ERROR] Pipeline Job Failed: {str(e)}", exc_info=True)
        raise e

if __name__ == "__main__":
    run_pipeline()
