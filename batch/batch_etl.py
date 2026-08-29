
import os
import sys

# 1. Clean sys.path temporarily of any elements containing spaces
# This bypasses the Windows PySpark JVM launch bug when directory paths contain spaces.
saved_sys_path = list(sys.path)
sys.path = [p for p in sys.path if " " not in p]

# 2. Import PySpark and SparkSession
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, lower, to_date, broadcast, sum, avg, count, countDistinct, year, month, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, BooleanType, TimestampType

# 3. Restore sys.path for local imports
sys.path = saved_sys_path
workspace_dir = os.getcwd()
if workspace_dir not in sys.path:
    sys.path.insert(0, workspace_dir)

import urllib.request
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("BatchETL")

# Load environment variables
load_dotenv()

def get_spark_session(app_name="ECommerceMedallionETL"):
    """
    Creates a local Spark Session pre-configured for Delta Lake and Windows compatibility.
    To prevent network/Maven Ivy resolution errors, compatible Delta JARs are dynamically
    downloaded directly from Maven Central and loaded as local Spark Jars.
    """
    version = pyspark.__version__
    
    # Map PySpark versions to compatible Delta Lake jar versions
    if version.startswith("3.5"):
        delta_ver = "3.1.0"
    elif version.startswith("3.4"):
        delta_ver = "2.4.0"
    else:
        delta_ver = "3.1.0"  # Default fallback
        
    logger.info(f"Detected PySpark version: {version}. Fetching Delta Lake version: {delta_ver}")
    
    # Local Jars directory path
    jars_dir = "jars"
    os.makedirs(jars_dir, exist_ok=True)
    
    jars = {
        f"delta-spark_2.12-{delta_ver}.jar": f"https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/{delta_ver}/delta-spark_2.12-{delta_ver}.jar",
        f"delta-storage-{delta_ver}.jar": f"https://repo1.maven.org/maven2/io/delta/delta-storage/{delta_ver}/delta-storage-{delta_ver}.jar"
    }
    
    local_jar_paths = []
    for jar_name, url in jars.items():
        dest_path = os.path.join(jars_dir, jar_name)
        local_jar_paths.append(dest_path)
        if not os.path.exists(dest_path):
            logger.info(f"Downloading Delta JAR {jar_name} from Maven Central...")
            try:
                urllib.request.urlretrieve(url, dest_path)
                logger.info(f"Downloaded {jar_name} successfully.")
            except Exception as e:
                logger.error(f"Error downloading {jar_name}: {e}")
                raise e
                
    jar_config = ",".join(local_jar_paths)
    
    # Configure directories for local running to prevent locks and permission issues on Windows
    warehouse_dir = "data/spark-warehouse"
    derby_dir = "data/derby"
    
    # Clean sys.path permanently of any elements containing spaces to bypass Windows PySpark JVM launch bug
    sys.path = [p for p in sys.path if " " not in p]
    
    # Clean environment variables containing spaces/quotes to prevent Windows spark-class2.cmd parser failure
    for k in list(os.environ.keys()):
        if k.startswith("ANTIGRAVITY_"):
            del os.environ[k]
            
    spark_builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", jar_config) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_dir}") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true")
        
    spark = spark_builder.getOrCreate()
    return spark

class DataQualityException(Exception):
    """Exception raised when a critical data quality check fails."""
    pass

# Data Schemas
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("is_active", BooleanType(), True)
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("order_status", StringType(), True)
])

PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("payment_mode", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_date", StringType(), True)
])

def clean_orders(df):
    """Cleans orders raw data."""
    return df.fillna({"discount": 0.0}) \
        .withColumn("category", trim(lower(col("category")))) \
        .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
        .withColumn("quantity", col("quantity").cast("int")) \
        .withColumn("unit_price", col("unit_price").cast("double")) \
        .withColumn("discount", col("discount").cast("double"))

def clean_customers(df):
    """Cleans customers raw data."""
    return df \
        .withColumn("customer_name", trim(col("customer_name"))) \
        .withColumn("city", trim(col("city"))) \
        .withColumn("signup_date", to_date(col("signup_date"), "yyyy-MM-dd"))

def clean_payments(df):
    """Cleans payments raw data."""
    return df \
        .withColumn("payment_mode", trim(col("payment_mode"))) \
        .withColumn("payment_status", trim(col("payment_status"))) \
        .withColumn("payment_date", to_date(col("payment_date"), "yyyy-MM-dd"))

def run_critical_checks(df, primary_key, table_name):
    """Validates the input dataframe is not empty and has no null values in primary keys."""
    row_count = df.count()
    if row_count == 0:
        raise DataQualityException(f"Critical DQS Failure: {table_name} is empty.")
    
    null_count = df.filter(col(primary_key).isNull()).count()
    if null_count > 0:
        raise DataQualityException(f"Critical DQS Failure: Found {null_count} NULL keys in primary key '{primary_key}' of table '{table_name}'.")
    logger.info(f"DQS passed for {table_name} (Row count: {row_count})")

def write_to_snowflake(df, table_name):
    """
    Loads a Spark DataFrame into Snowflake.
    If credentials are set to mock/default or missing, it falls back to local Parquet files.
    """
    sf_user = os.getenv("SF_USER", "your_snowflake_username")
    sf_password = os.getenv("SF_PASSWORD")
    sf_account = os.getenv("SF_ACCOUNT")
    
    if any(v is None or "your_" in v for v in [sf_user, sf_password, sf_account]):
        fallback_path = os.path.join("data", "warehouse_fallback", table_name)
        logger.warning(f"Snowflake credentials missing or default. Falling back to local Parquet at {fallback_path}")
        df.write.mode("overwrite").parquet(fallback_path)
        logger.info(f"Local fallback write completed for table: {table_name}")
        return
        
    sf_options = {
        "sfURL": f"{sf_account}.snowflakecomputing.com" if not sf_account.endswith(".snowflakecomputing.com") else sf_account,
        "sfUser": sf_user,
        "sfPassword": sf_password,
        "sfDatabase": os.getenv("SF_DATABASE", "ECOMMERCE_DB"),
        "sfSchema": os.getenv("SF_SCHEMA", "PUBLIC"),
        "sfWarehouse": os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
        "sfRole": os.getenv("SF_ROLE", "SYSADMIN")
    }
    
    logger.info(f"Writing to Snowflake table {table_name}...")
    try:
        df.write \
            .format("snowflake") \
            .options(**sf_options) \
            .option("dbtable", table_name) \
            .mode("overwrite") \
            .save()
        logger.info(f"Successfully loaded data into Snowflake table: {table_name}")
    except Exception as e:
        logger.error(f"Error loading to Snowflake: {e}. Check connector and drivers.")
        raise e

def run_etl(orders_path="data/orders.csv", customers_path="data/customers.csv", payments_path="data/payments.csv"):
    """
    Runs the E-Commerce Medallion Batch Pipeline:
    1. Bronze Ingestion: Enforces schema and saves raw CSV to Bronze Delta tables.
    2. Silver Processing: Cleans raw tables, checks primary keys, performs broadcast joins,
       splits and quarantines anomalous records, and saves clean conformed records to Silver Delta tables.
    3. Gold Warehousing: Filters delivered records, runs aggregations, and loads gold summaries to Snowflake.
    """
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Starting Medallion ETL Execution...")
    
    # Storage Paths
    bronze_orders = "data/delta/bronze_orders"
    bronze_customers = "data/delta/bronze_customers"
    bronze_payments = "data/delta/bronze_payments"
    
    silver_enriched = "data/delta/silver_enriched_orders"
    silver_quarantine = "data/delta/quarantined_orders"
    
    gold_summary = "data/delta/gold_category_summary"
    
    # ----------------------------------------------------
    # STAGE 1: BRONZE LAYER (Raw Ingestion)
    # ----------------------------------------------------
    logger.info("--- STAGE 1: Bronze Ingestion ---")
    
    raw_orders_df = spark.read.option("header", "true").schema(ORDERS_SCHEMA).csv(orders_path)
    raw_customers_df = spark.read.option("header", "true").schema(CUSTOMERS_SCHEMA).csv(customers_path)
    raw_payments_df = spark.read.option("header", "true").schema(PAYMENTS_SCHEMA).csv(payments_path)
    
    # Save to Bronze Delta tables
    raw_orders_df.write.format("delta").mode("overwrite").save(bronze_orders)
    raw_customers_df.write.format("delta").mode("overwrite").save(bronze_customers)
    raw_payments_df.write.format("delta").mode("overwrite").save(bronze_payments)
    logger.info("Successfully ingested raw data to Bronze Delta Tables.")
    
    # ----------------------------------------------------
    # STAGE 2: SILVER LAYER (Cleansing, Validation, Quarantine & Joins)
    # ----------------------------------------------------
    logger.info("--- STAGE 2: Silver Cleaning & Enrichment ---")
    
    # Read from Bronze Delta tables
    bronze_orders_df = spark.read.format("delta").load(bronze_orders)
    bronze_customers_df = spark.read.format("delta").load(bronze_customers)
    bronze_payments_df = spark.read.format("delta").load(bronze_payments)
    
    # Run critical checks (PK null and Empty DataFrame checks)
    run_critical_checks(bronze_orders_df, "order_id", "bronze_orders")
    run_critical_checks(bronze_customers_df, "customer_id", "bronze_customers")
    run_critical_checks(bronze_payments_df, "payment_id", "bronze_payments")
    
    # Cleaning Transformations
    cleaned_orders = clean_orders(bronze_orders_df)
    cleaned_customers = clean_customers(bronze_customers_df)
    cleaned_payments = clean_payments(bronze_payments_df)

    # Monetary and Bucket Calculations (Catalyst-friendly Spark expression replaces slow Python UDF)
    enriched_orders = cleaned_orders \
        .withColumn("gross_amount", col("quantity") * col("unit_price")) \
        .withColumn("discount_amount", (col("gross_amount") * col("discount")) / 100.0) \
        .withColumn("net_amount", col("gross_amount") - col("discount_amount")) \
        .withColumn(
            "order_bucket",
            when(col("quantity") >= 4, "large")
            .when(col("quantity") >= 2, "medium")
            .when(col("quantity").isNotNull(), "small")
            .otherwise("unknown")
        )
        
    # Joins: Broadcast small dimension tables (customers, payments) to prevent driver shuffling
    joined_df = enriched_orders \
        .join(broadcast(cleaned_customers), "customer_id", "inner") \
        .join(broadcast(cleaned_payments), "order_id", "left")

    # Quarantine Pattern (Fail-safe data quality splitting)
    valid_condition = (
        (col("quantity") > 0) & 
        (col("unit_price") >= 0) & 
        (col("discount") >= 0) & 
        (col("discount") <= 100)
    )
    
    clean_silver_df = joined_df.filter(valid_condition)
    quarantined_df = joined_df.filter(~valid_condition)
    
    clean_count = clean_silver_df.count()
    quarantine_count = quarantined_df.count()
    
    logger.info(f"DQS Quarantine Split. Clean records: {clean_count}, Quarantined: {quarantine_count}")
    
    # Save quarantined records with metadata
    if quarantine_count > 0:
        logger.warning(f"Directing {quarantine_count} anomalous records to quarantine delta directory...")
        quarantined_df \
            .withColumn("quarantine_reason", lit("Invalid quantity, negative price, or illegal discount percentage")) \
            .withColumn("quarantined_at", current_timestamp()) \
            .write.format("delta").mode("append").save(silver_quarantine)
            
    # Add calendar partitions for optimized writing
    partitioned_silver = clean_silver_df \
        .withColumn("year", year(col("order_date"))) \
        .withColumn("month", month(col("order_date")))
        
    partitioned_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save(silver_enriched)
    logger.info("Successfully conformed and saved Silver Enriched Delta Table.")
    
    # ----------------------------------------------------
    # STAGE 3: GOLD LAYER (Summary Metrics & Warehousing)
    # ----------------------------------------------------
    logger.info("--- STAGE 3: Gold Summary & Snowflake Load ---")
    
    silver_orders_df = spark.read.format("delta").load(silver_enriched)
    delivered_orders = silver_orders_df.filter(col("order_status") == "DELIVERED")
    
    # Aggregated revenue summary per product category
    category_summary = delivered_orders \
        .groupBy("category") \
        .agg(
            count("*").alias("total_orders"),
            sum("net_amount").alias("total_revenue"),
            avg("net_amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers")
        ) \
        .orderBy(col("total_revenue").desc()) \
        .withColumn("calculated_at", current_timestamp())
        
    category_summary.write.format("delta").mode("overwrite").save(gold_summary)
    logger.info("Gold Category Summary Delta table written.")
    
    # Query reporting showcase
    logger.info("Displaying Gold Level Category Summary aggregates:")
    category_summary.show(truncate=False)
    
    # Write to Snowflake database (with fallback to Parquet)
    write_to_snowflake(delivered_orders, "FACT_DELIVERED_ORDERS")
    write_to_snowflake(category_summary, "AGG_CATEGORY_REVENUE")
    
    logger.info("ETL run finished successfully!")

if __name__ == "__main__":
    # Create input directories if they do not exist
    os.makedirs("data", exist_ok=True)
    
    # If raw files don't exist, seed them automatically
    if not os.path.exists("data/orders.csv"):
        from local_generator import setup_all_data
        setup_all_data("data")
        
    run_etl()
