from pyspark.sql import SparkSession
import os

def get_spark(app_name: str = "EComDatabricksPipeline") -> SparkSession:
    """
    Creates or retrieves a SparkSession. Configures settings for optimization
    and adds Delta Lake support if running in a local environment.
    """
    builder = SparkSession.builder.appName(app_name)
    
    # Check if we are running in Databricks (usually has "DATABRICKS_RUNTIME_VERSION" env variable)
    is_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
    
    if not is_databricks:
        # Configuration for running locally with Delta Lake support
        # Using Maven packages for Delta Lake compatibility with Spark 3.5.x
        builder = (
            builder
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
        )
    
    # Performance Tuning for E-Commerce Datasets (3-4 years experience justification):
    # 1. Adaptive Query Execution (AQE) optimizes query plans at runtime based on partition statistics.
    # 2. Lowering shuffle partitions prevents small-file problems for small/medium local/test runs.
    # 3. Dynamic Partition Pruning (DPP) speeds up join queries against partitioned tables.
    spark = (
        builder
        .config("spark.sql.shuffle.partitions", "8") 
        .config("spark.sql.adaptive.enabled", "true") 
        .config("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        .getOrCreate()
    )
    
    return spark
