from pyspark.sql import DataFrame
from config.config import get_snowflake_options, SF_USER
from src.logger import get_logger
import os

logger = get_logger("Warehouse")

def write_to_snowflake(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """
    Loads a Spark DataFrame into Snowflake using the official Snowflake Spark Connector.
    
    If credentials are set to default/MOCK values (e.g., during local testing or CI),
    it automatically falls back to writing local Parquet files, keeping the pipeline 
    fully runnable and testable in any environment.
    """
    sf_options = get_snowflake_options()
    
    # Check if credentials are mock/default. If so, fall back to local parquet writing.
    if SF_USER == "MOCK_USER" or not SF_USER:
        fallback_dir = os.path.join("data", "warehouse_fallback", table_name)
        logger.warning(
            f"[CONFIG] Snowflake credentials not configured or using default ('{SF_USER}'). "
            f"Falling back to local Parquet storage at: {fallback_dir}"
        )
        try:
            os.makedirs(os.path.dirname(fallback_dir), exist_ok=True)
            (
                df.write
                .mode(mode)
                .parquet(fallback_dir)
            )
            logger.info(f"[SUCCESS] Local fallback write completed successfully for table: {table_name}")
            return
        except Exception as e:
            logger.error(f"Failed local fallback write: {str(e)}")
            raise e
            
    # Production deployment flow using spark-snowflake connector
    logger.info(f"Loading data into Snowflake table: {table_name} (Mode: {mode})")
    try:
        (
            df.write
            .format("snowflake")
            .options(**sf_options)
            .option("dbtable", table_name)
            .mode(mode)
            .save()
        )
        logger.info(f"[SUCCESS] Successfully wrote table '{table_name}' to Snowflake.")
    except Exception as e:
        logger.error(f"Failed to write to Snowflake database: {str(e)}")
        logger.error("Verify that the snowflake spark connector package is included in the spark session context.")
        raise e

def write_partitioned_delta(df: DataFrame, output_path: str, partition_cols: list[str]) -> None:
    """
    Writes data locally in partitioned Delta format (Silver/Gold standard).
    """
    logger.info(f"Writing partitioned Delta output to: {output_path} (Partitioned by: {partition_cols})")
    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy(*partition_cols)
            .save(output_path)
        )
        logger.info(f"[SUCCESS] Successfully wrote partitioned Delta files to {output_path}")
    except Exception as e:
        logger.error(f"Failed writing partitioned Delta to {output_path}: {str(e)}")
        raise e
