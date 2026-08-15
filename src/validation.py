from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from src.logger import get_logger

logger = get_logger("Validation")

class DataQualityException(Exception):
    """Exception raised when a critical data quality check fails."""
    pass

class DataValidator:
    """
    Implements industry-standard Data Quality System (DQS) checks,
    separating critical failures from minor anomalies (using a quarantine pattern).
    """
    
    @staticmethod
    def run_critical_checks(df: DataFrame, primary_key: str, name: str) -> None:
        """
        Runs checks that must pass. If they fail, the pipeline halts immediately.
        - Check for empty DataFrame.
        - Check for null values in primary keys.
        """
        logger.info(f"Running critical DQS checks on {name}...")
        
        # 1. Empty Check
        row_count = df.count()
        if row_count == 0:
            raise DataQualityException(f"Critical Check Failed: DataFrame '{name}' is empty.")
        
        # 2. Null Primary Key Check
        null_count = df.filter(col(primary_key).isNull()).count()
        if null_count > 0:
            raise DataQualityException(
                f"Critical Check Failed: Found {null_count} NULLs in primary key '{primary_key}' of table '{name}'."
            )
            
        logger.info(f"[SUCCESS] Critical checks passed for {name}. Row count: {row_count}")

    @staticmethod
    def split_quarantine_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Implements the Quarantine Pattern.
        Splits orders into Clean records (for downstream warehousing) and 
        Quarantined records (written to a dead-letter location for monitoring/fixing).
        
        Rules:
        - quantity must be > 0
        - unit_price must be >= 0
        - discount must be between 0 and 100
        """
        logger.info("Evaluating records for anomaly quarantine...")
        
        # Define validation rule
        valid_condition = (
            (col("quantity") > 0) & 
            (col("unit_price") >= 0) & 
            (col("discount") >= 0) & 
            (col("discount") <= 100)
        )
        
        # Clean data
        clean_df = df.filter(valid_condition)
        
        # Quarantined data (negated condition)
        quarantine_df = df.filter(~valid_condition)
        
        # Add metadata telemetry to quarantine tables (highly professional)
        quarantine_df = quarantine_df.withColumn("quarantine_reason", 
            lit("Invalid quantity, negative price, or illegal discount percentage")
        ).withColumn("quarantined_at", current_timestamp())
        
        clean_count = clean_df.count()
        quarantine_count = quarantine_df.count()
        
        logger.info(f"DQS Split Complete. Clean Records: {clean_count}, Quarantined Records: {quarantine_count}")
        
        if quarantine_count > 0:
            logger.warning(f"[WARNING] Found {quarantine_count} anomalous records. Directing to quarantine storage.")
            
        return clean_df, quarantine_df
