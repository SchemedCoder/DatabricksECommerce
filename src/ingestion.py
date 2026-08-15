from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, BooleanType
from src.logger import get_logger

logger = get_logger("Ingestion")

# Define explicit schemas to enforce during ingestion (industry standard)
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", StringType(), True),  # Load as string first, clean to date later
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

def load_raw_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    """
    Ingests CSV files from raw path with a predefined schema for reliability.
    """
    logger.info(f"Ingesting raw CSV from: {path}")
    try:
        df = (
            spark.read
            .option("header", "true")
            .schema(schema)
            .csv(path)
        )
        return df
    except Exception as e:
        logger.error(f"Error loading CSV from {path}: {str(e)}")
        raise e

def write_to_bronze(df: DataFrame, bronze_path: str) -> None:
    """
    Writes raw dataframes to Delta Lake (Bronze stage) with ACID guarantees.
    """
    logger.info(f"Writing raw dataframe to Bronze Delta table at: {bronze_path}")
    try:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(bronze_path)
        )
        logger.info(f"Successfully wrote data to {bronze_path}")
    except Exception as e:
        logger.error(f"Failed to write to Bronze at {bronze_path}: {str(e)}")
        raise e
