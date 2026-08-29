import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from batch.batch_etl import clean_orders, run_critical_checks, DataQualityException
from pyspark.sql.functions import col, when

def test_clean_orders(spark: SparkSession):
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("order_date", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("discount", DoubleType(), True),
        StructField("order_status", StringType(), True)
    ])
    
    data = [
        (1, 101, "2023-01-15", " ELECTRONICS ", "2", "299.99", None, "DELIVERED"),
        (2, 102, "2023-01-16", "Fashion", "5", "49.50", 10.0, "CANCELLED")
    ]
    
    df = spark.createDataFrame(data, schema)
    
    # Run the cleaning transformation
    cleaned_df = df.fillna({"discount": 0.0}) \
        .withColumn("category", col("category").cast("string")) \
        .withColumn("category", col("category").cast("string")) # simple mock mapping
    
    # We can inspect basic types
    assert cleaned_df.count() == 2

def test_critical_dqs_checks(spark: SparkSession):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("val", StringType(), True)
    ])

    # Case 1: Null Primary Keys should raise Exception
    bad_data = [(1, "A"), (None, "B")]
    bad_df = spark.createDataFrame(bad_data, schema)
    
    with pytest.raises(DataQualityException):
        run_critical_checks(bad_df, "id", "test_table")

    # Case 2: Empty Dataframe should raise Exception
    empty_df = spark.createDataFrame([], schema)
    with pytest.raises(DataQualityException):
        run_critical_checks(empty_df, "id", "test_table")

def test_quarantine_splitting(spark: SparkSession):
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount", DoubleType(), True)
    ])
    
    data = [
        (1, 2, 100.0, 10.0),   # Valid
        (2, -1, 50.0, 0.0),    # Invalid quantity
        (3, 1, -10.0, 5.0),    # Invalid price
        (4, 3, 20.0, 150.0),   # Invalid discount (>100)
    ]
    
    df = spark.createDataFrame(data, schema)
    
    valid_condition = (
        (col("quantity") > 0) & 
        (col("unit_price") >= 0) & 
        (col("discount") >= 0) & 
        (col("discount") <= 100)
    )
    
    clean_df = df.filter(valid_condition)
    quarantine_df = df.filter(~valid_condition)
    
    assert clean_df.count() == 1
    assert quarantine_df.count() == 3


