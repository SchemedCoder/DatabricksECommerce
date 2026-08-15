import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from src.transformation import clean_orders, clean_customers, clean_payments, enrich_and_join, generate_category_summary
from src.validation import DataValidator, DataQualityException

def test_clean_orders(spark: SparkSession):
    # Mock raw orders data
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("order_date", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", StringType(), True),  # String to be cast to Int
        StructField("unit_price", StringType(), True), # String to be cast to Double
        StructField("discount", DoubleType(), True),   # Contains Null
        StructField("order_status", StringType(), True)
    ])
    
    data = [
        (1, 101, "2023-01-15", " ELECTRONICS ", "2", "299.99", None, "DELIVERED"),
        (2, 102, "2023-01-16", "Fashion", "5", "49.50", 10.0, "CANCELLED")
    ]
    
    df = spark.createDataFrame(data, schema)
    cleaned_df = clean_orders(df)
    
    results = cleaned_df.collect()
    
    # Assertions
    assert results[0]["category"] == "electronics"  # Lowercase and trimmed
    assert results[0]["quantity"] == 2             # Cast to int
    assert results[0]["unit_price"] == 299.99       # Cast to double
    assert results[0]["discount"] == 0.0           # Null filled with 0
    
    assert results[1]["category"] == "fashion"
    assert results[1]["quantity"] == 5
    assert results[1]["unit_price"] == 49.5
    assert results[1]["discount"] == 10.0

def test_enrich_and_join(spark: SparkSession):
    # Setup schemas
    orders_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("discount", DoubleType(), True)
    ])
    customers_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True)
    ])
    payments_schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("payment_mode", StringType(), True),
        StructField("payment_status", StringType(), True)
    ])

    # Populate data
    orders_data = [(1, 101, 5, 100.0, 10.0), (2, 102, 1, 20.0, 0.0)]
    customers_data = [(101, "Alice", "Mumbai"), (102, "Bob", "Delhi")]
    payments_data = [(1, "UPI", "SUCCESS")] # Order 2 has no payment yet (left join)

    orders_df = spark.createDataFrame(orders_data, orders_schema)
    customers_df = spark.createDataFrame(customers_data, customers_schema)
    payments_df = spark.createDataFrame(payments_data, payments_schema)

    enriched_df = enrich_and_join(orders_df, customers_df, payments_df)
    results = enriched_df.orderBy("order_id").collect()

    # Order 1 Checks
    assert results[0]["gross_amount"] == 500.0
    assert results[0]["discount_amount"] == 50.0
    assert results[0]["net_amount"] == 450.0
    assert results[0]["order_bucket"] == "large" # quantity >= 4
    assert results[0]["customer_name"] == "Alice"
    assert results[0]["payment_mode"] == "UPI"

    # Order 2 Checks
    assert results[1]["gross_amount"] == 20.0
    assert results[1]["discount_amount"] == 0.0
    assert results[1]["net_amount"] == 20.0
    assert results[1]["order_bucket"] == "small" # quantity < 2
    assert results[1]["customer_name"] == "Bob"
    assert results[1].payment_mode is None      # Left join verification

def test_data_quality_critical_checks(spark: SparkSession):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("val", StringType(), True)
    ])

    # Case 1: Null Primary Keys should raise Exception
    bad_data = [(1, "A"), (None, "B")]
    bad_df = spark.createDataFrame(bad_data, schema)
    
    with pytest.raises(DataQualityException) as exc_info:
        DataValidator.run_critical_checks(bad_df, "id", "test_table")
    assert "NULLs in primary key" in str(exc_info.value)

    # Case 2: Empty Dataframe should raise Exception
    empty_df = spark.createDataFrame([], schema)
    with pytest.raises(DataQualityException) as exc_info:
        DataValidator.run_critical_checks(empty_df, "id", "test_table")
    assert "is empty" in str(exc_info.value)

def test_data_quarantine_split(spark: SparkSession):
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
    clean_df, quarantine_df = DataValidator.split_quarantine_orders(df)
    
    assert clean_df.count() == 1
    assert quarantine_df.count() == 3
    
    clean_rows = clean_df.collect()
    assert clean_rows[0]["order_id"] == 1
    
    quarantine_rows = quarantine_df.collect()
    quarantine_ids = [row["order_id"] for row in quarantine_rows]
    assert 2 in quarantine_ids
    assert 3 in quarantine_ids
    assert 4 in quarantine_ids
    assert "quarantine_reason" in quarantine_df.columns
