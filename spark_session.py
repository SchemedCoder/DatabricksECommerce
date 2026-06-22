from pyspark.sql import SparkSession

def get_spark(app_name="EComDatabricksPipeline"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Optimizations for smaller datasets and dynamic shuffling
        .config("spark.sql.shuffle.partitions", "8") 
        .config("spark.sql.adaptive.enabled", "true") 
        .getOrCreate()
    )
    # spark.sparkContext.setLogLevel("WARN")
    return spark