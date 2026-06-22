from pyspark.sql import DataFrame

def load_csv(spark, path: str) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )