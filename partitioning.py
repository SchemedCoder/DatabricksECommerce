from pyspark.sql.functions import year, month, col

def write_partitioned_output(df, output_path):
    # Partition by Year and Month as you suggested in your snippets
    write_df = (
        df
        .withColumn("year", year(col("order_date")))
        .withColumn("month", month(col("order_date")))
    )

    (
        write_df.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(output_path)  # Parquet is much faster and cheaper to query than CSV
    )
    print(f"✅ Data partitioned by Year/Month and written to {output_path}")

def write_summary_output(df, output_path):
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )
    print(f"✅ Summary Data written to {output_path}")