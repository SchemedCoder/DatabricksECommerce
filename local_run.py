import os
import shutil
import time
import sys

# Clean sys.path of space-containing elements to prevent JVM launch issues on Windows
saved_sys_path = list(sys.path)
sys.path = [p for p in sys.path if " " not in p]

# Import SparkSession after cleaning path
from pyspark.sql import SparkSession

# Restore sys.path
sys.path = saved_sys_path
workspace_dir = os.getcwd()
if workspace_dir not in sys.path:
    sys.path.insert(0, workspace_dir)

import local_generator
from batch import batch_etl

def clear_data():
    """
    Cleans up all data directories to ensure a clean end-to-end run.
    """
    paths_to_clean = ["data/delta", "data/spark-warehouse", "data/derby", "data/warehouse_fallback"]
    print("[*] Cleaning up previous data files...")
    for path in paths_to_clean:
        if os.path.exists(path):
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
                print(f"    - Cleaned: {path}")
            except Exception as e:
                print(f"    - [Warning] Could not clean {path}: {e}")

def main():
    print("====================================================================")
    print("           E-COMMERCE METRICS & DATA WAREHOUSE PLATFORM")
    print("====================================================================")
    
    # 1. Clean up folders
    clear_data()
    
    # 2. Seed mock datasets (orders.csv, customers.csv, payments.csv)
    print("\n----------------------------------------------------")
    print("   STAGE 0: Seeding Mock CSV Datasets")
    print("----------------------------------------------------")
    local_generator.setup_all_data("data")
    
    # 3. Run Spark Medallion ETL (Bronze -> Silver -> Gold)
    print("\n----------------------------------------------------")
    print("   STAGE 1: Running Medallion Batch Pipeline (PySpark)")
    print("----------------------------------------------------")
    batch_etl.run_etl(
        orders_path="data/orders.csv",
        customers_path="data/customers.csv",
        payments_path="data/payments.csv"
    )
    
    # 4. Delta Lake Verifications
    print("\n----------------------------------------------------")
    print("   STAGE 2: Querying Delta Tables & Verifications")
    print("----------------------------------------------------")
    spark = batch_etl.get_spark_session()
    
    silver_path = "data/delta/silver_enriched_orders"
    gold_path = "data/delta/gold_category_summary"
    
    if os.path.exists(silver_path) and os.path.exists(gold_path):
        silver_df = spark.read.format("delta").load(silver_path)
        gold_df = spark.read.format("delta").load(gold_path)
        
        silver_orders_count = silver_df.filter("order_status = 'DELIVERED'").count()
        gold_orders_count = gold_df.agg({"total_orders": "sum"}).collect()[0][0]
        
        print(f"[+] Total DELIVERED orders in Silver Table: {silver_orders_count}")
        print(f"[+] Sum of total_orders aggregated in Gold Table: {gold_orders_count}")
        
        if silver_orders_count == gold_orders_count:
            print("[SUCCESS] Data lineage and metrics verification passed!")
        else:
            print("[WARNING] Data discrepancy detected. Check aggregations.")
    else:
        print("[!] Delta tables not found. Verify execution logs.")
        
    print("\n====================================================================")
    print("                     DEMO RUN COMPLETED SUCCESSFULLY")
    print("====================================================================")

if __name__ == "__main__":
    main()
