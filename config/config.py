import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists (for local testing)
load_dotenv()

# App Settings
APP_NAME = os.getenv("APP_NAME", "EcommercePipeline")
ENV = os.getenv("ENV", "local")  # 'local' or 'databricks'

# Raw Input CSV Paths (with local fallbacks)
ORDERS_PATH = os.getenv("ORDERS_PATH", "/Volumes/workspace/default/ecommercepipeline/data/orders.csv")
CUSTOMERS_PATH = os.getenv("CUSTOMERS_PATH", "/Volumes/workspace/default/ecommercepipeline/data/customers.csv")
PAYMENTS_PATH = os.getenv("PAYMENTS_PATH", "/Volumes/workspace/default/ecommercepipeline/data/payments.csv")

# If running locally, let's point to relative path fallbacks if the volume paths don't exist
if ENV == "local" or not os.path.exists(ORDERS_PATH):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    local_data_dir = os.path.join(base_dir, "data")
    
    # Fallback to local files if they exist
    if os.path.exists(local_data_dir):
        ORDERS_PATH = os.path.join(local_data_dir, "orders.csv")
        CUSTOMERS_PATH = os.path.join(local_data_dir, "customers.csv")
        PAYMENTS_PATH = os.path.join(local_data_dir, "payments.csv")

# Output Paths (Bronze & Silver layers on DBFS/ADLS/S3)
OUTPUT_BRONZE_ORDERS = os.getenv("OUTPUT_BRONZE_ORDERS", "/Volumes/workspace/default/ecommercepipeline/bronze/orders")
OUTPUT_BRONZE_CUSTOMERS = os.getenv("OUTPUT_BRONZE_CUSTOMERS", "/Volumes/workspace/default/ecommercepipeline/bronze/customers")
OUTPUT_BRONZE_PAYMENTS = os.getenv("OUTPUT_BRONZE_PAYMENTS", "/Volumes/workspace/default/ecommercepipeline/bronze/payments")

OUTPUT_SILVER_ENRICHED = os.getenv("OUTPUT_SILVER_ENRICHED", "/Volumes/workspace/default/ecommercepipeline/silver/enriched_orders")
OUTPUT_SILVER_QUARANTINE = os.getenv("OUTPUT_SILVER_QUARANTINE", "/Volumes/workspace/default/ecommercepipeline/silver/quarantined_orders")
OUTPUT_GOLD_SUMMARY = os.getenv("OUTPUT_GOLD_SUMMARY", "/Volumes/workspace/default/ecommercepipeline/gold/category_summary")

# If running locally, let's redirect outputs to a project-relative 'storage' directory to avoid polluting the drive root
if ENV == "local" or not os.path.exists("/Volumes/workspace"):
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    storage_dir = os.path.join(project_root, "storage")
    
    OUTPUT_BRONZE_ORDERS = os.path.join(storage_dir, "bronze", "orders")
    OUTPUT_BRONZE_CUSTOMERS = os.path.join(storage_dir, "bronze", "customers")
    OUTPUT_BRONZE_PAYMENTS = os.path.join(storage_dir, "bronze", "payments")
    OUTPUT_SILVER_ENRICHED = os.path.join(storage_dir, "silver", "enriched_orders")
    OUTPUT_SILVER_QUARANTINE = os.path.join(storage_dir, "silver", "quarantined_orders")
    OUTPUT_GOLD_SUMMARY = os.path.join(storage_dir, "gold", "category_summary")

# Snowflake Warehousing Configuration
# In production, these should be loaded from Databricks Secrets: dbutils.secrets.get(scope, key)
SF_USER = os.getenv("SF_USER", "MOCK_USER")
SF_PASSWORD = os.getenv("SF_PASSWORD", "MOCK_PASSWORD")
SF_ACCOUNT = os.getenv("SF_ACCOUNT", "MOCK_ACCOUNT.snowflakecomputing.com")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE", "COMPUTE_WH")
SF_DATABASE = os.getenv("SF_DATABASE", "ECOMMERCE_DB")
SF_SCHEMA = os.getenv("SF_SCHEMA", "PUBLIC")
SF_ROLE = os.getenv("SF_ROLE", "SYSADMIN")

# Snowflake Spark Connector Options
def get_snowflake_options():
    return {
        "sfURL": SF_ACCOUNT if SF_ACCOUNT.endswith(".snowflakecomputing.com") else f"{SF_ACCOUNT}.snowflakecomputing.com",
        "sfUser": SF_USER,
        "sfPassword": SF_PASSWORD,
        "sfDatabase": SF_DATABASE,
        "sfSchema": SF_SCHEMA,
        "sfWarehouse": SF_WAREHOUSE,
        "sfRole": SF_ROLE
    }
