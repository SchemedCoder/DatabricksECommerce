from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

# Default arguments for Airflow DAG (vital for a 3-4 years exp candidate to show operational readiness)
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['de-alerts@ecommerce.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Define DAG structure
with DAG(
    dag_id='ecommerce_medallion_etl',
    default_args=default_args,
    description='Orchestrates the Medallion E-Commerce Pipeline in Databricks and Warehouses to Snowflake',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['ecommerce', 'databricks', 'snowflake', 'production'],
) as dag:

    # Databricks Task definition using DatabricksSubmitRunOperator
    # Launches the job on a new, optimized single-node or multi-node cluster (cost optimization)
    run_databricks_pipeline = DatabricksSubmitRunOperator(
        task_id='run_pyspark_etl_job',
        databricks_conn_id='databricks_default',
        new_cluster={
            'spark_version': '13.3.x-scala2.12',  # Production standard Spark version
            'node_type_id': 'i3.xlarge',          # Cost-efficient node with high memory
            'num_workers': 2,
            'spark_conf': {
                'spark.sql.adaptive.enabled': 'true',
                'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
            },
            'aws_attributes': {
                'availability': 'SPOT_WITH_FALLBACK_AZ'  # Use spot instances to save 70% cluster cost
            }
        },
        spark_python_task={
            'python_file': 'dbfs:/FileStore/ecommerce_pipeline/src/main.py',
            'parameters': [
                '--env', 'production'
            ]
        },
        # Libraries required by the job (such as Snowflake Spark connector)
        libraries=[
            {
                'maven': {
                    'coordinates': 'net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4'
                }
            },
            {
                'pypi': {
                    'package': 'python-dotenv>=1.0.0'
                }
            }
        ]
    )

    run_databricks_pipeline
