import pytest
import os
import sys

# 1. Clean sys.path temporarily of any elements containing spaces
# This bypasses the Windows PySpark JVM launch bug when directory paths contain spaces.
saved_sys_path = list(sys.path)
sys.path = [p for p in sys.path if " " not in p]

# 2. Import PySpark and SparkSession
import pyspark
from pyspark.sql import SparkSession

# 3. Restore sys.path
sys.path = saved_sys_path
workspace_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if workspace_dir not in sys.path:
    sys.path.insert(0, workspace_dir)

import shutil
import urllib.request

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Session-scoped local SparkSession fixture with dynamic Delta Lake jar downloads
    matching Windows compatibility criteria.
    """
    version = pyspark.__version__
    
    # Map PySpark versions to compatible Delta Lake jar versions
    if version.startswith("3.5"):
        delta_ver = "3.1.0"
    elif version.startswith("3.4"):
        delta_ver = "2.4.0"
    else:
        delta_ver = "3.1.0"
        
    jars_dir = "jars"
    os.makedirs(jars_dir, exist_ok=True)
    
    jars = {
        f"delta-spark_2.12-{delta_ver}.jar": f"https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/{delta_ver}/delta-spark_2.12-{delta_ver}.jar",
        f"delta-storage-{delta_ver}.jar": f"https://repo1.maven.org/maven2/io/delta/delta-storage/{delta_ver}/delta-storage-{delta_ver}.jar"
    }
    
    local_jar_paths = []
    for jar_name, url in jars.items():
        dest_path = os.path.join(jars_dir, jar_name)
        local_jar_paths.append(dest_path)
        if not os.path.exists(dest_path):
            try:
                urllib.request.urlretrieve(url, dest_path)
            except Exception as e:
                pytest.fail(f"Failed downloading {jar_name} for tests: {e}")
                
    jar_config = ",".join(local_jar_paths)
    
    warehouse_dir = "data/spark-warehouse"
    derby_dir = "data/derby"
    
    # Clean sys.path permanently of any elements containing spaces to bypass Windows PySpark JVM launch bug
    sys.path = [p for p in sys.path if " " not in p]
    
    # Clean environment variables containing spaces/quotes
    for k in list(os.environ.keys()):
        if k.startswith("ANTIGRAVITY_"):
            del os.environ[k]
            
    spark_session = SparkSession.builder \
        .master("local[2]") \
        .appName("ETL-Pipeline-Unit-Tests") \
        .config("spark.jars", jar_config) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", warehouse_dir) \
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_dir}") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
        
    yield spark_session
    
    spark_session.stop()
    
    # Clean up local Derby database metadata after session shutdown
    for path in ["derby.log", "metastore_db"]:
        full_path = os.path.join(workspace_dir, path)
        if os.path.exists(full_path):
            try:
                if os.path.isdir(full_path):
                    shutil.rmtree(full_path)
                else:
                    os.remove(full_path)
            except Exception:
                pass
