import os
import sys
import pickle
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

# 1. Add parent directory to sys.path to allow local imports
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# 2. Clean sys.path COMPLETELY of any paths containing spaces temporarily
saved_sys_path = list(sys.path)
sys.path = [p for p in sys.path if " " not in p]

# 3. Import stream/batch session builder
from batch.batch_etl import get_spark_session

# 4. Restore sys.path
sys.path = saved_sys_path

def train_revenue_prediction_model(gold_path="data/delta/gold_category_summary", model_output_path="ml/revenue_model.pkl"):
    """
    Simulates a machine learning model training pipeline.
    Reads features directly from the Delta Gold Table, trains a Regression model
    to predict category revenue, and saves the model artifact.
    """
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"[*] Loading training data from Delta Gold Table at {gold_path}...")
    
    # 1. Read features from Gold Delta table
    if not os.path.exists(gold_path):
        print(f"[!] Delta Gold table not found at {gold_path}. Run ETL first.")
        return
        
    df_spark = spark.read.format("delta").load(gold_path)
    
    # Check if we have enough data
    count = df_spark.count()
    print(f"[*] Found {count} records in Delta Gold table.")
    if count < 2:
        print("[!] Insufficient data categories for local training. Seeding mock model.")
        return
        
    # 2. Convert to Pandas for Scikit-Learn training
    df = df_spark.toPandas()
    
    # 3. Feature Engineering
    # Predicting total_revenue based on total_orders and unique_customers
    X = df[["total_orders", "unique_customers"]]
    y = df["total_revenue"]
    
    print("[*] Training features sample:")
    print(X.head())
    
    # 4. Model Training (Linear Regression)
    print("[*] Fitting Linear Regression model...")
    model = LinearRegression()
    model.fit(X, y)
    
    # Evaluate on the training set (small aggregate dataset)
    predictions = model.predict(X)
    mse = mean_squared_error(y, predictions)
    r2 = r2_score(y, predictions)
    print(f"[+] Model Training Results:")
    print(f"    - Mean Squared Error (MSE): {mse:.4f}")
    print(f"    - R-squared (R2 Score): {r2:.4f}")
    print(f"    - Coefficients: {model.coef_}")
    print(f"    - Intercept: {model.intercept_}")
    
    # 5. Save Model (Simulating MLflow registry artifact)
    os.makedirs(os.path.dirname(model_output_path), exist_ok=True)
    with open(model_output_path, "wb") as f:
        pickle.dump(model, f)
    print(f"[+] Model artifact registered and saved locally at {model_output_path}")

if __name__ == "__main__":
    train_revenue_prediction_model()
