# Ecommerce ETL Pipeline (PySpark + Databricks)

An optimized, end-to-end distributed data engineering pipeline built in PySpark for Databricks. The pipeline ingests multi-source e-commerce retail data (Orders, Customers, and Payments), cleanses anomalies, computes business metrics using high-performance broadcast joins, and loads the structured data into an optimized, partitioned Parquet storage layer.

## Project Architecture

The project follows a modular, decoupled software engineering design separating configuration, orchestration, compute initialization, and transformation components.

```text
├── data/
│   ├── orders.csv            # Raw transactional order data
│   ├── customers.csv         # Customer profile dimension data
│   └── payments.csv          # Payment transaction status data
└── src/
    ├── config.py             # Centralized environment constants and paths
    ├── spark_session.py      # Optimized SparkSession factory
    ├── ingestion.py          # Data ingestion utility module
    ├── transformation.py     # Main business logic, cleansing, UDFs, and aggregations
    ├── partitioning.py       # Storage layer writer (Parquet partitioning)
    └── main.ipynb            # Pipeline Master Orchestrator Notebook