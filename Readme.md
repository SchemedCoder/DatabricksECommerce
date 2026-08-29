# E-Commerce Metrics & Data Warehouse Platform

A production-grade distributed data engineering platform built in **PySpark** and **Delta Lake** on **Databricks**, loading final analytics-ready conformed datasets into a **Snowflake** data warehouse.

This project implements a robust **Medallion Architecture (Bronze -> Silver -> Gold)**, designed with industrial best practices for data validation, performance optimization, and operational resilience.

---

## 🏗️ Project Architecture & Data Flow

```
[Ingestion Sources]
  └── CSV Data Files (Orders, Customers, Payments)
           │
           ▼
[Processing Engine: Azure Databricks (PySpark)]
  ├── BRONZE LAYER: Raw ingestion. Schema-enforced CSV payloads stored in Delta Lake.
  │     │
  │     ▼
  ├── SILVER LAYER: Type casting, conformed joining (broadcast lookup), cleaning (drops nulls),
  │                 and anomaly quarantine (redirects corrupt transactions).
  │     │
  │     ▼
  └── GOLD LAYER: Analytical summaries aggregated by category.
           │
           ▼
[Storage: Delta Lake / Unity Catalog]
  └── Silver & Gold Delta Tables (Served to Snowflake Data Warehouse)
```

---

## 🛠️ Tech Stack

- **Compute**: Azure Databricks, Apache Spark (PySpark)
- **Storage Layer**: Delta Lake, Unity Catalog, Azure ADLS Gen2
- **Warehouse**: Snowflake Data Warehouse
- **Orchestration**: Apache Airflow / Databricks Workflows
- **Validation & Testing**: Pytest

---

## 📂 Project Structure

```text
├── batch/
│   └── batch_etl.py          # Central Medallion ETL pipeline (Bronze -> Silver -> Gold)
├── warehouse/
│   ├── schema.sql            # Unity Catalog & Snowflake DDL tables
│   └── queries.sql           # Analytics, time-travel, and optimization query examples
├── tests/
│   ├── conftest.py           # Pytest local Spark Session fixture with Delta Jar downloads
│   └── test_etl.py           # Unit tests for cleansing and DQS checks
├── ci_cd/
│   └── github_actions.yml    # CI pipeline for automated testing
├── .env.example              # Template for configuring Snowflake credentials
├── .gitignore                # Files excluded from git tracking
├── requirements.txt          # Python package requirements
├── local_generator.py        # Local mock CSV data generator
├── local_run.py              # Orchestrator to run the entire demo end-to-end locally
└── README.md                 # Project documentation
```

---

## 🚀 How to Run Locally

The pipeline is fully executable locally without needing a live Databricks cluster or Snowflake warehouse.

### 1. Set Up Virtual Environment & Dependencies
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure Environment Variables
Copy `.env.example` to `.env` and update credentials:
```bash
cp .env.example .env
```

### 3. Run the End-to-End Demo
To seed mock files, run the batch Medallion pipeline, and verify Delta table counts, execute:
```bash
python local_run.py
```

### 4. Run Pytest Suite
```bash
python -m pytest tests/
```